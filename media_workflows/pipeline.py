"""Content-addressed, atomic publication of FFmpeg web-video renditions."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from hashlib import sha256
import json, math, os
from pathlib import Path
import shutil, subprocess, tempfile


@dataclass(frozen=True)
class Profile:
    name: str
    width: int
    crf: int = 23
    audio_kbps: int = 128

    def __post_init__(self):
        if (
            not self.name.isalnum()
            or self.width < 2
            or self.width % 2
            or not 0 <= self.crf <= 51
            or self.audio_kbps < 32
        ):
            raise ValueError("invalid encoding profile")


DEFAULT_PROFILES = (Profile("mobile", 640, 25, 96), Profile("desktop", 1280, 23, 128))


def digest_file(path):
    digest = sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run_command(args):
    return subprocess.run(
        args, capture_output=True, text=True, check=True, timeout=600
    ).stdout


class Pipeline:
    def __init__(
        self, output, profiles=DEFAULT_PROFILES, workers=2, runner=run_command
    ):
        self.output = Path(output)
        self.profiles = tuple(profiles)
        self.runner = runner
        if (
            not self.profiles
            or len({p.name for p in self.profiles}) != len(self.profiles)
            or type(workers) is not int
            or not 1 <= workers <= 4
        ):
            raise ValueError("invalid pipeline configuration")
        self.workers = workers

    def probe(self, source):
        data = json.loads(
            self.runner(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_streams",
                    "-show_format",
                    "-of",
                    "json",
                    str(source),
                ]
            )
        )
        video = next(
            (s for s in data.get("streams", []) if s.get("codec_type") == "video"), None
        )
        if not video:
            raise ValueError("input contains no video stream")
        width = int(video.get("width", 0))
        height = int(video.get("height", 0))
        duration = float(
            data.get("format", {}).get("duration", video.get("duration", 0))
        )
        if width < 2 or height < 2 or not math.isfinite(duration) or duration <= 0:
            raise ValueError("invalid video metadata")
        return {"width": width, "height": height, "duration": duration}

    def plan(self, source, staging, metadata):
        jobs = []
        for profile in self.profiles:
            width = min(profile.width, metadata["width"])
            width -= width % 2
            target = Path(staging) / (profile.name + ".mp4")
            args = [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(source),
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-vf",
                f"scale={width}:-2",
                "-c:v",
                "libx264",
                "-preset",
                "medium",
                "-crf",
                str(profile.crf),
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                str(profile.audio_kbps) + "k",
                "-movflags",
                "+faststart",
                "-map_metadata",
                "-1",
                str(target),
            ]
            jobs.append(
                {"name": profile.name, "path": target, "args": args, "width": width}
            )
        target = Path(staging) / "poster.jpg"
        timestamp = min(2.0, metadata["duration"] / 2)
        jobs.append(
            {
                "name": "poster",
                "path": target,
                "args": [
                    "ffmpeg",
                    "-nostdin",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-ss",
                    str(timestamp),
                    "-i",
                    str(source),
                    "-frames:v",
                    "1",
                    "-vf",
                    "scale=640:-2",
                    "-update",
                    "1",
                    str(target),
                ],
            }
        )
        return jobs

    def _cached(self, directory, fingerprint):
        try:
            manifest = json.loads((directory / "manifest.json").read_text())
            if manifest["fingerprint"] != fingerprint:
                return None
            artifacts = manifest["artifacts"]
            if len(artifacts) != len(self.profiles) + 1:
                return None
            for item in artifacts:
                if Path(item["file"]).name != item["file"]:
                    return None
                path = directory / item["file"]
                if (
                    not path.is_file()
                    or path.stat().st_size == 0
                    or digest_file(path) != item["sha256"]
                ):
                    return None
            return manifest
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def process(self, source, *, dry_run=False):
        source = Path(source).resolve()
        if not source.is_file():
            raise FileNotFoundError(source)
        metadata = self.probe(source)
        source_digest = digest_file(source)
        config = {
            "version": 1,
            "profiles": [asdict(p) for p in self.profiles],
            "source_sha256": source_digest,
        }
        fingerprint = sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
        destination = self.output / fingerprint[:20]
        if dry_run:
            return {
                "fingerprint": fingerprint,
                "metadata": metadata,
                "commands": [
                    j["args"] for j in self.plan(source, destination, metadata)
                ],
            }
        cached = self._cached(destination, fingerprint)
        if cached:
            return cached
        self.output.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".encoding-", dir=self.output
        ) as temporary:
            staging = Path(temporary)
            jobs = self.plan(source, staging, metadata)

            def execute(job):
                self.runner(job["args"])
                if not job["path"].is_file() or job["path"].stat().st_size == 0:
                    raise RuntimeError("encoder produced no output: " + job["name"])
                return {
                    "name": job["name"],
                    "file": job["path"].name,
                    "bytes": job["path"].stat().st_size,
                    "sha256": digest_file(job["path"]),
                }

            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                artifacts = list(pool.map(execute, jobs))
            manifest = {
                "fingerprint": fingerprint,
                "source_sha256": source_digest,
                "metadata": metadata,
                "profiles": config["profiles"],
                "artifacts": artifacts,
            }
            (staging / "manifest.json").write_text(
                json.dumps(manifest, indent=2) + "\n"
            )
            if destination.exists():
                valid = self._cached(destination, fingerprint)
                if valid:
                    return valid
                raise RuntimeError(
                    "existing publication failed integrity checks; move it aside before rebuilding"
                )
            try:
                os.rename(staging, destination)
            except FileExistsError:
                valid = self._cached(destination, fingerprint)
                if valid:
                    return valid
                raise
        return manifest
