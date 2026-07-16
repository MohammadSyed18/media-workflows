import json, tempfile, unittest
from pathlib import Path
from media_workflows import Pipeline, Profile


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "trailer.mov"
        self.source.write_bytes(b"source video")
        self.calls = []

    def runner(self, args):
        self.calls.append(args)
        if args[0] == "ffprobe":
            return json.dumps(
                {
                    "streams": [{"codec_type": "video", "width": 640, "height": 360}],
                    "format": {"duration": "20"},
                }
            )
        Path(args[-1]).write_bytes(("encoded " + str(args)).encode())
        return ""

    def test_publication_is_cached_and_hash_verified(self):
        pipeline = Pipeline(self.root / "out", runner=self.runner)
        first = pipeline.process(self.source)
        self.assertEqual(len(first["artifacts"]), 3)
        encodes = len([c for c in self.calls if c[0] == "ffmpeg"])
        self.assertEqual(encodes, 3)
        self.assertEqual(pipeline.process(self.source), first)
        self.assertEqual(len([c for c in self.calls if c[0] == "ffmpeg"]), 3)
        self.assertTrue(all(p["width"] >= 640 for p in first["profiles"]))

    def test_dry_run_does_not_write_outputs_or_upscale_video(self):
        pipeline = Pipeline(self.root / "out", runner=self.runner)
        result = pipeline.process(self.source, dry_run=True)
        self.assertFalse((self.root / "out").exists())
        self.assertTrue(
            all("scale=640:-2" in command for command in result["commands"])
        )

    def test_encoder_failure_does_not_publish_partial_directory(self):
        def fail(args):
            if args[0] == "ffprobe":
                return self.runner(args)
            raise RuntimeError("encode failed")

        with self.assertRaises(RuntimeError):
            Pipeline(self.root / "out", runner=fail).process(self.source)
        self.assertEqual(list((self.root / "out").iterdir()), [])
