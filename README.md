# Media Workflows

An FFmpeg pipeline for film-portfolio trailers, responsive renditions, poster extraction, and on-demand browser loading.

- Probe and validate video metadata before encoding
- Produce mobile and desktop H.264/AAC MP4 outputs with fast-start metadata
- Avoid upscaling the video stream
- Bound concurrent encoder processes and execution time
- Publish a complete output directory atomically
- Reuse verified content-addressed outputs, with SHA-256 artifact checks
- Generate a JSON manifest and poster image

```sh
python -m unittest discover -s tests -v
python -m media_workflows.cli trailer.mov --output outputs --dry-run
python -m media_workflows.cli trailer.mov --output outputs --workers 2
```

Python 3.12+, FFmpeg, and FFprobe are required for actual encoding. Unit tests use an isolated encoder adapter. The manifest's artifact filenames are relative to its publication directory. The browser module in `web/lazy-video.js` loads trailers near the viewport and provides a cleanup function for page navigation.

## Durable batch processing

[jobs.py](media_workflows/jobs.py) persists queued/running/succeeded/failed jobs in SQLite. Content hashes deduplicate enqueue requests. Claim tokens protect against late acknowledgements after a lease is recovered; retries have bounded attempts and backoff. Changed input files fail without encoding content different from the queued digest.

[worker.py](media_workflows/worker.py) connects the queue to the existing content-addressed pipeline. If a process dies after publication but before acknowledgement, a recovered job can reuse verified output. [batch_cli.py](media_workflows/batch_cli.py) exposes local queue operations:

```sh
python -m media_workflows.batch_cli add trailer.mov interview.mov
python -m media_workflows.batch_cli run --output outputs
python -m media_workflows.batch_cli list
python -m media_workflows.batch_cli recover
```

The run command drains currently ready jobs and exits; it does not wait indefinitely for delayed retries. Run it again after backoff. Recover only expired leases. Actual encoding still requires FFmpeg/FFprobe. Tests use isolated adapters and cover deduplication, worker replacement, retry delays, source changes, and persisted results.
