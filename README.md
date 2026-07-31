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
