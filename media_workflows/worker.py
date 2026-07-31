"""Single-worker driver; publication remains content-addressed and reusable."""

import subprocess
from .pipeline import digest_file


def run_one(queue, pipeline, worker="local"):
    job = queue.claim(worker)
    if job is None:
        return None
    try:
        if digest_file(job["source"]) != job["source_digest"]:
            queue.finish(
                job["id"],
                job["token"],
                error="source changed after enqueue",
                retryable=False,
            )
            return {"id": job["id"], "status": "failed"}
        result = pipeline.process(job["source"])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        status = queue.finish(job["id"], job["token"], error=str(error))
        return {"id": job["id"], "status": status, "error": str(error)}
    status = queue.finish(job["id"], job["token"], result=result)
    return {"id": job["id"], "status": status, "result": result}
