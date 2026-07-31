import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from media_workflows.jobs import JobQueue, StaleClaim
from media_workflows.worker import run_one


class JobsTests(unittest.TestCase):
    def setUp(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.clock = 100
        self.source = self.folder / "video.mov"
        self.source.write_bytes(b"video")
        self.queue = JobQueue(self.folder / "jobs.db", clock=lambda: self.clock)

    def test_deduplication_and_stale_worker_protection(self):
        identifier = self.queue.add(self.source)
        self.assertEqual(identifier, self.queue.add(self.source))
        old = self.queue.claim("old", lease_seconds=2)
        self.assertIsNone(self.queue.claim("other"))
        self.clock = 103
        self.assertEqual(self.queue.recover_expired(), 1)
        new = self.queue.claim("new")
        with self.assertRaises(StaleClaim):
            self.queue.finish(identifier, old["token"], result={})
        self.assertEqual(
            self.queue.finish(identifier, new["token"], result={"ok": True}),
            "succeeded",
        )

    def test_retry_backoff_and_changed_source(self):
        self.queue.add(self.source)
        job = self.queue.claim("worker")
        self.assertEqual(
            self.queue.finish(job["id"], job["token"], error="temporary"), "queued"
        )
        self.assertIsNone(self.queue.claim("worker"))
        self.clock += 3
        self.source.write_bytes(b"changed")

        class NoEncoding:
            def process(self, source):
                raise AssertionError("changed source should not encode")

        self.assertEqual(run_one(self.queue, NoEncoding())["status"], "failed")

    def test_completed_result_survives_new_queue_instance(self):
        self.queue.add(self.source)

        class FakePipeline:
            def process(self, source):
                return {"fingerprint": "test"}

        self.assertEqual(run_one(self.queue, FakePipeline())["status"], "succeeded")
        reopened = JobQueue(self.folder / "jobs.db", clock=lambda: self.clock)
        self.assertEqual(reopened.list()[0]["result"], {"fingerprint": "test"})
