import tempfile, unittest
from pathlib import Path
from media_workflows.jobs import JobQueue, StaleClaim
from media_workflows.control import QueueControl


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1000
        self.root = Path(self.temp.name)
        self.source = self.root / "video"
        self.source.write_bytes(b"video")
        self.queue = JobQueue(self.root / "jobs.db", clock=lambda: self.now)
        self.control = QueueControl(self.queue)
        self.id = self.queue.add(self.source)

    def test_renewal_keeps_owner_and_expired_claim_cannot_finish(self):
        claim = self.queue.claim("worker", lease_seconds=10)
        self.now += 5
        self.assertEqual(
            self.control.renew(self.id, claim["token"], lease_seconds=10), 1015
        )
        self.now = 1016
        with self.assertRaises(StaleClaim):
            self.control.renew(self.id, claim["token"])
        with self.assertRaises(StaleClaim):
            self.queue.finish(self.id, claim["token"], result={})
        self.assertEqual(self.control.health()["expired_leases"], 1)

    def test_cancel_invalidates_worker_and_retry_gets_new_token(self):
        claim = self.queue.claim("worker")
        self.control.cancel(self.id)
        self.control.cancel(self.id)
        with self.assertRaises(StaleClaim):
            self.queue.finish(self.id, claim["token"], result={})
        self.control.retry(self.id)
        new = self.queue.claim("other")
        self.assertNotEqual(new["token"], claim["token"])
        self.assertEqual(new["attempts"], 1)
        self.queue.finish(self.id, new["token"], result={"published": True})
        with self.assertRaises(ValueError):
            self.control.cancel(self.id)

    def test_health_counts_ready_and_delayed_jobs(self):
        self.assertEqual(self.control.health()["ready"], 1)
        claim = self.queue.claim("worker")
        self.queue.finish(self.id, claim["token"], error="temporary")
        self.assertEqual(self.control.health()["ready"], 0)
        self.now += 10
        self.assertGreater(self.control.health()["oldest_ready_seconds"], 0)
        with self.assertRaises(ValueError):
            self.control.retry(self.id)
