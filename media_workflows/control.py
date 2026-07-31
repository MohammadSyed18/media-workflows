"""Operator controls for leases, cancellation, retries, and queue health."""

import math
from .jobs import StaleClaim


class QueueControl:
    def __init__(self, queue):
        self.queue = queue

    def renew(self, identifier, token, *, lease_seconds=1800):
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 86400:
            raise ValueError("lease must be 1..86400 seconds")
        with self.queue.transaction() as db:
            now = self.queue.clock()
            if not math.isfinite(now):
                raise ValueError("invalid queue clock")
            changed = db.execute(
                """UPDATE jobs SET lease_until=? WHERE id=? AND token=?
                AND status='running' AND lease_until>?""",
                (now + lease_seconds, identifier, token, now),
            )
            if changed.rowcount != 1:
                raise StaleClaim("cannot renew an expired or replaced claim")
            return now + lease_seconds

    def cancel(self, identifier):
        """Invalidate ownership. An already-running encoder is not force-killed."""
        with self.queue.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            if row is None:
                raise KeyError(identifier)
            if row["status"] == "cancelled":
                return "cancelled"
            if row["status"] == "succeeded":
                raise ValueError("a published job cannot be cancelled")
            db.execute(
                """UPDATE jobs SET status='cancelled',token=NULL,worker=NULL,
                lease_until=NULL,error='cancelled by operator' WHERE id=?""",
                (identifier,),
            )
            return "cancelled"

    def retry(self, identifier):
        """Explicitly reset a terminal failed/cancelled job's bounded attempt budget."""
        with self.queue.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            if row is None:
                raise KeyError(identifier)
            if row["status"] not in {"failed", "cancelled"}:
                raise ValueError("only failed or cancelled jobs can be retried")
            db.execute(
                """UPDATE jobs SET status='queued',attempts=0,ready_at=?,result=NULL,
                error=NULL,token=NULL,worker=NULL,lease_until=NULL WHERE id=?""",
                (self.queue.clock(), identifier),
            )
            return "queued"

    def health(self):
        with self.queue.connection() as db:
            now = self.queue.clock()
            counts = {
                row["status"]: row["count"]
                for row in db.execute(
                    "SELECT status,count(*) AS count FROM jobs GROUP BY status"
                )
            }
            ready = db.execute(
                "SELECT count(*) FROM jobs WHERE status='queued' AND ready_at<=?",
                (now,),
            ).fetchone()[0]
            expired = db.execute(
                "SELECT count(*) FROM jobs WHERE status='running' AND lease_until<=?",
                (now,),
            ).fetchone()[0]
            oldest = db.execute(
                "SELECT min(ready_at) FROM jobs WHERE status='queued' AND ready_at<=?",
                (now,),
            ).fetchone()[0]
        return {
            "counts": counts,
            "ready": ready,
            "expired_leases": expired,
            "oldest_ready_seconds": (
                max(0, now - oldest) if oldest is not None else None
            ),
        }
