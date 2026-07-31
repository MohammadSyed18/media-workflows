"""Durable SQLite batch jobs with claim tokens and recoverable worker leases."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
from uuid import uuid4
from .pipeline import digest_file


class StaleClaim(ValueError):
    pass


class JobQueue:
    def __init__(self, path, *, clock=time.time, attempts=3):
        if type(attempts) is not int or not 1 <= attempts <= 10:
            raise ValueError("attempts must be 1..10")
        self.path, self.clock, self.maximum_attempts = Path(path), clock, attempts
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.executescript(
                """PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS jobs(
                  id TEXT PRIMARY KEY, source TEXT NOT NULL, source_digest TEXT NOT NULL,
                  status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                  ready_at REAL NOT NULL, lease_until REAL, token TEXT, worker TEXT,
                  result TEXT, error TEXT, UNIQUE(source,source_digest));
                CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,ready_at);"""
            )

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise

    def add(self, source):
        path = Path(source).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        digest = digest_file(path)
        with self.transaction() as db:
            old = db.execute(
                "SELECT id FROM jobs WHERE source=? AND source_digest=?",
                (str(path), digest),
            ).fetchone()
            if old:
                return old["id"]
            identifier = str(uuid4())
            db.execute(
                "INSERT INTO jobs(id,source,source_digest,status,ready_at) VALUES(?,?,?,?,?)",
                (identifier, str(path), digest, "queued", self.clock()),
            )
            return identifier

    def claim(self, worker, *, lease_seconds=1800):
        if (
            not isinstance(worker, str)
            or not worker.strip()
            or type(lease_seconds) is not int
            or not 1 <= lease_seconds <= 86400
        ):
            raise ValueError("worker and valid lease required")
        with self.transaction() as db:
            now = self.clock()
            row = db.execute(
                "SELECT * FROM jobs WHERE status='queued' AND ready_at<=? AND attempts<? ORDER BY ready_at,id LIMIT 1",
                (now, self.maximum_attempts),
            ).fetchone()
            if row is None:
                return None
            token = str(uuid4())
            db.execute(
                "UPDATE jobs SET status='running',attempts=attempts+1,lease_until=?,token=?,worker=? WHERE id=?",
                (now + lease_seconds, token, worker, row["id"]),
            )
            return dict(
                db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            )

    def finish(self, identifier, token, *, result=None, error=None, retryable=True):
        if (result is None) == (error is None):
            raise ValueError("supply exactly one result or error")
        rendered = json.dumps(result, allow_nan=False) if result is not None else None
        with self.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            if (
                row is None
                or row["status"] != "running"
                or row["token"] != token
                or row["lease_until"] <= self.clock()
            ):
                raise StaleClaim("job is no longer owned by this claim")
            status = (
                "succeeded"
                if error is None
                else (
                    "queued"
                    if retryable and row["attempts"] < self.maximum_attempts
                    else "failed"
                )
            )
            delay = min(300, 2 ** row["attempts"]) if status == "queued" else 0
            db.execute(
                "UPDATE jobs SET status=?,result=?,error=?,ready_at=?,lease_until=NULL,token=NULL,worker=NULL WHERE id=?",
                (
                    status,
                    rendered,
                    str(error)[:2000] if error is not None else None,
                    self.clock() + delay,
                    identifier,
                ),
            )
            return status

    def recover_expired(self):
        with self.transaction() as db:
            rows = db.execute(
                "SELECT * FROM jobs WHERE status='running' AND lease_until<=?",
                (self.clock(),),
            ).fetchall()
            for row in rows:
                status = (
                    "queued" if row["attempts"] < self.maximum_attempts else "failed"
                )
                db.execute(
                    "UPDATE jobs SET status=?,ready_at=?,lease_until=NULL,token=NULL,worker=NULL,error=? WHERE id=?",
                    (status, self.clock(), "worker lease expired", row["id"]),
                )
            return len(rows)

    def list(self):
        with self.connection() as db:
            return [
                {
                    **dict(row),
                    "result": json.loads(row["result"]) if row["result"] else None,
                }
                for row in db.execute("SELECT * FROM jobs ORDER BY ready_at,id")
            ]
