"""db.py - the persistent queue (SQLite).

Every video link is a "job" row. Because the queue lives in a file,
it survives crashes, Ctrl+C and reboots.

Job status flow:
    queued -> downloading -> done
                         -> queued (retry later, with a delay)
                         -> failed (gave up)
"""
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "vidqueue.db"


@contextmanager
def db():
    # isolation_level=None -> we control transactions ourselves
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with db() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL UNIQUE,
                status      TEXT NOT NULL DEFAULT 'queued',
                attempts    INTEGER NOT NULL DEFAULT 0,
                next_try_at REAL NOT NULL DEFAULT 0,
                last_error  TEXT,
                created_at  REAL NOT NULL
            )"""
        )


def add_job(url):
    """Add a link. Returns the new id, or None if it is already in the queue."""
    try:
        with db() as c:
            cur = c.execute(
                "INSERT INTO jobs (url, created_at) VALUES (?, ?)", (url, time.time())
            )
            return cur.lastrowid
    except sqlite3.IntegrityError:
        return None


def list_jobs():
    with db() as c:
        return c.execute("SELECT * FROM jobs ORDER BY id").fetchall()


def claim_next_job():
    """Atomically take the next ready job and mark it 'downloading'."""
    with db() as c:
        c.execute("BEGIN IMMEDIATE")  # lock so two workers never grab the same job
        row = c.execute(
            """SELECT * FROM jobs
               WHERE status = 'queued' AND next_try_at <= ?
               ORDER BY id LIMIT 1""",
            (time.time(),),
        ).fetchone()
        if row is None:
            c.execute("COMMIT")
            return None
        c.execute(
            "UPDATE jobs SET status='downloading', attempts=attempts+1 WHERE id=?",
            (row["id"],),
        )
        c.execute("COMMIT")
        return dict(row, attempts=row["attempts"] + 1)


def mark_done(job_id):
    with db() as c:
        c.execute(
            "UPDATE jobs SET status='done', last_error=NULL WHERE id=?", (job_id,)
        )


def mark_failed(job_id, error):
    with db() as c:
        c.execute(
            "UPDATE jobs SET status='failed', last_error=? WHERE id=?", (error, job_id)
        )


def mark_retry(job_id, error, delay_seconds):
    with db() as c:
        c.execute(
            "UPDATE jobs SET status='queued', last_error=?, next_try_at=? WHERE id=?",
            (error, time.time() + delay_seconds, job_id),
        )


def requeue_interrupted(job_id):
    """Ctrl+C happened mid-download: put it back and don't count the attempt."""
    with db() as c:
        c.execute(
            """UPDATE jobs SET status='queued', attempts=MAX(attempts-1, 0)
               WHERE id=? AND status='downloading'""",
            (job_id,),
        )


def reset_stale():
    """Crash recovery: anything stuck in 'downloading' goes back to 'queued'."""
    with db() as c:
        cur = c.execute("UPDATE jobs SET status='queued' WHERE status='downloading'")
        return cur.rowcount


def active_count():
    """How many jobs are still waiting or running."""
    with db() as c:
        return c.execute(
            "SELECT COUNT(*) FROM jobs WHERE status IN ('queued','downloading')"
        ).fetchone()[0]


def retry_job(job_id=None):
    """Re-queue one failed job, or all failed jobs if job_id is None."""
    with db() as c:
        sql = "UPDATE jobs SET status='queued', attempts=0, next_try_at=0 WHERE status='failed'"
        if job_id is not None:
            cur = c.execute(sql + " AND id=?", (job_id,))
        else:
            cur = c.execute(sql)
        return cur.rowcount


def remove_job(job_id):
    with db() as c:
        return c.execute("DELETE FROM jobs WHERE id=?", (job_id,)).rowcount