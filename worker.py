"""worker.py - the asynchronous part: several downloads at the same time."""
import asyncio
import random

import db
from downloader import PermanentError, TransientError, download


async def run_queue(concurrency=3, max_retries=3, out_dir="downloads"):
    recovered = db.reset_stale()
    if recovered:
        print(f"Recovered {recovered} interrupted job(s) from last time.")

    slots = asyncio.Semaphore(concurrency)  # max downloads at once
    running = set()

    async def process(job):
        jid, url = job["id"], job["url"]
        try:
            print(f"[{jid}] START   (attempt {job['attempts']}) {url}")
            await download(url, out_dir)
            db.mark_done(jid)
            print(f"[{jid}] DONE    {url}")
        except PermanentError as e:
            db.mark_failed(jid, str(e))
            print(f"[{jid}] FAILED  (permanent) {e}")
        except TransientError as e:
            if job["attempts"] >= max_retries:
                db.mark_failed(jid, str(e))
                print(f"[{jid}] FAILED  (gave up after {max_retries} tries) {e}")
            else:
                # exponential backoff: 2s, 4s, 8s... plus a little randomness
                delay = min(60, 2 ** job["attempts"]) + random.uniform(0, 1)
                db.mark_retry(jid, str(e), delay)
                print(f"[{jid}] RETRY   in {delay:.0f}s - {e}")
        except asyncio.CancelledError:
            db.requeue_interrupted(jid)
            raise
        finally:
            slots.release()

    try:
        while True:
            await slots.acquire()
            job = db.claim_next_job()
            if job is None:
                slots.release()
                if db.active_count() == 0:
                    break  # nothing left to do
                await asyncio.sleep(1)  # jobs are waiting for their retry time
                continue
            task = asyncio.create_task(process(job))
            running.add(task)
            task.add_done_callback(running.discard)
        if running:
            await asyncio.gather(*running)
    except asyncio.CancelledError:
        for t in list(running):
            t.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        raise