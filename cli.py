"""cli.py - the command-line interface.

Examples:
    python cli.py add <url> [<url> ...]
    python cli.py list
    python cli.py start -c 3 -r 3 -o downloads
    python cli.py retry [id]
    python cli.py remove <id>
"""
import argparse
import asyncio
import os

import db
from worker import run_queue


def cmd_add(args):
    for url in args.urls:
        jid = db.add_job(url)
        print(f"Added #{jid}: {url}" if jid else f"Already in queue: {url}")


def cmd_list(args):
    jobs = db.list_jobs()
    if not jobs:
        print("Queue is empty.")
        return
    print(f"{'ID':<4} {'STATUS':<12} {'TRIES':<6} URL")
    for j in jobs:
        print(f"{j['id']:<4} {j['status']:<12} {j['attempts']:<6} {j['url']}")
        if j["last_error"] and j["status"] != "done":
            print(f"     last error: {j['last_error']}")


def cmd_start(args):
    os.makedirs(args.output, exist_ok=True)
    try:
        asyncio.run(run_queue(args.concurrency, args.retries, args.output))
        print("Queue finished.")
    except KeyboardInterrupt:
        print("\nStopped. Unfinished jobs were saved; run 'start' again to resume.")
    finally:
        db.reset_stale()


def cmd_retry(args):
    n = db.retry_job(args.id)
    print(f"Re-queued {n} failed job(s).")


def cmd_remove(args):
    print("Removed." if db.remove_job(args.id) else "No such job.")


def main():
    p = argparse.ArgumentParser(description="Resilient video download queue")
    sub = p.add_subparsers(dest="command", required=True)

    a = sub.add_parser("add", help="add video links")
    a.add_argument("urls", nargs="+")
    a.set_defaults(func=cmd_add)

    sub.add_parser("list", help="show the queue").set_defaults(func=cmd_list)

    s = sub.add_parser("start", help="start downloading")
    s.add_argument("-c", "--concurrency", type=int, default=3, help="downloads at once")
    s.add_argument("-r", "--retries", type=int, default=3, help="max attempts per link")
    s.add_argument("-o", "--output", default="downloads", help="output folder")
    s.set_defaults(func=cmd_start)

    r = sub.add_parser("retry", help="re-queue failed jobs")
    r.add_argument("id", nargs="?", type=int, help="job id (omit for all failed)")
    r.set_defaults(func=cmd_retry)

    d = sub.add_parser("remove", help="remove a job")
    d.add_argument("id", type=int)
    d.set_defaults(func=cmd_remove)

    db.init_db()
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()