"""downloader.py - runs yt-dlp for one link and classifies errors.

TransientError  = might work if we try again (network problem, timeout...)
PermanentError  = retrying is pointless (private/removed video, bad link...)
"""
import asyncio
import sys


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


# If yt-dlp's error text contains one of these, don't bother retrying.
PERMANENT_HINTS = (
    "private video",
    "video unavailable",
    "has been removed",
    "does not exist",
    "unsupported url",
    "is not a valid url",
    "http error 404",
    "http error 410",
    "no module named yt_dlp",
)


async def download(url, out_dir):
    """Download one video. Raises TransientError / PermanentError on failure."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "yt_dlp",
        "--no-playlist",
        "--continue",  # resume partial .part files
        "--newline",
        "-o", f"{out_dir}/%(title)s [%(id)s].%(ext)s",
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, err = await proc.communicate()
    except asyncio.CancelledError:  # Ctrl+C: stop the child process too
        proc.kill()
        await proc.wait()
        raise

    if proc.returncode == 0:
        return

    text = err.decode(errors="replace").strip()
    last_line = text.splitlines()[-1] if text else "unknown error"
    if any(hint in text.lower() for hint in PERMANENT_HINTS):
        raise PermanentError(last_line)
    raise TransientError(last_line)