# Resilient CLI-Based Asynchronous Video Acquisition and Queue Management System

A command-line tool that keeps a persistent queue of video links and downloads
many of them at the same time, with automatic retries. It uses yt-dlp and ffmpeg.

## Setup
pip install -r requirements.txt
(ffmpeg must also be installed)

## Usage
python cli.py add LINK1 LINK2
python cli.py list
python cli.py start -c 3
python cli.py retry
python cli.py remove 1

## Features
- Persistent queue (SQLite), safe across restarts
- Asynchronous downloads (asyncio + semaphore)
- Retry with exponential backoff (2s, 4s, 8s)
- Crash recovery and safe Ctrl+C
- Duplicate links are ignored

Download only content you have the right to download.
