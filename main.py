#!/usr/bin/env python3
"""BBC 5‑Minute Bulletin grabber & player — catalogue, chooser, temp‑seek.

Patch – *avoid re‑downloading latest when chosen from list* + *file end fixed*
============================================================================
If the user selects the bulletin that is already cached in **latest_episode.mp3**
(the most‑recent one), the script now re‑uses that file instead of fetching it
again.  Only older items are downloaded to a temporary file.  The `if __name__`
block is restored so the script is runnable.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import queue
import re
import select
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional

import click
import feedparser
import requests
import sounddevice as sd

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
FEED_URL = (
    "https://podcast.voice.api.bbci.co.uk/rss/audio/p002vsmz?api_key="
    "Wbek5zSqxz0Hk1blo5IBqbd9SCWIfNbT"
)
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / ".bvc5min_state.json"
CACHED_FILE = SCRIPT_DIR / "latest_episode.mp3"  # permanent cache (latest only)
PCM_RATE = 48_000
CHANNELS = 2
_CUR_UTC = _dt.datetime.now(_dt.timezone.utc)

# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except FileNotFoundError:
        return {}


def _save_state(state: dict) -> None:
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except PermissionError as e:
        click.echo(f"Error writing state file: {e}", err=True)

# ---------------------------------------------------------------------------
# Feed interaction
# ---------------------------------------------------------------------------

def _parse_entry(entry) -> dict:
    title = entry.title
    url = entry.enclosures[0].href
    if getattr(entry, "published_parsed", None):
        ts = _dt.datetime(*entry.published_parsed[:6], tzinfo=_dt.timezone.utc)
    else:
        ts = _CUR_UTC
    return {"title": title, "url": url, "published": ts.isoformat()}


def _fetch_latest_episode() -> dict:
    feed = feedparser.parse(FEED_URL)
    if not feed.entries:
        click.echo("No entries in feed", err=True)
        sys.exit(1)
    return _parse_entry(feed.entries[0])

# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _download(url: str, dest: Path) -> None:
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(1024 * 64):
            f.write(chunk)

# ---------------------------------------------------------------------------
# FFprobe duration helper
# ---------------------------------------------------------------------------

def _duration(src: str | Path):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(src),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return float(res.stdout.strip())
    except ValueError:
        return None

# ---------------------------------------------------------------------------
# Audio Player (unchanged)
# ---------------------------------------------------------------------------
class Player:
    def __init__(self, source: str | Path, duration: float | None):
        self.source = str(source)
        self.duration = duration or float("inf")
        self.position = 0.0
        self.pcm_q: queue.Queue[bytes | None] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.proc: subprocess.Popen | None = None
        self._lock = threading.Lock()

    def start(self):
        self._spawn_stream()
        threading.Thread(target=self._ticker, daemon=True).start()
        threading.Thread(target=self._render, daemon=True).start()
        threading.Thread(target=self._controls, daemon=True).start()

    def _ticker(self):
        while not self.stop_event.is_set():
            time.sleep(1)
            if not self.pause_event.is_set():
                with self._lock:
                    self.position = min(self.position + 1, self.duration)

    def _spawn_stream(self):
        self.pcm_q = queue.Queue()
        if self.proc:
            self.proc.terminate()
        with self._lock:
            start = max(0.0, min(self.position, self.duration))
        cmd = [
            "ffmpeg",
            "-loglevel",
            "quiet",
            "-ss",
            str(start),
            "-i",
            self.source,
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            "-ac",
            str(CHANNELS),
            "-ar",
            str(PCM_RATE),
            "pipe:1",
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        threading.Thread(target=self._feed_queue, daemon=True).start()

    def _feed_queue(self):
        while not self.stop_event.is_set() and self.proc:
            data = self.proc.stdout.read(4096)
            if not data:
                break
            self.pcm_q.put(data)
        self.pcm_q.put(None)

    def _render(self):
        with sd.RawOutputStream(samplerate=PCM_RATE, channels=CHANNELS, dtype="int16") as out:
            while not self.stop_event.is_set():
                chunk = self.pcm_q.get()
                if chunk is None:
                    break
                while self.pause_event.is_set() and not self.stop_event.is_set():
                    time.sleep(0.1)
                out.write(chunk)

    def _controls(self):
        click.echo("Controls: a -10s, d +10s, space pause/play, q quit")
        if os.name == "nt":
            import msvcrt
        else:
            import termios, tty
        while not self.stop_event.is_set():
            if os.name == "nt":
                if not msvcrt.kbhit():
                    time.sleep(0.1)
                    continue
                ch = msvcrt.getwch()
            else:
                fd = sys.stdin.fileno()
                old = termios.tcgetattr(fd)
                tty.setcbreak(fd)
                try:
                    if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                        ch = sys.stdin.read(1)
                    else:
                        time.sleep(0.1)
                        continue
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)
            if ch.lower() == "q":
                self.stop_event.set(); return
            if ch == " ":
                if self.pause_event.is_set():
                    self.pause_event.clear()
                else:
                    self.pause_event.set()
            if ch.lower() == "a":
                with self._lock:
                    self.position = max(self.position - 10.0, 0.0)
                self._spawn_stream()
            if ch.lower() == "d":
                with self._lock:
                    self.position = min(self.position + 10.0, self.duration)
                self._spawn_stream()

# ---------------------------------------------------------------------------
# Catalogue helpers
# ---------------------------------------------------------------------------

def _ensure_catalogue_has(latest: dict) -> dict:
    state = _load_state()
    eps: List[dict] = state.get("episodes", [])
    if not any(ep["url"] == latest["url"] for ep in eps):
        eps.append(latest)
        eps.sort(key=lambda e: e["published"])
        state.update(
            episodes=eps,
            latest_title=latest["title"],
            latest_url=latest["url"],
        )
        _save_state(state)
    return state


def _list_for_date(state: dict, day: _dt.date) -> List[dict]:
    return [
        ep
        for ep in sorted(state.get("episodes", []), key=lambda e: e["published"])
        if _dt.datetime.fromisoformat(ep["published"]).date() == day
    ]

# ---------------------------------------------------------------------------
# Playback helpers
# ---------------------------------------------------------------------------

def _play_local(path: Path, title: str, delete_after: bool = False):
    player = Player(path, _duration(path))
    click.echo(f"Starting playback → {title}")
    player.start()
    try:
        while not player.stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        player.stop_event.set()
    if delete_after:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _play_episode(ep: dict, latest: dict):
    """Play *ep*: reuse cached file if it is the latest; otherwise temp‑download."""
    if ep["url"] == latest["url"] and CACHED_FILE.exists():
        _play_local(CACHED_FILE, ep["title"])
        return
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3", dir=SCRIPT_DIR) as tmp:
        temp_path = Path(tmp.name)
    click.echo("Downloading bulletin for local seek …")
    _download(ep["url"], temp_path)
    _play_local(temp_path, ep["title"], delete_after=True)


def _ensure_latest_cached(latest: dict):
    if not CACHED_FILE.exists() or _load_state().get("latest_url") != latest["url"]:
        click.echo("Downloading newest episode …")
        _download(latest["url"], CACHED_FILE)
    else:
        click.echo("Using cached episode file.")

# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------
_DATE_PATTERNS = [
    (re.compile(r"^(\d{1,2})-(\d{1,2})$"), "%d-%m"),          # DD-MM (current year)
    (re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{2})$"), "%d-%m-%y"),  # DD-MM-YY
    (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$"), "%Y-%m-%d"),  # YYYY-MM-DD
]

def _parse_cli_date(text: str) -> Optional[_dt.date]:
    """Parse user‑supplied CLI date text into a `datetime.date` (UTC)."""
    text = text.strip()
    for rx, fmt in _DATE_PATTERNS:
        if rx.match(text):
            if fmt == "%d-%m":
                d, m = map(int, text.split("-"))
                return _dt.date(_CUR_UTC.year, m, d)
            if fmt == "%d-%m-%y":
                d, m, yy = map(int, text.split("-"))
                return _dt.date(2000 + yy, m, d)
            return _dt.datetime.strptime(text, fmt).date()
    return None

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--date", "date_text", type=str, help="DATE in DD‑MM, DD‑MM‑YY, or YYYY‑MM‑DD (UTC)")
@click.option("--today", "today_flag", is_flag=True, help="Shortcut for today's date (UTC)")
@click.option("--list", "list_only", is_flag=True, help="List catalogue and exit.")
def cli(date_text: Optional[str], today_flag: bool, list_only: bool):
    """Play the BBC 5‑minute bulletin (download once, seek everywhere)."""
    if date_text and today_flag:
        click.echo("--date and --today are mutually exclusive", err=True)
        sys.exit(1)

    latest = _fetch_latest_episode()
    state = _ensure_catalogue_has(latest)

    if list_only:
        for ep in state.get("episodes", []):
            click.echo(f"{ep['published']}  |  {ep['title']}")
        return

    target_date: Optional[_dt.date] = None
    if today_flag:
        target_date = _CUR_UTC.date()
    elif date_text:
        target_date = _parse_cli_date(date_text)
        if target_date is None:
            click.echo("Unrecognised date format", err=True);
            sys.exit(1)

    if target_date is not None:
        bulletins = _list_for_date(state, target_date)
        if not bulletins:
            click.echo("No bulletins stored for that date");
            return
        if len(bulletins) == 1:
            click.echo(f"Auto‑selecting {bulletins[0]['title']}")
            # Ensure latest is cached if it's the latest
            if bulletins[0]["url"] == latest["url"]:
                _ensure_latest_cached(latest)
            _play_episode(bulletins[0], latest)
            return
        click.echo(f"Bulletins for {target_date.isoformat()} (UTC):")
        for idx, ep in enumerate(bulletins, 1):
            hhmm = _dt.datetime.fromisoformat(ep["published"]).strftime("%H:%M")
            click.echo(f"  {idx}) {hhmm} — {ep['title']}")
        choice = click.prompt("Enter number", type=int, default=len(bulletins))
        if not 1 <= choice <= len(bulletins):
            click.echo("Invalid selection");
            return
        selected_ep = bulletins[choice - 1]
        # Ensure latest is cached if it's the latest
        if selected_ep["url"] == latest["url"]:
            _ensure_latest_cached(latest)
        _play_episode(selected_ep, latest)
        return

    _ensure_latest_cached(latest)
    _play_local(CACHED_FILE, latest["title"])


if __name__ == "__main__":
    cli()
