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
from rich.progress import Progress, BarColumn, TextColumn, ProgressColumn
from rich.text import Text
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------


FEED_URL = (
    "https://podcast.voice.api.bbci.co.uk/rss/audio/p002vsmz?api_key="
    "Wbek5zSqxz0Hk1blo5IBqbd9SCWIfNbT"
)
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / ".bbcli_state.json"
CACHED_FILE = SCRIPT_DIR / "latest_episode.mp3"
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


def _parse_duration(dur):
    if dur is None:
        return None
    if isinstance(dur, int):
        return dur
    if isinstance(dur, str):
        parts = dur.strip().split(":")
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            elif len(parts) == 1:
                return int(parts[0])
        except Exception:
            return None
    return None


def _parse_entry(entry) -> dict:
    title = entry.title
    url = entry.enclosures[0].href
    guid = getattr(entry, "id", getattr(entry, "guid", entry.link))
    if getattr(entry, "published_parsed", None):
        ts = _dt.datetime(*entry.published_parsed[:6], tzinfo=_dt.timezone.utc)
    else:
        ts = _CUR_UTC
    dur = None
    if getattr(entry, "itunes_duration", None):
        dur = _parse_duration(entry.itunes_duration)
    return {
        "guid": guid,
        "title": title,
        "url": url,
        "published": ts.isoformat(),
        "duration": dur,
    }


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
# Audio Player
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
        self.console = Console()
        self.progress = None
        self.task_id = None

    def start(self):
        self._spawn_stream()
        threading.Thread(target=self._ticker, daemon=True).start()
        threading.Thread(target=self._render, daemon=True).start()
        threading.Thread(target=self._controls, daemon=True).start()
        threading.Thread(target=self._progress_bar, daemon=True).start()

    def _progress_bar(self):
        if not self.duration or self.duration == float("inf"):
            return
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeColumn(),
            console=self.console,
            transient=True,
        ) as progress:
            self.progress = progress
            self.task_id = progress.add_task("Playing", total=self.duration)
            finished = False
            paused = False
            self.console.print(
                "[dim]Controls: a/← -10s, d/→ +10s, space pause/play, q quit[/dim]"
            )
            while not self.stop_event.is_set():
                with self._lock:
                    pos = self.position
                progress.update(self.task_id, completed=pos)
                if self.pause_event.is_set():
                    if not paused:
                        progress.update(self.task_id, description="Paused")
                        paused = True
                elif paused:
                    if pos >= self.duration:
                        progress.update(self.task_id, description="Finished")
                    else:
                        progress.update(self.task_id, description="Playing")
                    paused = False
                if (
                    not finished
                    and pos >= self.duration
                    and not self.pause_event.is_set()
                ):
                    progress.update(self.task_id, description="Finished")
                    finished = True
                elif finished and pos < self.duration and not self.pause_event.is_set():
                    progress.update(self.task_id, description="Playing")
                    finished = False
                time.sleep(0.2)

    def _ticker(self):
        while not self.stop_event.is_set():
            time.sleep(1)
            if not self.pause_event.is_set():
                with self._lock:
                    self.position = min(self.position + 1, self.duration)

    def _spawn_stream(self):
        if self.proc:
            self.proc.terminate()
            self.proc = None
        while not self.pcm_q.empty():
            try:
                self.pcm_q.get_nowait()
            except queue.Empty:
                break
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
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        self.proc = proc
        threading.Thread(target=self._feed_queue, args=(proc,), daemon=True).start()

    def _feed_queue(self, proc):
        while not self.stop_event.is_set() and proc.poll() is None:
            data = proc.stdout.read(4096)
            if not data:
                break
            if proc is self.proc:
                self.pcm_q.put(data)
        if proc is self.proc:
            self.pcm_q.put(None)

    def _render(self):
        with sd.RawOutputStream(
            samplerate=PCM_RATE, channels=CHANNELS, dtype="int16"
        ) as out:
            while not self.stop_event.is_set():
                chunk = self.pcm_q.get()
                if chunk is None:
                    if not self.stop_event.is_set():
                        continue
                    else:
                        break
                while self.pause_event.is_set() and not self.stop_event.is_set():
                    time.sleep(0.1)
                out.write(chunk)

    def _controls(self):
        if os.name == "nt":
            import msvcrt
        else:
            import termios
            import tty
        while not self.stop_event.is_set():
            if os.name == "nt":
                if not msvcrt.kbhit():
                    time.sleep(0.1)
                    continue
                ch = msvcrt.getwch()
                if ch == "\xe0":
                    arrow = msvcrt.getwch()
                    if arrow == "K":
                        ch = "LEFT"
                    elif arrow == "M":
                        ch = "RIGHT"
            else:
                fd = sys.stdin.fileno()
                old = termios.tcgetattr(fd)
                tty.setcbreak(fd)
                try:
                    if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                        ch = sys.stdin.read(1)
                        if ch == "\x1b":
                            next1 = sys.stdin.read(1)
                            if next1 == "[":
                                next2 = sys.stdin.read(1)
                                if next2 == "D":
                                    ch = "LEFT"
                                elif next2 == "C":
                                    ch = "RIGHT"
                    else:
                        time.sleep(0.1)
                        continue
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)
            if ch.lower() == "q":
                self.stop_event.set()
                return
            if ch == " ":
                if self.pause_event.is_set():
                    self.pause_event.clear()
                else:
                    self.pause_event.set()
            if ch.lower() == "a" or ch == "LEFT":
                with self._lock:
                    self.position = max(self.position - 10.0, 0.0)
                self._spawn_stream()
            if ch.lower() == "d" or ch == "RIGHT":
                with self._lock:
                    self.position = min(self.position + 10.0, self.duration)
                self._spawn_stream()


# ---------------------------------------------------------------------------
# Catalogue helpers
# ---------------------------------------------------------------------------


def _ensure_catalogue_has(latest: dict) -> dict:
    state = _load_state()
    eps = state.get("episodes", [])
    eps.append(latest)
    uniq = {}
    for ep in eps:
        uniq[ep["guid"]] = ep
    deduped = list(uniq.values())
    deduped.sort(key=lambda e: e["published"])
    prev_latest = state.get("latest_guid")
    state["episodes"] = deduped
    state["latest_guid"] = latest["guid"]
    if prev_latest != latest["guid"]:
        state["finished"] = False
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


def _format_published(
    dt_str: str, with_date: bool = False, with_title: Optional[str] = None
) -> str:
    dt = _dt.datetime.fromisoformat(dt_str)
    base = dt.strftime("%H:%M %d/%m/%Y" if with_date else "%H:%M")
    if with_title:
        return f"{base} — {with_title}"
    return base


def _play_local(
    path: Path, published: str, delete_after: bool = False, guid: str = None
):
    player = Player(path, _duration(path))
    click.echo(f"Starting playback → {_format_published(published, with_date=True)}")
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

    if guid and player.position >= player.duration - 1:
        _mark_episode_finished()


def _play_episode(ep: dict, latest: dict):
    """Play *ep*: reuse cached file if it is the latest; otherwise temp‑download."""
    if ep["guid"] == latest["guid"] and CACHED_FILE.exists():
        _play_local(CACHED_FILE, ep["published"], guid=ep["guid"])
        return
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".mp3", dir=SCRIPT_DIR
    ) as tmp:
        temp_path = Path(tmp.name)
    click.echo("Downloading bulletin for local seek …")
    _download(ep["url"], temp_path)
    _play_local(temp_path, ep["published"], delete_after=True, guid=ep["guid"])


def _mark_episode_finished() -> None:
    state = _load_state()
    state["finished"] = True
    _save_state(state)


def _is_episode_finished() -> bool:
    state = _load_state()
    return bool(state.get("finished", False))


def _confirm_replay_if_finished() -> bool:
    if _is_episode_finished():
        ans = Prompt.ask(
            "You have already finished listening to the latest available episode. "
            "Play again? (y/n)",
            default="n",
        )
        return ans.strip().lower() == "y"
    return True


def _ensure_latest_cached(latest: dict) -> None:
    """
    Make sure *latest* is present in latest_episode.mp3.
    A download occurs only if the cached GUID (file on disk) differs from *latest*.
    """
    state = _load_state()
    cached_guid = state.get("cached_guid")  # GUID of the file on disk
    need_download = True

    if CACHED_FILE.exists() and cached_guid == latest["guid"]:
        if latest.get("duration"):
            cached_dur = _duration(CACHED_FILE) or 0
            if abs(cached_dur - latest["duration"]) < 2:
                need_download = False

    if need_download:
        click.echo("Downloading newest episode …")
        _download(latest["url"], CACHED_FILE)
        state["cached_guid"] = latest["guid"]  # update only after success
        _save_state(state)
    else:
        click.echo("Using cached episode file.")


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------


_DATE_PATTERNS = [
    (re.compile(r"^(\d{1,2})-(\d{1,2})$"), "%d-%m"),  # DD-MM (current year)
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
@click.option(
    "--date", "date_text", type=str, help="DATE in DD‑MM, DD‑MM‑YY, or YYYY‑MM‑DD (UTC)"
)
@click.option(
    "--today", "today_flag", is_flag=True, help="Shortcut for today's date (UTC)"
)
@click.option("--list", "list_only", is_flag=True, help="List catalogue and exit.")
def cli(date_text: Optional[str], today_flag: bool, list_only: bool):
    if date_text and today_flag:
        click.echo("--date and --today are mutually exclusive", err=True)
        sys.exit(1)

    latest = _fetch_latest_episode()
    _ensure_latest_cached(latest)
    state = _ensure_catalogue_has(latest)

    if list_only:
        table = Table(title="Bulletin Catalogue")
        table.add_column("Time", style="cyan")
        table.add_column("Title", style="magenta")
        for ep in state.get("episodes", []):
            table.add_row(
                _format_published(ep["published"], with_date=True), ep["title"]
            )
        Console().print(table)
        return

    target_date: Optional[_dt.date] = None
    if today_flag:
        target_date = _CUR_UTC.date()
    elif date_text:
        target_date = _parse_cli_date(date_text)
        if target_date is None:
            click.echo("Unrecognised date format", err=True)
            sys.exit(1)

    if target_date is not None:
        bulletins = list(reversed(_list_for_date(state, target_date)))
        if not bulletins:
            Console().print("[red]No bulletins stored for that date[/red]")
            return
        if len(bulletins) == 1:
            ep = bulletins[0]
            Console().print(f"Auto‑selecting {_format_published(ep['published'])}")
            if ep["guid"] == latest["guid"]:
                _ensure_latest_cached(latest)
                if not _confirm_replay_if_finished():
                    click.echo("Exiting.")
                    return
            _play_episode(ep, latest)
            return

        table = Table(
            title=f"Bulletins for {target_date}",
            title_justify="left",
            show_header=True,
            header_style="bold",
        )
        table.add_column("#", style="cyan")
        table.add_column("Time", style="green")
        for idx, ep in enumerate(bulletins, 1):
            table.add_row(str(idx), _format_published(ep["published"]))
        Console().print(table)

        while True:
            choice = Prompt.ask("Enter number or 'q' to quit", default="1")
            if isinstance(choice, str) and choice.lower() == "q":
                Console().print("[yellow]Quitting selection.[/yellow]")
                return
            try:
                num_choice = int(choice)
            except ValueError:
                Console().print("[red]Invalid selection[/red]")
                continue
            if not 1 <= num_choice <= len(bulletins):
                Console().print("[red]Invalid selection[/red]")
                continue
            selected_ep = bulletins[num_choice - 1]
            if selected_ep["guid"] == latest["guid"]:
                _ensure_latest_cached(latest)
                if not _confirm_replay_if_finished():
                    click.echo("Exiting.")
                    return
            _play_episode(selected_ep, latest)
            return

    if not _confirm_replay_if_finished():
        click.echo("Exiting.")
        return

    _play_local(CACHED_FILE, latest["published"], guid=latest["guid"])


# ---------------------------------------------------------------------------
# Progress helper column
# ---------------------------------------------------------------------------


class TimeColumn(ProgressColumn):
    def render(self, task):
        elapsed = int(task.completed)
        total = int(task.total)
        return Text.from_markup(
            f"[cyan]{elapsed // 60:02}:{elapsed % 60:02}[/cyan]/"
            f"[magenta]{total // 60:02}:{total % 60:02}[/magenta]"
        )


if __name__ == "__main__":
    cli()
