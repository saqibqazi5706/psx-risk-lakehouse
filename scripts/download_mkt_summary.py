import csv, gzip, io, os, random, time, zipfile
import datetime as dt
import requests

# ---- Settings ----
BASE_DIR = "/content/drive/MyDrive/psx_raw"
SAVE_DIR = f"{BASE_DIR}/mkt_summary"
LOG_FILE = f"{BASE_DIR}/download_log.csv"
DELAY    = 60
URL      = "https://dps.psx.com.pk/download/mkt_summary/{date}.Z"
HEADERS  = {"User-Agent": "psx-risk-lakehouse academic project (FAST-NUCES Lahore; l242506@lhr.nu.edu.pk)"}


def _archive_type(content):
    if not isinstance(content, bytes):
        return None
    if content[:2] == b"PK":
        return "zip"
    if content[:2] == b"\x1f\x8b":
        return "gzip"
    return None


def _read_lines(path):
    with open(path, "rb") as f:
        data = f.read()
    kind = _archive_type(data)
    if kind == "zip":
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            text = z.read(z.namelist()[0])
    elif kind == "gzip":
        text = gzip.decompress(data)
    else:
        raise ValueError("Unknown file format")
    return kind, text.decode("latin-1").splitlines()


def _log(date, status, nbytes, result):
    os.makedirs(BASE_DIR, exist_ok=True)
    new = not os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["date", "status", "bytes", "result", "logged_at"])
        w.writerow([date, status, nbytes, result, dt.datetime.now().isoformat(timespec="seconds")])


def _fetch(date):
    try:
        r = requests.get(URL.format(date=date), headers=HEADERS, timeout=60)
        return r.status_code, r.content
    except requests.RequestException as e:
        return None, str(e)


def _known_no_file_dates():
    """Dates the log already recorded as 404 (holidays) -- don't ask PSX again."""
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE) as f:
        return {row["date"] for row in csv.DictReader(f) if row["status"] == "404"}


def peek(path, n=3):
    kind, lines = _read_lines(path)
    print(f"{os.path.basename(path)} [{kind}]: {len(lines)} rows, {len(lines[0].split('|'))} columns")
    for line in lines[:n]:
        print("   ", line)


def download_range(start, end, delay=DELAY, max_no_file_streak=10):
    day, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    known_holidays = _known_no_file_dates()
    saved = skipped = no_file = streak = 0

    while day <= last:
        d = day.isoformat()
        path = f"{SAVE_DIR}/{day.year}/{d}.Z"
        is_weekend = day.weekday() >= 5
        day += dt.timedelta(days=1)

        if is_weekend:
            continue
        if os.path.exists(path) or d in known_holidays:
            skipped += 1
            streak = 0
            continue

        status, content = _fetch(d)

        if status in (403, 429):
            _log(d, status, len(content), "BLOCKED")
            print(f"STOPPED at {d}: PSX returned {status}. Do not retry today.")
            break
        kind = _archive_type(content) if status == 200 else None
        if kind:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(content)
            _log(d, status, len(content), f"saved_{kind}")
            saved += 1
            streak = 0
        else:
            _log(d, status, len(content) if isinstance(content, bytes) else 0, "no_file")
            no_file += 1
            streak += 1
            if streak >= max_no_file_streak:
                print(f"STOPPED at {d}: {streak} days in a row with no file. Check what PSX is returning.")
                break

        if (saved + no_file) % 25 == 0 and (saved + no_file) > 0:
            print(f"...{d}: saved={saved}, no_file={no_file}, already_had={skipped}")
        time.sleep(delay + random.uniform(0, 3))

    print(f"Done. saved={saved}, no_file={no_file}, already_had={skipped}")

print("Ready.")
