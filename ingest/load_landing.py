#!/usr/bin/env python3
"""Kaynak dosyaları indirip s3://<bucket>/landing/ altına olduğu gibi yükler.

- TLC linkleri her çalışmada TLC sayfasından okunur, URL tahmin edilmez.
- landing/ değiştirilmez: hedef anahtar zaten varsa dosya atlanır, üzerine yazılmaz.
- Her nesnenin sha256 ve kaynak URL'si S3 metadata'sında tutulur; manifest
  (ops/manifest/) her çalışmanın sonunda bu metadata'dan yeniden üretilir.

Yalnızca stdlib ve AWS CLI kullanır (Amazon Linux 2023'te ikisi de kurulu).

Örnek:
    python3 load_landing.py --bucket B --only tlc-trips --start 2024-01 --end 2024-01
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
TLC_DATASETS = ("fhvhv", "yellow")
TLC_REFERENCE = (
    "taxi_zone_lookup.csv",
    "taxi_zones.zip",
    "data_dictionary_trip_records_hvfhs.pdf",
    "data_dictionary_trip_records_yellow.pdf",
)

# Central Park (NOAA KNYC istasyonu)
WEATHER_LAT, WEATHER_LON = 40.7789, -73.9692
WEATHER_HOURLY = (
    "temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,snow_depth,"
    "weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m"
)
# Open-Meteo arşivi birkaç gün geriden gelir; bu kadar gün öncesine kadar iste.
WEATHER_LAG_DAYS = 7

HOLIDAY_YEARS = range(2023, 2027)

# NYC Open Data: "NYC Permitted Event Information - Historical"
EVENTS_DATASET = "bkfu-528j"
EVENTS_API = f"https://data.cityofnewyork.us/resource/{EVENTS_DATASET}"

GROUPS = ("tlc-trips", "tlc-reference", "weather", "holidays", "events")
USER_AGENT = "nyc-taxi-aws-loader/1.0"


def log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Hedefler: (s3 key, kaynak URL, içerik üretici) üçlüleri
# ---------------------------------------------------------------------------


class Target:
    def __init__(self, key: str, source_url: str, content: bytes | None = None, verify=None):
        self.key = key
        self.source_url = source_url
        self.content = content  # None ise source_url'den indirilir
        self.verify = verify  # verify(path) -> ek metadata; sorun varsa hata fırlatır


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def tlc_links() -> list[str]:
    html = fetch_text(TLC_PAGE)
    hrefs = (h.strip() for h in re.findall(r'href="([^"]+)"', html))
    return sorted({urllib.parse.urljoin(TLC_PAGE, h) for h in hrefs})


def month_range(start: str, end: str) -> list[str]:
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def tlc_trip_targets(links: list[str], datasets: list[str], start: str, end: str | None) -> list[Target]:
    pattern = re.compile(r"/(%s)_tripdata_(\d{4})-(\d{2})\.parquet$" % "|".join(datasets))
    targets = []
    for url in links:
        m = pattern.search(url)
        if not m:
            continue
        ds, year, month = m.groups()
        ym = f"{year}-{month}"
        if ym < start or (end and ym > end):
            continue
        key = f"landing/tlc/{ds}/year={year}/month={month}/{url.rsplit('/', 1)[1]}"
        targets.append(Target(key, url))
    return sorted(targets, key=lambda t: t.key)


def tlc_reference_targets(links: list[str]) -> list[Target]:
    targets = []
    for name in TLC_REFERENCE:
        matches = [u for u in links if u.rsplit("/", 1)[-1] == name]
        if not matches:
            raise SystemExit(f"TLC sayfasında {name} linki bulunamadı")
        targets.append(Target(f"landing/tlc/reference/{name}", matches[0]))
    return targets


def weather_targets(start: str, today: dt.date) -> list[Target]:
    last_day = today - dt.timedelta(days=WEATHER_LAG_DAYS)
    targets = []
    for ym in month_range(start, last_day.strftime("%Y-%m")):
        y, m = map(int, ym.split("-"))
        first = dt.date(y, m, 1)
        next_first = dt.date(y + (m == 12), m % 12 + 1, 1)
        end = min(next_first - dt.timedelta(days=1), last_day)
        complete = end == next_first - dt.timedelta(days=1)
        # Ay tamamlanmadıysa dosya adı bitiş tarihini taşır; sonraki çalışmalar
        # yeni bir anlık görüntü ekler, eskisinin üzerine yazmaz.
        name = f"hourly_{ym}.json" if complete else f"hourly_{first}_{end}.json"
        query = urllib.parse.urlencode({
            "latitude": WEATHER_LAT,
            "longitude": WEATHER_LON,
            "start_date": first.isoformat(),
            "end_date": end.isoformat(),
            "hourly": WEATHER_HOURLY,
            "timezone": "UTC",
        })
        url = f"https://archive-api.open-meteo.com/v1/archive?{query}"
        key = f"landing/weather/open_meteo/central_park/year={y:04d}/month={m:02d}/{name}"
        targets.append(Target(key, url))
    return targets


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    d = dt.date(year, month, 1)
    d += dt.timedelta(days=(weekday - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    d = dt.date(year + (month == 12), month % 12 + 1, 1) - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() - weekday) % 7)


def _observed(d: dt.date) -> dt.date:
    if d.weekday() == 5:  # cumartesi -> cuma
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:  # pazar -> pazartesi
        return d + dt.timedelta(days=1)
    return d


def holiday_target() -> Target:
    """5 U.S.C. 6103'teki 11 federal tatil; tarihler kurallardan hesaplanır."""
    mon, thu = 0, 3
    rows = []
    for y in HOLIDAY_YEARS:
        days = [
            ("New Year's Day", dt.date(y, 1, 1)),
            ("Birthday of Martin Luther King, Jr.", _nth_weekday(y, 1, mon, 3)),
            ("Washington's Birthday", _nth_weekday(y, 2, mon, 3)),
            ("Memorial Day", _last_weekday(y, 5, mon)),
            ("Juneteenth National Independence Day", dt.date(y, 6, 19)),
            ("Independence Day", dt.date(y, 7, 4)),
            ("Labor Day", _nth_weekday(y, 9, mon, 1)),
            ("Columbus Day", _nth_weekday(y, 10, mon, 2)),
            ("Veterans Day", dt.date(y, 11, 11)),
            ("Thanksgiving Day", _nth_weekday(y, 11, thu, 4)),
            ("Christmas Day", dt.date(y, 12, 25)),
        ]
        rows += [(d, _observed(d), name) for name, d in days]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["date", "observed_date", "name"])
    w.writerows((d.isoformat(), o.isoformat(), n) for d, o, n in rows)
    first, last = HOLIDAY_YEARS[0], HOLIDAY_YEARS[-1]
    return Target(
        f"landing/reference/holidays/us_federal_holidays_{first}_{last}.csv",
        "https://www.law.cornell.edu/uscode/text/5/6103",
        buf.getvalue().encode(),
    )


def events_target(start: str, today: dt.date) -> Target:
    """Veri seti değişebilen bir tablo: indirme günü snapshot= klasörüne yazılır."""
    where = f"start_date_time >= '{start}-01T00:00:00'"
    query = urllib.parse.urlencode({"$where": where, "$order": ":id", "$limit": 50_000_000})
    url = f"{EVENTS_API}.csv?{query}"
    count_url = f"{EVENTS_API}.json?" + urllib.parse.urlencode({"$select": "count(*)", "$where": where})

    def verify(path: str) -> dict:
        expected = int(json.loads(fetch_text(count_url))[0]["count"])
        csv.field_size_limit(sys.maxsize)
        with open(path, newline="", encoding="utf-8") as f:
            rows = sum(1 for _ in csv.reader(f)) - 1  # başlık satırı
        if rows != expected:
            raise IOError(f"satır sayısı {rows}, API {expected} diyor")
        return {"row-count": str(rows)}

    key = (f"landing/events/nyc_permitted_events_historical/snapshot={today.isoformat()}/"
           f"permitted_events_since_{start}.csv")
    return Target(key, url, verify=verify)


# ---------------------------------------------------------------------------
# S3 (AWS CLI üzerinden)
# ---------------------------------------------------------------------------


def aws(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["aws", *args], capture_output=True, text=True)


def s3_head(bucket: str, key: str) -> dict | None:
    r = aws("s3api", "head-object", "--bucket", bucket, "--key", key, "--output", "json")
    if r.returncode == 0:
        return json.loads(r.stdout)
    if "Not Found" in r.stderr or "404" in r.stderr:
        return None
    raise RuntimeError(f"head-object {key}: {r.stderr.strip()}")


def s3_put(bucket: str, key: str, path: str, metadata: dict) -> None:
    r = aws(
        "s3", "cp", path, f"s3://{bucket}/{key}",
        "--metadata", json.dumps(metadata),
        "--checksum-algorithm", "SHA256",
        "--only-show-errors",
    )
    if r.returncode != 0:
        raise RuntimeError(f"upload {key}: {r.stderr.strip()}")


# ---------------------------------------------------------------------------
# İndir + yükle
# ---------------------------------------------------------------------------


def download(url: str, dest: str, attempts: int = 3) -> tuple[int, str]:
    for i in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            h = hashlib.sha256()
            size = 0
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
                expected = r.headers.get("Content-Length")
                while chunk := r.read(8 << 20):
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
            if expected is not None and int(expected) != size:
                raise IOError(f"eksik indirme: {size}/{expected} bayt")
            return size, h.hexdigest()
        except Exception as e:  # noqa: BLE001 - ağ hatalarında tekrar dene
            if i == attempts:
                raise
            log(f"  tekrar ({i}/{attempts}) {url}: {e}")
            time.sleep(5 * i)
    raise AssertionError("unreachable")


def process(bucket: str, t: Target, tmpdir: str, dry_run: bool) -> dict:
    existing = s3_head(bucket, t.key)
    if existing is not None:
        return {"key": t.key, "action": "skipped", "size": existing["ContentLength"]}
    if dry_run:
        return {"key": t.key, "action": "would_upload", "source_url": t.source_url}

    path = os.path.join(tmpdir, hashlib.md5(t.key.encode()).hexdigest())
    started = time.time()
    try:
        if t.content is None:
            size, sha = download(t.source_url, path)
        else:
            with open(path, "wb") as f:
                f.write(t.content)
            size, sha = len(t.content), hashlib.sha256(t.content).hexdigest()
        extra = t.verify(path) if t.verify else {}
        s3_put(bucket, t.key, path, {
            "sha256": sha,
            "source-url": t.source_url,
            "downloaded-at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            **extra,
        })
    finally:
        # Hata olsa da geçici dosyayı sil; yoksa disk dolar ve diğer işler de düşer.
        if os.path.exists(path):
            os.remove(path)
    secs = time.time() - started
    log(f"  yüklendi {t.key} ({size / 1e6:.1f} MB, {secs:.0f} sn)")
    return {"key": t.key, "action": "uploaded", "size": size, "sha256": sha,
            "source_url": t.source_url, "seconds": round(secs, 1)}


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def write_manifest(bucket: str, run: list[dict], run_id: str) -> int:
    r = aws("s3api", "list-objects-v2", "--bucket", bucket, "--prefix", "landing/", "--output", "json")
    if r.returncode != 0:
        raise RuntimeError(f"list-objects-v2: {r.stderr.strip()}")
    objects = json.loads(r.stdout or "{}").get("Contents", [])

    rows = []
    for obj in objects:
        meta = (s3_head(bucket, obj["Key"]) or {}).get("Metadata", {})
        rows.append({
            "path": f"s3://{bucket}/{obj['Key']}",
            "source_url": meta.get("source-url", ""),
            "size": obj["Size"],
            "sha256": meta.get("sha256", ""),
            "downloaded_at": meta.get("downloaded-at", ""),
        })

    with tempfile.TemporaryDirectory() as d:
        csv_path, jsonl_path, run_path = (os.path.join(d, n) for n in ("m.csv", "m.jsonl", "run.jsonl"))
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["path"])
            w.writeheader()
            w.writerows(rows)
        with open(jsonl_path, "w") as f:
            f.writelines(json.dumps(row) + "\n" for row in rows)
        with open(run_path, "w") as f:
            f.writelines(json.dumps(row) + "\n" for row in run)
        for local, key in ((csv_path, "ops/manifest/manifest.csv"),
                           (jsonl_path, "ops/manifest/manifest.jsonl"),
                           (run_path, f"ops/manifest/runs/{run_id}.jsonl")):
            r = aws("s3", "cp", local, f"s3://{bucket}/{key}", "--only-show-errors")
            if r.returncode != 0:
                raise RuntimeError(f"upload {key}: {r.stderr.strip()}")
    return len(rows)


# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True)
    p.add_argument("--only", default=",".join(GROUPS), help=f"virgülle: {','.join(GROUPS)}")
    p.add_argument("--datasets", default=",".join(TLC_DATASETS))
    p.add_argument("--start", default="2024-01", help="YYYY-MM")
    p.add_argument("--end", default=None, help="YYYY-MM (varsayılan: sayfadaki en son ay)")
    p.add_argument("--workers", type=int, default=4)
    # AL2023'te /tmp RAM üzerindeki tmpfs (c7i.large'da 1.9 GB); büyük dosyalar diske yazılsın.
    p.add_argument("--tmpdir", default="/var/tmp" if os.path.isdir("/var/tmp") else None)
    p.add_argument("--dry-run", action="store_true", help="indirme/yükleme yapma, sadece listele")
    a = p.parse_args()

    if a.start < "2022-01":
        raise SystemExit("data before 2022 is out of scope")
    groups = set(a.only.split(","))
    unknown = groups - set(GROUPS)
    if unknown:
        raise SystemExit(f"bilinmeyen grup: {', '.join(sorted(unknown))}")

    targets: list[Target] = []
    if groups & {"tlc-trips", "tlc-reference"}:
        links = tlc_links()
        if "tlc-trips" in groups:
            targets += tlc_trip_targets(links, a.datasets.split(","), a.start, a.end)
        if "tlc-reference" in groups:
            targets += tlc_reference_targets(links)
    if "weather" in groups:
        targets += weather_targets(a.start, dt.date.today())
    if "holidays" in groups:
        targets.append(holiday_target())
    if "events" in groups:
        targets.append(events_target(a.start, dt.date.today()))

    log(f"{len(targets)} hedef, bucket={a.bucket}, dry_run={a.dry_run}")
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results, failures = [], []
    with tempfile.TemporaryDirectory(dir=a.tmpdir) as tmpdir, \
            concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
        futures = {pool.submit(process, a.bucket, t, tmpdir, a.dry_run): t for t in targets}
        for fut in concurrent.futures.as_completed(futures):
            t = futures[fut]
            try:
                res = fut.result()
                results.append(res)
                if res["action"] != "uploaded":
                    log(f"  {res['action']} {t.key}")
            except Exception as e:  # noqa: BLE001 - bir dosyanın hatası diğerlerini durdurmasın
                failures.append({"key": t.key, "action": "failed", "error": str(e)})
                log(f"  HATA {t.key}: {e}")

    counts = {k: sum(r["action"] == k for r in results) for k in ("uploaded", "skipped", "would_upload")}
    uploaded_mb = sum(r.get("size", 0) for r in results if r["action"] == "uploaded") / 1e6
    log(f"bitti: {counts}, hata={len(failures)}, yüklenen={uploaded_mb:.1f} MB")

    if not a.dry_run:
        n = write_manifest(a.bucket, sorted(results + failures, key=lambda r: r["key"]), run_id)
        log(f"manifest yazıldı: {n} nesne -> s3://{a.bucket}/ops/manifest/")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
