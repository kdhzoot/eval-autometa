#!/usr/bin/env python3

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = (SCRIPT_DIR / "log_traces").resolve()
DEFAULT_OUTPUT = (DEFAULT_ROOT / "perf_parsed.csv").resolve()


METRICS = [
    "block_cache_hit_count",
    "block_cache_miss_count",
    "block_cache_index_hit_count",
    "block_cache_index_miss_count",
    "block_cache_filter_hit_count",
    "block_cache_filter_miss_count",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Parse RocksDB PERF_CONTEXT lines from *.out files and export "
            "level-wise index/filter cache hit/miss stats."
        )
    )
    p.add_argument(
        "--root",
        default=str(DEFAULT_ROOT),
        help="Root directory to scan recursively for *.out files.",
    )
    p.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path.",
    )
    return p.parse_args()


def parse_run_meta(out_path: Path) -> Tuple[str, str, str, str]:
    # Expected path pattern:
    # .../perf_<ts>_<filter_kind>_<db_size>/<cache_dir>/*.out
    # e.g. perf_20260226_061155_partitioned_1TB/partitioned_10/....
    run_dir = out_path.parents[1].name
    cache_dir = out_path.parent.name

    m = re.match(r"^perf_\d{8}_\d{6}_(full|partitioned)_(.+)$", run_dir)
    if m:
        filter_kind = m.group(1)
        db_size = m.group(2)
    else:
        filter_kind = ""
        db_size = ""

    # cache dir like "partitioned_10", "full_0p1"
    cache_pct = ""
    m2 = re.match(r"^(?:full|partitioned)_(.+)$", cache_dir)
    if m2:
        cache_pct = m2.group(1).replace("p", ".")

    return run_dir, filter_kind, db_size, cache_pct


def find_perf_context_line(out_path: Path) -> Optional[str]:
    # Keep only the last long PERF_CONTEXT payload line.
    # In db_bench output this is the line after "PERF_CONTEXT:"
    last = None
    with out_path.open("r", errors="replace") as f:
        for line in f:
            if "block_cache_index_hit_count" in line and "@level" in line:
                last = line.strip()
    return last


def parse_metric_levels(payload: str, metric: str) -> Dict[int, int]:
    # Metric appears twice in payload:
    # 1) global scalar
    # 2) per-level "x@levelN, ..."
    # We need per-level occurrence (contains @level).
    pat = re.compile(
        rf"{re.escape(metric)}\s*=\s*(.*?)(?=,\s*[A-Za-z_][A-Za-z0-9_]*\s*=|$)"
    )
    matches = [m.group(1).strip() for m in pat.finditer(payload)]
    target = ""
    for s in matches:
        if "@level" in s:
            target = s
    if not target:
        return {}

    out: Dict[int, int] = {}
    for num, lvl in re.findall(r"(\d+)@level(\d+)", target):
        out[int(lvl)] = int(num)
    return out


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    output = Path(args.output).resolve()
    out_files = sorted(root.rglob("*.out"))

    rows: List[List[str]] = []
    for out_path in out_files:
        payload = find_perf_context_line(out_path)
        if not payload:
            continue

        run_dir, filter_kind, db_size, cache_pct = parse_run_meta(out_path)
        parsed = {m: parse_metric_levels(payload, m) for m in METRICS}
        levels = sorted(
            set().union(
                parsed["block_cache_hit_count"].keys(),
                parsed["block_cache_miss_count"].keys(),
                parsed["block_cache_index_hit_count"].keys(),
                parsed["block_cache_index_miss_count"].keys(),
                parsed["block_cache_filter_hit_count"].keys(),
                parsed["block_cache_filter_miss_count"].keys(),
            )
        )

        for lv in levels:
            bh = parsed["block_cache_hit_count"].get(lv, 0)
            bm = parsed["block_cache_miss_count"].get(lv, 0)
            ih = parsed["block_cache_index_hit_count"].get(lv, 0)
            im = parsed["block_cache_index_miss_count"].get(lv, 0)
            fh = parsed["block_cache_filter_hit_count"].get(lv, 0)
            fm = parsed["block_cache_filter_miss_count"].get(lv, 0)

            ba = bh + bm
            ia = ih + im
            fa = fh + fm
            ma = ia + fa
            mh = ih + fh
            mm = im + fm
            bhr = (bh / ba) if ba > 0 else 0.0
            # Data block = total block - metadata block
            da = max(0, ba - ma)
            dh = max(0, bh - mh)
            dm = max(0, bm - mm)
            dhr = (dh / da) if da > 0 else 0.0

            ihr = (ih / ia) if ia > 0 else 0.0
            fhr = (fh / fa) if fa > 0 else 0.0
            mhr = (mh / ma) if ma > 0 else 0.0

            rows.append(
                [
                    str(out_path),
                    run_dir,
                    filter_kind,
                    db_size,
                    cache_pct,
                    str(lv),
                    str(ba),
                    str(bh),
                    str(bm),
                    f"{bhr:.6f}",
                    str(ma),
                    str(mh),
                    str(mm),
                    f"{mhr:.6f}",
                    str(ia),
                    str(ih),
                    str(im),
                    f"{ihr:.6f}",
                    str(fa),
                    str(fh),
                    str(fm),
                    f"{fhr:.6f}",
                    str(da),
                    str(dh),
                    str(dm),
                    f"{dhr:.6f}",
                ]
            )

    def db_size_key(db_size: str) -> float:
        s = (db_size or "").strip().upper()
        if s.endswith("TB"):
            try:
                return float(s[:-2]) * 1024.0
            except ValueError:
                return float("inf")
        if s.endswith("GB"):
            try:
                return float(s[:-2])
            except ValueError:
                return float("inf")
        return float("inf")

    def cache_pct_key(cache_pct: str) -> float:
        try:
            return float(cache_pct)
        except (TypeError, ValueError):
            return float("inf")

    # Sort by: db_size -> cache_pct -> level.
    rows.sort(
        key=lambda r: (
            db_size_key(r[3]),
            cache_pct_key(r[4]),
            int(r[5]),
            r[2],  # filter_kind
            r[1],  # run_dir
            r[0],  # out_file
        )
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "out_file",
                "run_dir",
                "filter_kind",
                "db_size",
                "cache_pct",
                "level",
                "block_access",
                "block_hit",
                "block_miss",
                "block_hit_rate",
                "metadata_access",
                "metadata_hit",
                "metadata_miss",
                "metadata_hit_rate",
                "index_access",
                "index_hit",
                "index_miss",
                "index_hit_rate",
                "filter_access",
                "filter_hit",
                "filter_miss",
                "filter_hit_rate",
                "data_access",
                "data_hit",
                "data_miss",
                "data_hit_rate",
            ]
        )
        w.writerows(rows)

    print(f"[DONE] parsed {len(rows)} rows from {len(out_files)} .out files")
    print(f"[DONE] output: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
