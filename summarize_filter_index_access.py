#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (SCRIPT_DIR / "log_traces" / "perf_context_level_metadata.csv").resolve()
DEFAULT_OUTPUT = (SCRIPT_DIR / "log_traces" / "perf_context_level_metadata_summary_filter_index.csv").resolve()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Aggregate index/filter access counts from perf_context_level_metadata.csv "
            "by level and emit index+filter totals plus ratio metrics."
        )
    )
    p.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Input CSV path (from parse_perf_context_level_metadata.py)",
    )
    p.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV path",
    )
    p.add_argument(
        "--db-size",
        choices=["500GB", "1TB", "2TB", ""],
        default="",
        help="If set, aggregate only the selected DB size.",
    )
    return p.parse_args()


def _size_sort_key(db_size: str) -> Tuple[int, str]:
    order = {"500GB": 1, "1TB": 2, "2TB": 3}
    s = (db_size or "").strip().upper()
    return (order.get(s, 99), s)


def aggregate(input_path: Path, db_size_filter: str = "") -> Dict[Tuple[str, str, float, int], Dict[str, float]]:
    grouped: DefaultDict[Tuple[str, str, float, int], Dict[str, float]] = defaultdict(
        lambda: {"index_access_sum": 0.0, "filter_access_sum": 0.0}
    )

    with input_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"db_size", "filter_kind", "cache_pct", "level", "index_access", "filter_access"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            db_size = (row["db_size"] or "").strip()
            if db_size_filter and db_size.upper() != db_size_filter.upper():
                continue

            filter_kind = (row["filter_kind"] or "").strip()
            try:
                cache_pct = float((row["cache_pct"] or "").strip())
                level = int((row["level"] or "").strip())
                index_access = float((row["index_access"] or 0))
                filter_access = float((row["filter_access"] or 0))
            except (ValueError, TypeError):
                continue

            key = (db_size, filter_kind, cache_pct, level)
            grouped[key]["index_access_sum"] += index_access
            grouped[key]["filter_access_sum"] += filter_access

    return grouped


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    grouped = aggregate(input_path, db_size_filter=args.db_size)

    if not grouped:
        raise ValueError("No valid rows found or no rows match db-size filter.")

    rows: List[Tuple[str, str, float, int, float, float, float, float, float, float]] = []
    for (db_size, filter_kind, cache_pct, level), vals in grouped.items():
        index_sum = vals["index_access_sum"]
        filter_sum = vals["filter_access_sum"]
        total_sum = index_sum + filter_sum

        idx_ratio = (index_sum / total_sum) if total_sum > 0 else 0.0
        flt_ratio = (filter_sum / total_sum) if total_sum > 0 else 0.0
        idx_vs_filter = (index_sum / filter_sum) if filter_sum > 0 else 0.0

        rows.append(
            (
                db_size,
                filter_kind,
                cache_pct,
                level,
                index_sum,
                filter_sum,
                total_sum,
                idx_ratio,
                flt_ratio,
                idx_vs_filter,
            )
        )

    rows.sort(key=lambda r: (_size_sort_key(r[0]), r[1], r[2], r[3]))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "db_size",
                "filter_kind",
                "cache_pct",
                "level",
                "index_access_sum",
                "filter_access_sum",
                "total_access_sum",
                "index_share_ratio",
                "filter_share_ratio",
                "index_vs_filter_ratio",
            ]
        )
        for row in rows:
            w.writerow(
                [
                    row[0],
                    row[1],
                    f"{row[2]}",
                    row[3],
                    f"{int(row[4])}",
                    f"{int(row[5])}",
                    f"{int(row[6])}",
                    f"{row[7]:.8f}",
                    f"{row[8]:.8f}",
                    f"{row[9]:.8f}",
                ]
            )

    print(f"[DONE] aggregated rows={len(rows)}")
    print(f"[DONE] output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
