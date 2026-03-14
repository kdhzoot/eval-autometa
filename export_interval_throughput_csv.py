#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from pathlib import Path


DIST_NAMES = {"uniform", "zipfian", "latest"}
WORKLOAD_READ_ONLY_RE = re.compile(r"^(?P<workload>.+)_(?P<read_only>[01])$")
THREADS_RE = re.compile(r"^(?P<threads>\d+)_threads$")
CACHE_PCT_RE = re.compile(r"^(?P<cache_pct>[0-9]+(?:\.[0-9]+)?)p$")
HIMETA_LEVEL_RE = re.compile(r"^himeta_(?P<level>\d+(?:,\d+)*)$")
RESULT_DIR_RE = re.compile(r"^result_\d+$")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate interval throughput from o.ld.rep.run files under a result root "
            "into a single CSV."
        )
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default="results",
        help="Result root to scan (default: results_backup)",
    )
    parser.add_argument(
        "output_csv",
        nargs="?",
        default="interval_throughput_results.csv",
        help="Output CSV path (default: interval_throughput_results.csv)",
    )
    return parser.parse_args()


def infer_db_name(root: Path) -> str:
    name = root.name
    if (
        name
        and name not in DIST_NAMES
        and not THREADS_RE.match(name)
        and not CACHE_PCT_RE.match(name)
        and not WORKLOAD_READ_ONLY_RE.match(name)
    ):
        return name
    return ""


def parse_path(file_path: Path, root: Path):
    rel_parts = list(file_path.relative_to(root).parts)
    if not rel_parts or rel_parts[-1] != "o.ld.rep.run":
        raise ValueError("unexpected file name")

    rel_parts = rel_parts[:-1]
    if rel_parts and RESULT_DIR_RE.match(rel_parts[-1]):
        rel_parts = rel_parts[:-1]

    if len(rel_parts) < 4:
        raise ValueError("path is too short to parse")

    scheme_part = rel_parts[-1]
    cache_pct_part = rel_parts[-2]
    threads_part = rel_parts[-3]
    prefix_parts = rel_parts[:-3]

    distribution = ""
    if prefix_parts and prefix_parts[-1] in DIST_NAMES:
        distribution = prefix_parts[-1]
        prefix_parts = prefix_parts[:-1]

    if not prefix_parts:
        raise ValueError("missing workload directory")

    workload_read_only_part = prefix_parts[-1]
    db_name_parts = prefix_parts[:-1]

    workload_match = WORKLOAD_READ_ONLY_RE.match(workload_read_only_part)
    if not workload_match:
        raise ValueError(f"invalid workload/read_only segment: {workload_read_only_part}")

    threads_match = THREADS_RE.match(threads_part)
    if not threads_match:
        raise ValueError(f"invalid threads segment: {threads_part}")

    cache_pct_match = CACHE_PCT_RE.match(cache_pct_part)
    if not cache_pct_match:
        raise ValueError(f"invalid cache_pct segment: {cache_pct_part}")

    scheme = scheme_part
    level_preference = ""
    himeta_match = HIMETA_LEVEL_RE.match(scheme_part)
    if himeta_match:
        scheme = "himeta"
        level_preference = himeta_match.group("level")

    db_name = "/".join(db_name_parts) if db_name_parts else infer_db_name(root)

    return {
        "db_name": db_name,
        "workload": workload_match.group("workload"),
        "read_only": workload_match.group("read_only"),
        "distribution": distribution,
        "threads": threads_match.group("threads"),
        "cache_pct": cache_pct_match.group("cache_pct"),
        "scheme": scheme,
        "level_preference": level_preference,
    }


def load_row(file_path: Path, metadata: dict):
    row_data = dict(metadata)
    with file_path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != ["secs_elapsed", "interval_qps"]:
            raise ValueError(f"unexpected header: {header}")

        for line_no, row in enumerate(reader, start=2):
            if len(row) != 2:
                raise ValueError(f"invalid row at line {line_no}: {row}")

            secs_elapsed = int(row[0])
            interval_qps = int(row[1])
            row_data[f"{secs_elapsed}s"] = interval_qps
    return row_data


def numeric_cache_pct(cache_pct: str):
    return float(cache_pct)


def sort_key(row: dict):
    return (
        row["workload"],
        row["distribution"],
        numeric_cache_pct(row["cache_pct"]),
        row["scheme"],
        row["level_preference"],
        int(row["threads"]),
        row["db_name"],
        row["read_only"],
    )


def main():
    args = parse_args()
    root = Path(args.input_dir).resolve()
    output_csv = Path(args.output_csv).resolve()

    if not root.is_dir():
        print(f"Error: result root not found: {root}", file=sys.stderr)
        return 1

    all_rows = []
    interval_columns = set()
    file_count = 0

    for file_path in sorted(root.glob("**/o.ld.rep.run")):
        metadata = parse_path(file_path.resolve(), root)
        row = load_row(file_path.resolve(), metadata)
        interval_columns.update(
            key for key in row.keys() if key.endswith("s") and key[:-1].isdigit()
        )
        all_rows.append(row)
        file_count += 1

    all_rows.sort(key=sort_key)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    metadata_fields = [
        "db_name",
        "workload",
        "read_only",
        "distribution",
        "threads",
        "cache_pct",
        "scheme",
        "level_preference",
    ]
    ordered_interval_fields = sorted(
        interval_columns, key=lambda value: int(value[:-1])
    )
    fieldnames = metadata_fields + ordered_interval_fields

    with output_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(
        f"Wrote {output_csv} "
        f"({len(all_rows)} rows, {len(ordered_interval_fields)} interval columns from {file_count} files)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
