#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
from matplotlib import font_manager

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (SCRIPT_DIR / "log_traces" / "perf_context_level_metadata.csv").resolve()
DEFAULT_OUTPUT = (SCRIPT_DIR / "perf_hit_ratio.png").resolve()
DEFAULT_OUTPUT_CSV = (SCRIPT_DIR / "perf_hit_ratio.csv").resolve()

METRIC_CHOICES = [
    "metadata_hit_rate",
    "block_hit_rate",
    "index_hit_rate",
    "filter_hit_rate",
    "data_hit_rate",
]

METRIC_LABELS = {
    "metadata_hit_rate": "Metadata Cache Hit Rate",
    "block_hit_rate": "Block Cache Hit Rate",
    "index_hit_rate": "Index Cache Hit Rate",
    "filter_hit_rate": "Filter Cache Hit Rate",
    "data_hit_rate": "Data Block Cache Hit Rate",
}

LINE_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#17becf",
    "#003f5c",
    "#bc5090",
    "#ef5675",
    "#ffa600",
]


def _require_times_new_roman() -> str:
    available = {font.name for font in font_manager.fontManager.ttflist}
    if "Times New Roman" not in available:
        raise RuntimeError(
            "Times New Roman font is required but not installed. "
            "Install Times New Roman and rerun."
        )
    return "Times New Roman"


plt.rcParams.update(
    {
        "font.family": _require_times_new_roman(),
        "axes.titlesize": 40,
        "axes.labelsize": 34,
        "xtick.labelsize": 24,
        "ytick.labelsize": 24,
        "legend.fontsize": 20,
    }
)


def _size_sort_key(size: str) -> Tuple[int, str]:
    s = (size or "").strip().upper()
    order = {"500GB": 1, "1TB": 2, "2TB": 3}
    if s in order:
        return (order[s], s)
    return (99, s)


def _canonicalize_db_size(size: str) -> str:
    if not size:
        return size
    s = size.strip().upper()
    try:
        if s.endswith("GB"):
            gb = float(s[:-2])
        elif s.endswith("TB"):
            tb = float(s[:-2])
            if tb == int(tb):
                return f"{int(tb)}TB"
            return f"{tb}TB"
        else:
            return s
    except ValueError:
        return s

    if gb == int(gb):
        gb_int = int(gb)
        if gb_int == 1000:
            return "1TB"
        if gb_int == 2000:
            return "2TB"
        return f"{gb_int}GB"
    return f"{gb}GB"


def _read_rows(
    path: Path, metric: str, db_size_filter: str = ""
) -> Dict[Tuple[str, str, float], Dict[int, List[float]]]:
    grouped: Dict[Tuple[str, str, float], Dict[int, List[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"db_size", "filter_kind", "cache_pct", "level", metric}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            try:
                db_size = _canonicalize_db_size(row["db_size"])
                filter_kind = row["filter_kind"].strip()
                cache_pct = float(row["cache_pct"])
                level = int(row["level"])
                value = float(row[metric])
            except (ValueError, KeyError):
                continue
            if db_size_filter and db_size.upper() != db_size_filter.upper():
                continue
            if level == 0:
                continue
            grouped[(db_size, filter_kind, cache_pct)][level].append(value)

    return grouped


def _series_mean(level_map: Dict[int, List[float]]) -> Tuple[List[int], List[float]]:
    levels = sorted(level_map.keys())
    values = [sum(level_map[l]) / len(level_map[l]) for l in levels]
    return levels, values


def write_plot_data_csv(
    grouped: Dict[Tuple[str, str, float], Dict[int, List[float]]],
    output_csv: Path,
    metric: str,
) -> None:
    rows = []
    for key in sorted(grouped.keys(), key=lambda k: (_size_sort_key(k[0]), k[2], k[1])):
        db_size, filter_kind, cache_pct = key
        for level, values in sorted(grouped[key].items()):
            if not values:
                continue
            value = sum(values) / len(values)
            rows.append(
                [
                    db_size,
                    filter_kind,
                    cache_pct,
                    level,
                    f"{value:.12f}",
                    f"{value * 100.0:.12f}",
                ]
            )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "db_size",
                "filter_kind",
                "cache_pct",
                "level",
                "metric_value_ratio",
                "metric_value_pct",
            ]
        )
        w.writerows(rows)


def plot_hit_level(
    grouped: Dict[Tuple[str, str, float], Dict[int, List[float]]],
    output_png: Path,
    metric: str,
) -> None:
    if not grouped:
        raise ValueError("No valid rows found for plot.")

    keys = sorted(
        grouped.keys(),
        key=lambda k: (_size_sort_key(k[0]), k[2], k[1]),
    )

    plt.figure(figsize=(13, 8))
    metric_label = METRIC_LABELS.get(metric, metric)

    for i, key in enumerate(keys):
        db_size, filter_kind, cache_pct = key
        levels, values = _series_mean(grouped[key])
        values_pct = [v * 100.0 for v in values]
        label = f"{db_size} | {filter_kind} | {cache_pct:g}%"
        plt.plot(
            levels,
            values_pct,
            marker="o",
            linewidth=2.1,
            markersize=6,
            color=LINE_COLORS[i % len(LINE_COLORS)],
            label=label,
        )

    plt.xlabel("LSM Level", fontsize=34)
    plt.ylabel(f"{metric_label} (%)", fontsize=34)
    plt.title(f"{metric_label} by LSM Level", fontsize=40)
    plt.xticks(sorted({lvl for key in keys for lvl in grouped[key].keys()}), fontsize=24)
    plt.yticks(fontsize=24)
    plt.grid(True, alpha=0.25)
    plt.ylim(0.0, 103.0)
    plt.legend(loc="best", fontsize=20, title="DB Size | Filter | Cache %", title_fontsize=22)
    plt.tight_layout()

    output_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_png, dpi=200)
    plt.close()


def _output_with_db_size(output: Path, db_size: str) -> Path:
    if not db_size:
        return output
    suffix = db_size.lower()
    stem = output.stem
    if suffix in stem.lower():
        return output
    return output.with_name(f"{stem}_{suffix}{output.suffix}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot level-wise hit rate from perf_context_level_metadata.csv"
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=None,
        help="Input CSV path (positional).",
    )
    parser.add_argument(
        "--input",
        dest="input_opt",
        default=None,
        help="Input CSV path (overrides positional input_csv if both are provided).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output PNG path",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Output CSV path for plotted series",
    )
    parser.add_argument(
        "--metric",
        choices=METRIC_CHOICES,
        default="metadata_hit_rate",
        help="Which hit-rate column to plot",
    )
    parser.add_argument(
        "--db-size",
        choices=["500GB", "1TB", "2TB", ""],
        default="",
        help="If set, plot only the selected DB size. CSV export always includes all available sizes.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_opt or args.input_csv or DEFAULT_INPUT)
    output_path = _output_with_db_size(Path(args.output), args.db_size)
    output_csv = Path(args.output_csv)

    all_grouped = _read_rows(input_path, args.metric, db_size_filter="")
    if not all_grouped:
        raise ValueError("No valid rows found in CSV.")

    available_sizes = sorted(
        {db_size for db_size, _, _ in all_grouped.keys()}, key=_size_sort_key
    )
    print(f"[INFO] available db_size values: {', '.join(available_sizes)}")

    # Always emit all-size series first, regardless of --db-size
    write_plot_data_csv(all_grouped, output_csv, args.metric)

    plot_grouped = all_grouped
    if args.db_size:
        target = args.db_size.upper()
        if target not in {size.upper() for size in available_sizes}:
            raise ValueError(
                f"No rows for db_size={args.db_size}. Available: {', '.join(available_sizes)}"
            )
        plot_grouped = {
            k: v for k, v in all_grouped.items() if (k[0] or "").upper() == target
        }

    plot_hit_level(plot_grouped, output_path, args.metric)


if __name__ == "__main__":
    main()
