#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Recursively scan a result root for o.ld.rep.run files and plot "
            "interval throughput over time."
        )
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default="results_write",
        help="Result root to scan (default: results_write)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="interval_throughput_plot.png",
        help="Output image path (default: interval_throughput_plot.png)",
    )
    parser.add_argument(
        "--title",
        default="Interval Throughput Over Time",
        help="Plot title",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=160,
        help="Output image DPI (default: 160)",
    )
    parser.add_argument(
        "--max-label-depth",
        type=int,
        default=6,
        help=(
            "How many trailing path components to keep in the legend label "
            "(default: 6)"
        ),
    )
    return parser.parse_args()


def make_label(file_path: Path, root: Path, max_label_depth: int) -> str:
    rel_parts = file_path.relative_to(root).parts[:-1]
    if max_label_depth > 0 and len(rel_parts) > max_label_depth:
        rel_parts = rel_parts[-max_label_depth:]
    return "/".join(rel_parts)


def load_series(file_path: Path):
    with file_path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != ["secs_elapsed", "interval_qps"]:
            raise ValueError(f"unexpected header {header!r}")

        xs = []
        ys = []
        for line_no, row in enumerate(reader, start=2):
            if len(row) != 2:
                raise ValueError(f"invalid row at line {line_no}: {row!r}")
            xs.append(int(row[0]))
            ys.append(float(row[1]))

    if not xs:
        raise ValueError("no interval rows found")
    return xs, ys


def main():
    args = parse_args()

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import ScalarFormatter
    except ImportError as exc:
        print(
            "Error: matplotlib is required to generate plots. "
            "Install it with `python3 -m pip install matplotlib`.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    root = Path(args.input_dir).resolve()
    output_path = Path(args.output).resolve()

    if not root.is_dir():
        print(f"Error: result root not found: {root}", file=sys.stderr)
        return 1

    series = []
    skipped = []

    for file_path in sorted(root.glob("**/o.ld.rep.run")):
        try:
            xs, ys = load_series(file_path)
            label = make_label(file_path, root, args.max_label_depth)
            series.append((label, xs, ys))
        except Exception as exc:  # noqa: BLE001
            skipped.append((file_path, str(exc)))

    if not series:
        print(f"Error: no valid o.ld.rep.run files found under {root}", file=sys.stderr)
        return 1

    fig_width = max(12, min(24, 12 + len(series) * 0.18))
    fig_height = max(7, min(18, 7 + len(series) * 0.08))
    plt.rcParams.update(
        {
            "font.size": 18,
            "axes.titlesize": 22,
            "axes.labelsize": 20,
            "xtick.labelsize": 18,
            "ytick.labelsize": 18,
            "legend.fontsize": 12,
        }
    )
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    for label, xs, ys in series:
        ax.plot(xs, ys, linewidth=1.5, alpha=0.95, label=label)

    ax.set_title(args.title)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)

    max_x = max(max(xs) for _, xs, _ in series)
    ax.set_xlim(0, max_x)
    x_formatter = ScalarFormatter(useOffset=False)
    x_formatter.set_scientific(False)
    ax.xaxis.set_major_formatter(x_formatter)
    y_formatter = ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    ax.yaxis.set_major_formatter(y_formatter)
    ax.xaxis.get_offset_text().set_visible(False)
    ax.yaxis.get_offset_text().set_visible(False)

    if len(series) <= 12:
        ax.legend(loc="best")
    else:
        ax.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        fig.subplots_adjust(right=0.72)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=args.dpi, bbox_inches="tight")

    print(f"Wrote plot: {output_path}")
    print(f"Plotted series: {len(series)}")
    if skipped:
        print(f"Skipped invalid files: {len(skipped)}", file=sys.stderr)
        for file_path, reason in skipped[:10]:
            print(f"  {file_path}: {reason}", file=sys.stderr)
        if len(skipped) > 10:
            print(f"  ... and {len(skipped) - 10} more", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
