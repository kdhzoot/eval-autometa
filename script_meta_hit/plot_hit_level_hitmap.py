#!/usr/bin/env python3

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors, font_manager
from matplotlib.gridspec import GridSpec

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (SCRIPT_DIR / "log_traces" / "perf_hit_ratio.csv").resolve()
DEFAULT_OUTPUT = (SCRIPT_DIR / "perf_hit_ratio_hitmap.png").resolve()

METRIC_CHOICES = [
    "metadata_hit_rate",
    "block_hit_rate",
    "index_hit_rate",
    "filter_hit_rate",
    "data_hit_rate",
]

METRIC_LABELS = {
    "metadata_hit_rate": "Metadata Hit Ratio",
    "block_hit_rate": "Block Cache Hit Ratio",
    "index_hit_rate": "Index Cache Hit Ratio",
    "filter_hit_rate": "Filter Cache Hit Ratio",
    "data_hit_rate": "Data Block Cache Hit Ratio",
}

DB_SIZE_ORDER = ["500GB", "1TB", "2TB"]
LEVEL_ORDER = [1, 2, 3, 4, 5]
CACHE_ORDER = [5.0, 2.0, 1.0, 0.1, 0.05]

FIGURE_WIDTH = 14.6
FIGURE_HEIGHT = 4.6
HEATMAP_VMIN = 0.0
HEATMAP_VMAX = 100.0
HEATMAP_NAN_COLOR = "#f4f1eb"
SPINE_COLOR = "#4a4a4a"
TEXT_DARK = "#1f1f1f"
TEXT_LIGHT = "#ffffff"
SAVE_DPI = 320
COLORMAP_MAX = 0.88

PANEL_TITLE_FONT = 34
AXIS_LABEL_FONT = 36
TICK_FONT = 26
CBAR_LABEL_FONT = 36
CBAR_TICK_FONT = 24
CELL_FONT_MIN = 22
CELL_FONT_MAX = 30


def _pick_font_family() -> str:
    available = {font.name for font in font_manager.fontManager.ttflist}
    for name in ("Times New Roman", "Nimbus Roman", "Liberation Serif", "DejaVu Serif"):
        if name in available:
            return name
    return "serif"


plt.rcParams.update(
    {
        "font.family": _pick_font_family(),
        "axes.edgecolor": SPINE_COLOR,
        "axes.linewidth": 0.8,
        "xtick.color": TEXT_DARK,
        "ytick.color": TEXT_DARK,
    }
)


def _canonicalize_db_size(size: str) -> str:
    if not size:
        return size
    s = size.strip().upper()
    try:
        if s.endswith("GB"):
            gb = float(s[:-2])
            if gb == int(gb):
                gb_int = int(gb)
                if gb_int == 1000:
                    return "1TB"
                if gb_int == 2000:
                    return "2TB"
                return f"{gb_int}GB"
            return f"{gb}GB"
        if s.endswith("TB"):
            tb = float(s[:-2])
            if tb == int(tb):
                return f"{int(tb)}TB"
            return f"{tb}TB"
    except ValueError:
        return s
    return s


def _read_heatmap_values(
    path: Path,
    metric: str,
    filter_kind: str,
) -> Dict[Tuple[str, float, int], float]:
    grouped: Dict[Tuple[str, float, int], List[float]] = defaultdict(list)

    with path.open("r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = set(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError("CSV has no header.")

        is_ratio_csv = {"db_size", "cache_pct", "level", "metric_value_ratio"}.issubset(
            fieldnames
        )
        if is_ratio_csv:
            for row in reader:
                try:
                    db_size = _canonicalize_db_size(row["db_size"])
                    cache_pct = float(row["cache_pct"])
                    level = int(row["level"])
                    value = float(row["metric_value_ratio"])
                except (KeyError, ValueError):
                    continue
                row_filter = (row.get("filter_kind") or "").strip()
                if filter_kind and row_filter and row_filter != filter_kind:
                    continue
                if db_size not in DB_SIZE_ORDER or level not in LEVEL_ORDER:
                    continue
                grouped[(db_size, cache_pct, level)].append(value)
        else:
            required = {"db_size", "filter_kind", "cache_pct", "level", metric}
            missing = required - fieldnames
            if missing:
                raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")
            for row in reader:
                try:
                    db_size = _canonicalize_db_size(row["db_size"])
                    row_filter = row["filter_kind"].strip()
                    cache_pct = float(row["cache_pct"])
                    level = int(row["level"])
                    value = float(row[metric])
                except (KeyError, ValueError):
                    continue
                if filter_kind and row_filter != filter_kind:
                    continue
                if db_size not in DB_SIZE_ORDER or level not in LEVEL_ORDER or level == 0:
                    continue
                grouped[(db_size, cache_pct, level)].append(value)

    return {key: sum(values) / len(values) for key, values in grouped.items() if values}


def _build_matrix(values: Dict[Tuple[str, float, int], float]) -> np.ndarray:
    matrix = np.full((len(CACHE_ORDER), len(DB_SIZE_ORDER) * len(LEVEL_ORDER)), np.nan)
    for db_idx, db_size in enumerate(DB_SIZE_ORDER):
        for level_idx, level in enumerate(LEVEL_ORDER):
            col_idx = db_idx * len(LEVEL_ORDER) + level_idx
            for row_idx, cache_pct in enumerate(CACHE_ORDER):
                value = values.get((db_size, cache_pct, level))
                if value is not None:
                    matrix[row_idx, col_idx] = value * 100.0
    return matrix


def _format_heat_value(value: float) -> str:
    rounded = round(value)
    if abs(value - rounded) < 0.05:
        return str(int(rounded))
    return f"{value:.1f}"


def _annotation_color(value: float, cmap, norm) -> str:
    r, g, b, _ = cmap(norm(value))
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return TEXT_DARK if luminance > 0.62 else TEXT_LIGHT


def _draw_heatmap(
    fig: plt.Figure,
    ax: plt.Axes,
    matrix: np.ndarray,
    cmap,
    norm,
    font_scale: float,
) -> None:
    ax.imshow(matrix, cmap=cmap, norm=norm, aspect="auto", interpolation="nearest")

    x_labels = [f"L{level}" for _ in DB_SIZE_ORDER for level in LEVEL_ORDER]
    ax.set_xticks(range(matrix.shape[1]))
    ax.set_xticklabels(x_labels, fontsize=round(TICK_FONT * font_scale))

    ax.set_yticks(range(len(CACHE_ORDER)))
    ax.set_yticklabels([f"{pct:g}" for pct in CACHE_ORDER], fontsize=round(TICK_FONT * font_scale))
    ax.set_ylabel("Memory budget (%)", fontsize=round(AXIS_LABEL_FONT * font_scale), labelpad=16)

    ax.tick_params(axis="both", which="major", length=0)

    for spine in ax.spines.values():
        spine.set_visible(False)

    for divider_idx in range(1, len(DB_SIZE_ORDER)):
        ax.axvline(
            divider_idx * len(LEVEL_ORDER) - 0.5,
            color="#ffffff",
            linewidth=2.4,
        )

    for db_idx, db_size in enumerate(DB_SIZE_ORDER):
        center = db_idx * len(LEVEL_ORDER) + (len(LEVEL_ORDER) - 1) / 2.0
        ax.text(
            center,
            -0.18,
            db_size,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=round(AXIS_LABEL_FONT * font_scale),
            fontweight="semibold",
        )

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    bbox = ax.get_window_extent(renderer=renderer)
    cell_w_pt = (bbox.width / matrix.shape[1]) * 72.0 / fig.dpi
    cell_h_pt = (bbox.height / matrix.shape[0]) * 72.0 / fig.dpi
    cell_font = max(
        round(CELL_FONT_MIN * font_scale),
        min(
            round(CELL_FONT_MAX * font_scale),
            int(min(cell_w_pt * 0.44, cell_h_pt * 0.50)),
        ),
    )

    for row_idx in range(matrix.shape[0]):
        for col_idx in range(matrix.shape[1]):
            value = matrix[row_idx, col_idx]
            if math.isnan(value):
                continue
            ax.text(
                col_idx,
                row_idx,
                _format_heat_value(value),
                ha="center",
                va="center",
                fontsize=cell_font,
                fontweight="semibold",
                color=_annotation_color(value, cmap, norm),
            )


def plot_hit_level_hitmap(
    input_csv: Path,
    output_png: Path,
    metric: str,
    font_scale: float,
    filter_kind: str,
) -> None:
    values = _read_heatmap_values(input_csv, metric, filter_kind)
    if not values:
        raise ValueError("No valid rows found for heatmap.")

    matrix = _build_matrix(values)
    if np.isnan(matrix).all():
        raise ValueError("No valid rows found for heatmap.")

    base_cmap = plt.get_cmap("coolwarm")
    cmap = colors.ListedColormap(base_cmap(np.linspace(0.0, COLORMAP_MAX, 256)))
    cmap.set_bad(HEATMAP_NAN_COLOR)
    norm = colors.Normalize(vmin=HEATMAP_VMIN, vmax=HEATMAP_VMAX)

    fig = plt.figure(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    gs = GridSpec(
        nrows=1,
        ncols=2,
        figure=fig,
        width_ratios=[1, 0.028],
        left=0.065,
        right=0.91,
        bottom=0.22,
        top=0.96,
        wspace=0.015,
    )

    ax = fig.add_subplot(gs[0, 0])
    cax = fig.add_subplot(gs[0, 1])

    _draw_heatmap(
        fig=fig,
        ax=ax,
        matrix=matrix,
        cmap=cmap,
        norm=norm,
        font_scale=font_scale,
    )

    metric_label = METRIC_LABELS.get(metric, metric)
    cbar = fig.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap=cmap),
        cax=cax,
        ticks=[0, 20, 40, 60, 80, 100],
    )
    cbar.set_label(
        "Metadata Hit (%)" if metric == "metadata_hit_rate" else f"{metric_label} (%)",
        fontsize=round(CBAR_LABEL_FONT * font_scale),
        labelpad=12,
    )
    cbar.ax.tick_params(labelsize=round(CBAR_TICK_FONT * font_scale), length=4)
    cbar.ax.set_aspect("auto")

    output_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_png, dpi=SAVE_DPI, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot hit-ratio heatmap grouped by DB size and metadata level."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=None,
        help="Input CSV path (positional). Supports perf_hit_ratio.csv or raw metadata CSV.",
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
        help="Output PNG path.",
    )
    parser.add_argument(
        "--metric",
        choices=METRIC_CHOICES,
        default="metadata_hit_rate",
        help="Metric to plot.",
    )
    parser.add_argument(
        "--filter-kind",
        default="",
        help="Optional filter_kind filter.",
    )
    parser.add_argument(
        "--font-scale",
        type=float,
        default=1.0,
        help="Global font scale multiplier.",
    )
    args = parser.parse_args()

    input_csv = Path(args.input_opt or args.input_csv or str(DEFAULT_INPUT))
    output_png = Path(args.output)
    plot_hit_level_hitmap(
        input_csv=input_csv,
        output_png=output_png,
        metric=args.metric,
        font_scale=args.font_scale,
        filter_kind=args.filter_kind,
    )


if __name__ == "__main__":
    main()
