#!/usr/bin/env python3

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
from matplotlib import font_manager
from matplotlib.transforms import Bbox

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
    "metadata_hit_rate": "Metadata Cache Hit Rate",
    "block_hit_rate": "Block Cache Hit Rate",
    "index_hit_rate": "Index Cache Hit Rate",
    "filter_hit_rate": "Filter Cache Hit Rate",
    "data_hit_rate": "Data Block Cache Hit Rate",
}

DB_SIZE_ORDER = ["500GB", "1TB", "2TB"]
LEVEL_ORDER = [1, 2, 3, 4, 5]
CACHE_ORDER = [5.0, 2.0, 1.0, 0.1, 0.05]

# ===== User-tunable style/layout =====
FIGURE_SIZE = (31, 12.5)
BASE_SCALE = 1.15

COLORMAP_NAME = "coolwarm"
COLORMAP_MAX = 0.88  # 1.0이면 최상단 색을 원본 그대로 사용
HEATMAP_VMIN = 0.0
HEATMAP_VMAX = 100.0
HEATMAP_NAN_COLOR = "#f0f0f0"

Y_AXIS_LABEL = "Memory budget (%)"
Y_AXIS_LABEL_PAD = 8

X_TICK_FONT_BASE = 50
X_GROUP_TICK_FONT_BASE = 70
Y_TICK_FONT_BASE = 60
AXIS_TITLE_FONT_BASE = 70
CBAR_TICK_FONT_BASE = 60
CELL_FONT_DESIRED_BASE = 50

X_TICK_PAD = 10
GROUP_AXIS_OUTWARD = 80
GROUP_AXIS_TICK_PAD = 6

CBAR_FRACTION = 0.03
CBAR_PAD = 0.01
CBAR_LABEL_PAD = 12
CBAR_TICKS = [0, 20, 40, 60, 80, 100]

SUBPLOT_LEFT = 0.075
SUBPLOT_RIGHT = 0.97
SUBPLOT_BOTTOM = 0.16
SUBPLOT_TOP = 0.985

SAVE_DPI = 200
SAVE_PAD_LEFT_INCHES = 0.5
SAVE_PAD_RIGHT_INCHES = 0.06
SAVE_PAD_BOTTOM_INCHES = 0.06
SAVE_PAD_TOP_INCHES = 0.5

HEAT_VALUE_INT_THRESHOLD = 0.05
CELL_FONT_MIN = 10
CELL_FIT_WIDTH_RATIO = 0.82
CELL_FIT_HEIGHT_RATIO = 0.58


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
        "axes.titlesize": 34,
        "axes.labelsize": 30,
        "xtick.labelsize": 24,
        "ytick.labelsize": 24,
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

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError("CSV has no header.")

        is_ratio_csv = {"db_size", "cache_pct", "level", "metric_value_ratio"}.issubset(
            fieldnames
        )
        if is_ratio_csv:
            required = {"db_size", "cache_pct", "level", "metric_value_ratio"}
            missing = required - fieldnames
            if missing:
                raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")
            for row in reader:
                try:
                    db_size = _canonicalize_db_size(row["db_size"])
                    cache_pct = float(row["cache_pct"])
                    level = int(row["level"])
                    value = float(row["metric_value_ratio"])
                except (ValueError, KeyError):
                    continue

                row_filter_kind = (row.get("filter_kind") or "").strip()
                if filter_kind and row_filter_kind and row_filter_kind != filter_kind:
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
                    row_filter_kind = row["filter_kind"].strip()
                    cache_pct = float(row["cache_pct"])
                    level = int(row["level"])
                    value = float(row[metric])
                except (ValueError, KeyError):
                    continue

                if filter_kind and row_filter_kind != filter_kind:
                    continue
                if level == 0:
                    continue
                if db_size not in DB_SIZE_ORDER or level not in LEVEL_ORDER:
                    continue
                grouped[(db_size, cache_pct, level)].append(value)

    return {k: sum(v) / len(v) for k, v in grouped.items() if v}


def _format_heat_value(v: float) -> str:
    rounded = round(v)
    if abs(v - rounded) < HEAT_VALUE_INT_THRESHOLD:
        return str(int(rounded))
    return f"{v:.1f}"


def _fit_cell_fontsize(
    fig: plt.Figure,
    ax: plt.Axes,
    n_rows: int,
    n_cols: int,
    desired_size: int,
) -> int:
    # Fit annotation text to cell geometry to avoid overflow.
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    bbox = ax.get_window_extent(renderer=renderer)
    cell_w_pt = (bbox.width / n_cols) * 72.0 / fig.dpi
    cell_h_pt = (bbox.height / n_rows) * 72.0 / fig.dpi
    max_size = int(min(cell_w_pt * CELL_FIT_WIDTH_RATIO, cell_h_pt * CELL_FIT_HEIGHT_RATIO))
    return max(CELL_FONT_MIN, min(desired_size, max_size))


def _save_figure_with_padding(fig: plt.Figure, output_png: Path) -> None:
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    tight_bbox = fig.get_tightbbox(renderer)
    padded_bbox = Bbox.from_extents(
        tight_bbox.x0 - SAVE_PAD_LEFT_INCHES,
        tight_bbox.y0 - SAVE_PAD_BOTTOM_INCHES,
        tight_bbox.x1 + SAVE_PAD_RIGHT_INCHES,
        tight_bbox.y1 + SAVE_PAD_TOP_INCHES,
    )
    fig.savefig(output_png, dpi=SAVE_DPI, bbox_inches=padded_bbox)


def plot_hit_level_hitmap(
    values: Dict[Tuple[str, float, int], float],
    output_png: Path,
    metric: str,
    filter_kind: str,
    font_scale: float = 1.0,
) -> None:
    n_rows = len(CACHE_ORDER)
    n_cols = len(DB_SIZE_ORDER) * len(LEVEL_ORDER)

    matrix = np.full((n_rows, n_cols), np.nan)
    x_labels: List[str] = []

    for db_idx, db_size in enumerate(DB_SIZE_ORDER):
        for level_idx, level in enumerate(LEVEL_ORDER):
            col_idx = db_idx * len(LEVEL_ORDER) + level_idx
            x_labels.append(f"L{level}")
            for row_idx, cache_pct in enumerate(CACHE_ORDER):
                value = values.get((db_size, cache_pct, level))
                if value is not None:
                    matrix[row_idx, col_idx] = value * 100.0

    if np.isnan(matrix).all():
        raise ValueError("No valid rows found for heatmap.")

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    effective_scale = BASE_SCALE * font_scale
    base_cmap = plt.get_cmap(COLORMAP_NAME)
    cmap = ListedColormap(base_cmap(np.linspace(0.0, COLORMAP_MAX, 256)))
    cmap.set_bad(color=HEATMAP_NAN_COLOR)
    image = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=HEATMAP_VMIN, vmax=HEATMAP_VMAX)
    x_tick_size = round(X_TICK_FONT_BASE * effective_scale)
    x_group_tick_size = round(X_GROUP_TICK_FONT_BASE * effective_scale)
    axis_title_size = round(AXIS_TITLE_FONT_BASE * effective_scale)

    plt.xticks(range(n_cols), x_labels, fontsize=x_tick_size)
    plt.yticks(
        range(n_rows),
        [f"{pct:g}" for pct in CACHE_ORDER],
        fontsize=round(Y_TICK_FONT_BASE * effective_scale),
    )

    plt.ylabel(
        Y_AXIS_LABEL,
        fontsize=axis_title_size,
        labelpad=Y_AXIS_LABEL_PAD,
    )
    ax.tick_params(axis="x", which="both", pad=X_TICK_PAD)
    group_ax = ax.secondary_xaxis("bottom")
    group_ax.spines["bottom"].set_position(("outward", GROUP_AXIS_OUTWARD))
    group_ax.spines["bottom"].set_visible(False)
    group_centers = []
    for db_idx, db_size in enumerate(DB_SIZE_ORDER):
        start = db_idx * len(LEVEL_ORDER)
        end = start + len(LEVEL_ORDER) - 1
        group_centers.append((start + end) / 2.0)
    group_ax.set_xticks(group_centers)
    group_ax.set_xticklabels(DB_SIZE_ORDER, fontsize=x_group_tick_size)
    group_ax.tick_params(axis="x", which="both", length=0, pad=GROUP_AXIS_TICK_PAD)

    metric_label = METRIC_LABELS.get(metric, metric)

    cbar_label = (
        "Metadata hit ratio (%)"
        if metric == "metadata_hit_rate"
        else f"{metric_label} (%)"
    )
    cbar = plt.colorbar(image, fraction=CBAR_FRACTION, pad=CBAR_PAD)
    cbar.set_label(cbar_label, fontsize=axis_title_size, labelpad=CBAR_LABEL_PAD)
    cbar.set_ticks(CBAR_TICKS)
    cbar.set_ticklabels([f"{t}" for t in CBAR_TICKS])
    cbar.ax.tick_params(labelsize=round(CBAR_TICK_FONT_BASE * effective_scale))

    plt.subplots_adjust(
        left=SUBPLOT_LEFT,
        right=SUBPLOT_RIGHT,
        bottom=SUBPLOT_BOTTOM,
        top=SUBPLOT_TOP,
    )

    cell_font = _fit_cell_fontsize(
        fig,
        ax,
        n_rows=n_rows,
        n_cols=n_cols,
        desired_size=round(CELL_FONT_DESIRED_BASE * effective_scale),
    )
    for row_idx in range(n_rows):
        for col_idx in range(n_cols):
            v = matrix[row_idx, col_idx]
            if math.isnan(v):
                continue
            text_color = "white" if v >= 60.0 else "black"
            ax.text(
                col_idx,
                row_idx,
                _format_heat_value(v),
                ha="center",
                va="center",
                fontsize=cell_font,
                color=text_color,
                clip_on=True,
            )

    output_png.parent.mkdir(parents=True, exist_ok=True)
    _save_figure_with_padding(fig, output_png)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot hit-ratio heatmap for DB sizes 500GB/1TB/2TB and levels 1-5"
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
        help="Output PNG path",
    )
    parser.add_argument(
        "--metric",
        choices=METRIC_CHOICES,
        default="metadata_hit_rate",
        help="Metric column used when input is raw metadata CSV",
    )
    parser.add_argument(
        "--filter-kind",
        default="full",
        help="Filter kind to include (default: full). If empty, include all filter kinds.",
    )
    parser.add_argument(
        "--font-scale",
        type=float,
        default=1.0,
        help="Global font scale (default: 1.0)",
    )
    args = parser.parse_args()

    input_path = Path(args.input_opt or args.input_csv or DEFAULT_INPUT)
    output_path = Path(args.output)
    filter_kind = args.filter_kind.strip()

    values = _read_heatmap_values(input_path, args.metric, filter_kind)
    plot_hit_level_hitmap(values, output_path, args.metric, filter_kind, args.font_scale)


if __name__ == "__main__":
    main()
