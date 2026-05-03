import sys

import matplotlib.pyplot as plt
import numpy as np

csv_path = "throughput.csv" if len(sys.argv) < 2 else sys.argv[1]
data = np.loadtxt(csv_path, delimiter=",", skiprows=1, dtype=str)

cardinalities = sorted(np.unique(data[:, 1].astype(int)))
card_labels = [str(c) for c in cardinalities]
n_cards = len(cardinalities)
x = np.arange(n_cards)

WORLD_STYLES = {
    "PerThreadLocal": {
        "update_color": "#4C72B0",
        "finalize_color": "#76B7CC",
        "hatch": None,
    },
    "Global": {
        "update_color": "#DD8452",
        "finalize_color": None,
        "hatch": "//",
    },
    "GlobalBuffered": {
        "update_color": "#55A868",
        "finalize_color": None,
        "hatch": "\\\\",
    },
}

worlds = [world for world in WORLD_STYLES if np.any(data[:, 0] == world)]
bar_width = 0.8 / max(len(worlds), 1)
offsets = np.linspace(-(len(worlds) - 1) / 2, (len(worlds) - 1) / 2, len(worlds)) * bar_width


def get_timing(world):
    mask = data[:, 0] == world
    rows = {int(r[1]): r for r in data[mask]}
    t_upd = np.array([float(rows[c][3]) * 1000 if c in rows else 0 for c in cardinalities])
    t_fin = np.array([float(rows[c][4]) * 1000 if c in rows else 0 for c in cardinalities])
    return t_upd, t_fin


fig, ax = plt.subplots(figsize=(13, 6))

for offset, world in zip(offsets, worlds):
    style = WORLD_STYLES[world]
    t_upd, t_fin = get_timing(world)

    ax.bar(
        x + offset,
        t_upd,
        width=bar_width,
        label=f"{world} – update",
        color=style["update_color"],
        hatch=style["hatch"],
        edgecolor="white",
    )

    if np.any(t_fin > 0):
        ax.bar(
            x + offset,
            t_fin,
            width=bar_width,
            label=f"{world} – finalize",
            color=style["finalize_color"],
            edgecolor="white",
            bottom=t_upd,
        )

    totals = t_upd + t_fin
    for i, total in enumerate(totals):
        if total > 0:
            ax.text(
                x[i] + offset,
                total + 0.3,
                f"{total:.1f}",
                ha="center",
                va="bottom",
                fontsize=7.5,
                color="#333333",
            )

ax.set_xticks(x)
ax.set_xticklabels(card_labels, fontsize=10)
ax.set_xlabel("Cardinality (#groups)", fontsize=12)
ax.set_ylabel("Time (ms)", fontsize=12)
ax.set_title(
    "CDP Groupby: Update vs. Finalize Time by Cardinality",
    fontsize=14,
    fontweight="bold",
)
ax.legend(fontsize=10)
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig("throughput_stacked.png", dpi=300)
print("Saved plot to throughput_stacked.png")


def get_throughput(world):
    mask = data[:, 0] == world
    rows = {int(r[1]): r for r in data[mask]}
    return np.array([float(rows[c][2]) / 1e6 if c in rows else 0 for c in cardinalities])


fig2, ax2 = plt.subplots(figsize=(12, 6))

for offset, world in zip(offsets, worlds):
    style = WORLD_STYLES[world]
    throughput = get_throughput(world)
    bars = ax2.bar(
        x + offset,
        throughput,
        width=bar_width,
        label=world,
        color=style["update_color"],
        hatch=style["hatch"],
        edgecolor="white",
    )

    for bar, val in zip(bars, throughput):
        if val > 0:
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                val + 8,
                f"{val:.0f}",
                ha="center",
                va="bottom",
                fontsize=7.5,
                color="#333333",
            )

ax2.set_xticks(x)
ax2.set_xticklabels(card_labels, fontsize=10)
ax2.set_xlabel("Cardinality (#groups)", fontsize=12)
ax2.set_ylabel("Throughput (M records/sec)", fontsize=12)
ax2.set_title("CDP Groupby: Throughput vs. Cardinality", fontsize=14, fontweight="bold")
ax2.legend(fontsize=11)
ax2.grid(axis="y", linestyle="--", alpha=0.4)
ax2.set_axisbelow(True)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig("throughput.png", dpi=300)
print("Saved plot to throughput.png")
