import numpy as np
import matplotlib.pyplot as plt
import sys

csv_path = "throughput.csv" if len(sys.argv) < 2 else sys.argv[1]
data = np.loadtxt(csv_path, delimiter=",", skiprows=1, dtype=str)

# Columns: world, cardinality, throughput_records_per_sec, t_update, t_finalize
cardinalities = sorted(np.unique(data[:, 1].astype(int)))
card_labels = [str(c) for c in cardinalities]
n_cards = len(cardinalities)

bar_width = 0.35
x = np.arange(n_cards)

# Pull timing rows for each world
def get_timing(world):
    mask = data[:, 0] == world
    rows = {int(r[1]): r for r in data[mask]}
    t_upd = np.array([float(rows[c][3]) * 1000 if c in rows else 0 for c in cardinalities])
    t_fin = np.array([float(rows[c][4]) * 1000 if c in rows else 0 for c in cardinalities])
    return t_upd, t_fin

ptl_upd, ptl_fin = get_timing("PerThreadLocal")
glb_upd, _       = get_timing("Global")

fig, ax = plt.subplots(figsize=(12, 6))

ptl_offset = -bar_width / 2
glb_offset =  bar_width / 2

# PerThreadLocal: stacked update + finalize
ax.bar(x + ptl_offset, ptl_upd, width=bar_width,
       label="PerThreadLocal – update",   color="#4C72B0", edgecolor="white")
ax.bar(x + ptl_offset, ptl_fin, width=bar_width,
       label="PerThreadLocal – finalize", color="#76B7CC", edgecolor="white",
       bottom=ptl_upd)

# Global: single bar (finalize is always 0)
ax.bar(x + glb_offset, glb_upd, width=bar_width,
       label="Global – update", color="#DD8452", hatch="//", edgecolor="white")

# Value labels on top of each full bar
for i, (pu, pf) in enumerate(zip(ptl_upd, ptl_fin)):
    total = pu + pf
    if total > 0:
        ax.text(x[i] + ptl_offset, total + 0.3, f"{total:.1f}",
                ha="center", va="bottom", fontsize=7.5, color="#333333")

for i, gu in enumerate(glb_upd):
    if gu > 0:
        ax.text(x[i] + glb_offset, gu + 0.3, f"{gu:.1f}",
                ha="center", va="bottom", fontsize=7.5, color="#333333")

ax.set_xticks(x)
ax.set_xticklabels(card_labels, fontsize=10)
ax.set_xlabel("Cardinality (#groups)", fontsize=12)
ax.set_ylabel("Time (ms)", fontsize=12)
ax.set_title("CDP Groupby: Update vs. Finalize Time by Cardinality", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig("throughput_stacked.png", dpi=300)
print("Saved plot to throughput_stacked.png")

# ── Plot 2: throughput (M rec/s) grouped bar ──────────────────────────────────
def get_throughput(world):
    mask = data[:, 0] == world
    rows = {int(r[1]): r for r in data[mask]}
    return np.array([float(rows[c][2]) / 1e6 if c in rows else 0 for c in cardinalities])

ptl_tp = get_throughput("PerThreadLocal")
glb_tp = get_throughput("Global")

fig2, ax2 = plt.subplots(figsize=(11, 6))

bars_ptl = ax2.bar(x + ptl_offset, ptl_tp, width=bar_width,
                   label="PerThreadLocal", color="#4C72B0", edgecolor="white")
bars_glb = ax2.bar(x + glb_offset, glb_tp, width=bar_width,
                   label="Global", color="#DD8452", hatch="//", edgecolor="white")

for bar, val in zip(bars_ptl, ptl_tp):
    if val > 0:
        ax2.text(bar.get_x() + bar.get_width() / 2, val + 8,
                 f"{val:.0f}", ha="center", va="bottom", fontsize=7.5, color="#333333")

for bar, val in zip(bars_glb, glb_tp):
    if val > 0:
        ax2.text(bar.get_x() + bar.get_width() / 2, val + 8,
                 f"{val:.0f}", ha="center", va="bottom", fontsize=7.5, color="#333333")

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
