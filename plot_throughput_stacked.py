
import numpy as np
import matplotlib.pyplot as plt
import sys

# ------------------------------
# Load CSV
# ------------------------------
csv_path = "throughput.csv" if len(sys.argv) < 2 else sys.argv[1]
data = np.loadtxt(csv_path, delimiter=",", skiprows=1, dtype=str)

# Columns:
# 0: world
# 1: cardinality
# 2: throughput_records_per_sec
# 3: t_update (or t_medium)
# 4: t_finalize

worlds = np.unique(data[:, 0])
n = len(data)

# ------------------------------
# Preprocess data
# ------------------------------
# Combine world and cardinality into labels
labels = np.array([f"{w}-{c}" for w, c in zip(data[:, 0], data[:, 1])])
x = np.arange(n)

# Use whichever timing column exists (t_update or t_medium)
try:
    t_update = data[:, 3].astype(float)
except Exception:
    t_update = np.zeros(n)

t_finalize = data[:, 4].astype(float)
total_time = t_update + t_finalize

# ------------------------------
# Plot stacked bars
# ------------------------------
plt.figure(figsize=(10, 6))

plt.bar(x, t_update, color="#4C72B0", label="update")
plt.bar(x, t_finalize, bottom=t_update, color="#55A868", label="finalize")

plt.ylabel("Time (seconds)")
plt.xlabel("Method and Cardinality")
plt.title("CDP Groupby Timing Breakdown")
plt.xticks(x, labels, rotation=45, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.legend()

plt.tight_layout()
plt.savefig("throughput_stacked.png", dpi=300)
print("Saved plot to throughput_stacked.png")
