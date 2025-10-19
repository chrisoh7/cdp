import numpy as np
import matplotlib.pyplot as plt

# Load CSV (treat first column as string)
data = np.loadtxt("throughput.csv", delimiter=",", skiprows=1, dtype=str)

worlds = np.unique(data[:, 0])

plt.figure(figsize=(7, 5))

for world in worlds:
    mask = data[:, 0] == world
    cardinality = data[mask, 1].astype(float)
    throughput = data[mask, 2].astype(float) / 1e6  # convert to M recs/sec
    plt.plot(
        cardinality,
        throughput,
        "o-",
        linewidth=2,
        markersize=5,
        label=world,
    )

plt.xscale("log")
plt.xlabel("Cardinality (#groups)")
plt.ylabel("Throughput (million records/sec)")
plt.title("Throughput vs. Cardinality (All Worlds)")
plt.grid(True, which="both", linestyle="--", alpha=0.5)
plt.legend()
plt.tight_layout()

# Show or save
# plt.show()
plt.savefig("throughput.png", dpi=200)
