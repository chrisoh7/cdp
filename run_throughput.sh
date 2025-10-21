#!/bin/bash
set -e

# ------------------------------
# Default configuration
# ------------------------------
BIN_NAME="cardinality_throughput"
OUT_CSV="throughput.csv"
PLOT_SCRIPT="plot_throughput.py"
WORLD_TYPE="all"
MAX_POW=5
NUM_RECORDS=1000000

# ------------------------------
# Parse arguments (both positional and keyword)
# ------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --world)
      WORLD_TYPE="$2"
      shift 2
      ;;
    --max-pow)
      MAX_POW="$2"
      shift 2
      ;;
    --records)
      NUM_RECORDS="$2"
      shift 2
      ;;
    -w)
      WORLD_TYPE="$2"
      shift 2
      ;;
    -p)
      MAX_POW="$2"
      shift 2
      ;;
    -n)
      NUM_RECORDS="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--world <type>] [--max-pow <n>] [--records <count>]"
      exit 1
      ;;
  esac
done

# ------------------------------
# Step 1: Build Rust binary
# ------------------------------
echo "Building Rust benchmark..."
cargo build --release --bin "$BIN_NAME"

# ------------------------------
# Step 2: Run benchmark and collect CSV
# ------------------------------
echo "Running throughput benchmark..."
cargo run --release --bin "$BIN_NAME" -- \
  --world "$WORLD_TYPE" \
  --max-pow "$MAX_POW" \
  --num-records "$NUM_RECORDS" > "$OUT_CSV"

echo "Results written to $OUT_CSV"

# ------------------------------
# Step 3: Run Python plotter
# ------------------------------
if command -v python3 &> /dev/null; then
  echo "Generating plot with $PLOT_SCRIPT..."
  python3 "$PLOT_SCRIPT"
  echo "Plot generated successfully."
else
  echo "⚠️ Python3 not found. Skipping plot generation."
fi
