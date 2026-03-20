import argparse
import math
import os
import statistics
import time
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import List, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np


@dataclass(frozen=True)
class ArrayStats:
    total_sum: int
    mean: float
    variance: float
    minimum: int
    maximum: int


def _compute_stats(data: Sequence[int]) -> tuple[int, int, int, int]:
    if not data: return 0, 0, math.inf, -math.inf

    total_sum = total_sum_sq = 0
    minimum = maximum = data[0]

    for x in data:
        total_sum += x
        total_sum_sq += x*x
        minimum = min(minimum, x)
        maximum = max(maximum, x)

    return total_sum, total_sum_sq, minimum, maximum

def compute_stats_sequential(data: Sequence[int]) -> ArrayStats:
    total_sum, total_sum_sq, minimum, maximum = _compute_stats(data)
    n = len(data)
    if n == 0: raise ValueError("Array must not be empty")
    mean = total_sum / n
    variance = total_sum_sq / n - mean**2
    return ArrayStats(total_sum, mean, variance, minimum, maximum)


def _chunk_stats(chunk_payload: Tuple[int, Sequence[int]]) -> Tuple[int, int, int, int, int]:
    chunk_index, chunk = chunk_payload
    total_sum, total_sum_sq, minimum, maximum = _compute_stats(chunk)
    return chunk_index, total_sum, total_sum_sq, minimum, maximum


def _build_chunks(data: Sequence[int], workers: int) -> List[Tuple[int, Sequence[int]]]:
    n = len(data)
    workers = max(1, workers)
    chunk_size = max(1, math.ceil(n / workers))
    return [(idx, data[start : start + chunk_size]) for idx, start in enumerate(range(0, n, chunk_size))]


def compute_stats_parallel(data: Sequence[int], parallelism: int) -> ArrayStats:
    if not data: raise ValueError("Input array must not be empty")
    if parallelism < 1: raise ValueError("parallelism must be >= 1")

    chunks = _build_chunks(data, parallelism)
    processes = min(parallelism, len(chunks))

    with Pool(processes=processes) as pool: partial_results = pool.map(_chunk_stats, chunks)

    total_sum = total_sum_sq = 0
    minimum = math.inf
    maximum = -math.inf

    for _, part_sum, part_sum_sq, part_min, part_max in partial_results:
        total_sum += part_sum
        total_sum_sq += part_sum_sq
        if part_min < minimum: minimum = part_min
        if part_max > maximum: maximum = part_max

    n = len(data)
    mean = total_sum / n
    variance = (total_sum_sq / n) - (mean * mean)
    return ArrayStats(int(total_sum), mean, variance, int(minimum), int(maximum))


def arrays_equal(l: ArrayStats, r: ArrayStats, eps: float = 1e-12) -> bool:
    return (
        l.total_sum == r.total_sum
        and l.minimum == r.minimum
        and l.maximum == r.maximum
        and abs(l.mean - r.mean) <= eps
        and abs(l.variance - r.variance) <= eps
    )


def timed_run(fn, *args, repeats: int = 3) -> Tuple[ArrayStats, float]:
    elapsed = []
    result = None
    for _ in range(repeats):
        start = time.perf_counter()
        result = fn(*args)
        elapsed.append(time.perf_counter() - start)
    assert result is not None
    return result, statistics.median(elapsed)


def generate_input(size: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.integers(low=-100_000, high=100_000, size=size, dtype=np.int64)


def build_parallelism_levels(cpu_count: int) -> List[int]: return sorted(set(list(range(1, cpu_count + 1)) + [cpu_count * 2, cpu_count * 4]))


def determinism_check(data: Sequence[int], parallelism: int, runs: int) -> bool:
    baseline = compute_stats_parallel(data, parallelism)
    for _ in range(runs - 1):
        current = compute_stats_parallel(data, parallelism)
        if not arrays_equal(baseline, current): return False
    return True


def plot_metrics(rows: List[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    sizes = sorted({row["size"] for row in rows})

    plt.figure(figsize=(11, 6))
    for size in sizes:
        subset = [r for r in rows if r["size"] == size]
        subset.sort(key=lambda x: x["parallelism"])
        x = [r["parallelism"] for r in subset]
        y = [r["parallel_time_sec"] for r in subset]
        plt.plot(x, y, marker="o", label=f"n={size:,}")
    plt.title("Parallel Runtime vs Parallelism")
    plt.xlabel("Parallelism (processes)")
    plt.ylabel("Runtime (sec)")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "runtime_vs_parallelism.png", dpi=140)
    plt.close()

    plt.figure(figsize=(11, 6))
    for size in sizes:
        subset = [r for r in rows if r["size"] == size]
        subset.sort(key=lambda x: x["parallelism"])
        x = [r["parallelism"] for r in subset]
        y = [r["efficiency"] for r in subset]
        plt.plot(x, y, marker="o", label=f"n={size:,}")
    plt.title("Efficiency vs Parallelism")
    plt.xlabel("Parallelism (processes)")
    plt.ylabel("Efficiency = Speedup / Parallelism")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "efficiency_vs_parallelism.png", dpi=140)
    plt.close()


def run_experiments(
    sizes: List[int],
    repeats: int,
    base_seed: int,
    determinism_runs: int,
    output_dir: Path,
) -> None:
    cpu_count = os.cpu_count() or 1
    parallelism_levels = build_parallelism_levels(cpu_count)

    print(f"Detected CPU cores: {cpu_count}")
    print(f"Parallelism levels: {parallelism_levels}")

    rows: List[dict] = []

    for i, size in enumerate(sizes):
        seed = base_seed + i
        data_np = generate_input(size, seed)
        data = data_np.tolist()

        seq_stats, seq_time = timed_run(compute_stats_sequential, data, repeats=repeats)
        print(f"\n[size={size}] sequential time (median, {repeats} runs): {seq_time:.6f} sec")

        for p in parallelism_levels:
            par_stats, par_time = timed_run(compute_stats_parallel, data, p, repeats=repeats)
            if not arrays_equal(seq_stats, par_stats):
                raise RuntimeError(
                    f"Result mismatch for size={size}, parallelism={p}.\n"
                    f"seq={seq_stats}\npar={par_stats}"
                )

            speedup = seq_time / par_time if par_time > 0 else float("inf")
            efficiency = speedup / p

            row = {
                "size": size,
                "seed": seed,
                "parallelism": p,
                "sequential_time_sec": seq_time,
                "parallel_time_sec": par_time,
                "speedup": speedup,
                "efficiency": efficiency,
                "sum": seq_stats.total_sum,
                "mean": seq_stats.mean,
                "variance": seq_stats.variance,
                "min": seq_stats.minimum,
                "max": seq_stats.maximum,
            }
            rows.append(row)
            print(
                f"  p={p:>3}: par={par_time:.6f}s | speedup={speedup:.3f} | efficiency={efficiency:.3f}"
            )

    det_size = sizes[min(1, len(sizes) - 1)]
    det_seed = base_seed + 10_000
    det_data = generate_input(det_size, det_seed).tolist()
    det_parallelism = cpu_count
    print(
        f"\nDeterminism check: runs={determinism_runs}, size={det_size}, parallelism={det_parallelism}"
    )
    is_deterministic = determinism_check(det_data, det_parallelism, determinism_runs)
    print(f"Deterministic: {is_deterministic}")

    plot_metrics(rows, output_dir)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lab #1: sequential vs parallel array statistics with scalability analysis."
    )
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=[100_000, 1_000_000, 100_000_000],
        help="Input array sizes for experiments.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of timed repeats for each measurement (median is used).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base RNG seed for reproducible input generation.",
    )
    parser.add_argument(
        "--det-runs",
        type=int,
        default=120,
        help="Number of repeated runs for determinism check (must be >100).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results"),
        help="Where to save CSV and plots.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_experiments(
        sizes=args.sizes,
        repeats=args.repeats,
        base_seed=args.seed,
        determinism_runs=args.det_runs,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
