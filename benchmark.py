import subprocess
import time
import argparse
import statistics
import sys
import os


def run_benchmark(
    executable_path: str,
    program_json: str,
    config_json: str,
    max_iterations: int,
    num_runs: int,
    output_dir: str,
) -> list[float]:
    """
    Runs the OCaml simulation benchmark multiple times and reports timings.
    """
    print("--- Starting Benchmark ---")
    print(f"  Executable:     {executable_path}")
    print(f"  Program JSON:   {program_json}")
    print(f"  Config JSON:    {config_json}")
    print(f"  Max Iterations: {max_iterations}")
    print(f"  Number of Runs: {num_runs}")
    print(f"  Output Dir:     {output_dir}")
    print("------------------------------\n")

    run_times = []

    for i in range(num_runs):
        # Define a unique output file for this specific run
        output_csv = os.path.join(output_dir, f"history_run_{i+1}.csv")

        # <exe> <compiled_json> <intermediate_output> <scheduler_config.json> <max_iterations>
        command = [
            executable_path,
            program_json,
            output_csv,
            config_json,
            str(max_iterations),
        ]

        print(f"Running iteration {i+1}/{num_runs}...", end=" ", flush=True)

        start_time = time.perf_counter()

        try:
            result = subprocess.run(
                command,
                check=True,  # Exit with an error if the process fails
                capture_output=True,
                text=True,
                encoding="utf-8",
            )

            end_time = time.perf_counter()
            elapsed = end_time - start_time
            run_times.append(elapsed)
            print(f"Success ({elapsed:.4f}s)")

        except subprocess.CalledProcessError as e:
            print(f"FAILED!")
            print(f"Error executing command: {' '.join(command)}", file=sys.stderr)
            print(f"Return Code: {e.returncode}", file=sys.stderr)
            print(f"Stderr:\n{e.stderr}", file=sys.stderr)
            print(f"Stdout:\n{e.stdout}", file=sys.stderr)
            return []
        except FileNotFoundError:
            print(f"FAILED!")
            print(
                f"Error: Executable not found at '{executable_path}'", file=sys.stderr
            )
            print(
                "Please provide the correct path to the compiled OCaml program.",
                file=sys.stderr,
            )
            return []

    return run_times


def print_summary(run_times: list[float]):
    """
    Prints a statistical summary of the benchmark run times.
    """
    if not run_times:
        print("\nBenchmark did not complete successfully. No summary available.")
        return

    count = len(run_times)
    total_time = sum(run_times)

    print("\n--- 📊 Benchmark Summary ---")
    print(f"  Total Runs:   {count}")
    print(f"  Total Time:   {total_time:.4f}s")

    if count > 0:
        mean_time = statistics.mean(run_times)
        print(f"  Mean Time:    {mean_time:.4f}s")

    if count > 1:
        min_time = min(run_times)
        max_time = max(run_times)
        std_dev = statistics.stdev(run_times)
        print(f"  Min Time:     {min_time:.4f}s")
        print(f"  Max Time:     {max_time:.4f}s")
        print(f"  Std. Dev:     {std_dev:.4f}s")

    print("\nAll Run Times (s):")
    for i, t in enumerate(run_times, 1):
        print(f"  Run {i}: {t:.4f}s")


def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(
        description="Python benchmark driver for the OCaml simulation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "executable", help="Path to the compiled OCaml executable (e.g., './main.exe')."
    )
    parser.add_argument(
        "program_json", help="Path to the input compiled program JSON file."
    )
    parser.add_argument(
        "config_json", help="Path to the input scheduler config JSON file."
    )

    parser.add_argument(
        "-n",
        "--num-runs",
        type=int,
        default=10,
        help="The number of times to execute the benchmark.",
    )

    parser.add_argument(
        "-i",
        "--iterations",
        type=int,
        default=1000,
        help="The 'max_iterations' parameter for the OCaml program.",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=".",
        help="Directory to store output CSV files.",
    )

    args = parser.parse_args()

    for f in [args.executable, args.program_json, args.config_json]:
        if not os.path.exists(f):
            print(f"Error: Input file not found at '{f}'", file=sys.stderr)
            sys.exit(1)

    if not os.access(args.executable, os.X_OK):
        print(
            f"Error: Executable at '{args.executable}' is not executable.",
            file=sys.stderr,
        )
        print(f"Please run: chmod +x {args.executable}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    run_times = run_benchmark(
        args.executable,
        args.program_json,
        args.config_json,
        args.iterations,
        args.num_runs,
        args.output_dir,
    )

    print_summary(run_times)


if __name__ == "__main__":
    main()
