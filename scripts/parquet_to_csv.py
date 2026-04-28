#!/usr/bin/env python3
"""Convert parquet files to CSV format."""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl


def convert_file(parquet_path: Path, input_dir: Path, output_dir: Path) -> tuple[Path, bool, str]:
    """Convert a single parquet file to CSV.

    Returns tuple of (path, success, message).
    """
    try:
        # Create flattened output name: folder_subfolder_filename.csv
        relative = parquet_path.relative_to(input_dir)
        parts = list(relative.parts)
        if len(parts) > 1:
            # Has subdirectories - join with underscores
            csv_name = "_".join(parts[:-1]) + "_" + parts[-1].replace(".parquet", ".csv")
        else:
            csv_name = parts[0].replace(".parquet", ".csv")

        output_path = output_dir / csv_name

        # Read parquet and write CSV
        df = pl.read_parquet(parquet_path)
        df.write_csv(output_path)

        return (parquet_path, True, f"Converted: {csv_name}")
    except Exception as e:
        return (parquet_path, False, f"Failed {parquet_path.name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Convert parquet files to CSV")
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=Path.home() / "data",
        help="Input directory containing parquet files (default: ~/data)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path.home() / "output" / "csv",
        help="Output directory for CSV files (default: ~/output/csv)"
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    args = parser.parse_args()

    input_dir = args.input.expanduser().resolve()
    output_dir = args.output.expanduser().resolve()

    # Validate input directory
    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}")
        sys.exit(1)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all parquet files
    parquet_files = list(input_dir.rglob("*.parquet"))
    total = len(parquet_files)

    if total == 0:
        print(f"No parquet files found in {input_dir}")
        sys.exit(0)

    print(f"Found {total} parquet files in {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Using {args.workers} workers")
    print("-" * 50)

    # Process files in parallel
    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(convert_file, pf, input_dir, output_dir): pf
            for pf in parquet_files
        }

        for future in as_completed(futures):
            path, success, message = future.result()
            completed += 1
            if not success:
                failed += 1
                print(f"[{completed}/{total}] ERROR: {message}")
            else:
                print(f"[{completed}/{total}] {message}")

    print("-" * 50)
    print(f"Completed: {completed - failed}/{total} files converted successfully")
    if failed > 0:
        print(f"Failed: {failed} files")
        sys.exit(1)


if __name__ == "__main__":
    main()