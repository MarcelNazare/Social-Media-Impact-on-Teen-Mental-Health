from pathlib import Path
import polars as pl
import sys
import os



def csv_to_parquet(csv_path: str, parquet_path: str, compression: str = "zstd"):
    """
    Convert a CSV file to a Parquet file using Polars.

    Args:
        csv_path (str): Path to the input CSV file.
        parquet_path (str): Path to the output Parquet file.
        compression (str): Compression type ('zstd', 'snappy', 'gzip', 'lz4', or None).
    """
    try:
        # Validate file existence
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Read CSV in streaming mode for large files
        df = pl.read_csv(csv_path)

        # Write to Parquet with chosen compression
        df.write_parquet(parquet_path, compression=compression)

        print(f"✅ Successfully converted '{csv_path}' → '{parquet_path}' using {compression} compression.")

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)


if __name__ == "__main__":
    # Example usage
    current_dir = Path.cwd()
    print("________________________________________________")
    data_folder_raw = current_dir / "data" / "raw" 
    input_csv =  f"{data_folder_raw}\\Teen_Mental_Health_Dataset.csv"
    output_parquet = f"{data_folder_raw}\\unprocessed_data.parquet"


    csv_to_parquet(input_csv, output_parquet)