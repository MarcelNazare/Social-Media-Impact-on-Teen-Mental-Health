import polars as pl
from pathlib import Path

def load_unprocessed_data():
    """
    Load the unprocessed data from the parquet file in ../data/raw.
    
    Returns:
        pl.DataFrame: The unprocessed data as a Polars DataFrame.
    """
    # Define the path to the unprocessed data parquet file
    data_path = Path("../data/raw/unprocessed_data.parquet")
    
    # Check if the file exists
    if not data_path.exists():
        raise FileNotFoundError(f"Unprocessed data file not found: {data_path}")
    
    # Read the parquet file
    df = pl.read_parquet(data_path)
    
    return df

