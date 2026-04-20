import polars as pl
from pathlib import Path


def load_cleaned_data():
    """
    Load the cleaned data from the parquet file in ../data/processed.
    
    Returns:
        pl.DataFrame: The cleaned data as a Polars DataFrame.
    """
    data_path = Path("../data/processed/cleaned_data.parquet")
    if not data_path.exists():
        raise FileNotFoundError(f"Cleaned data file not found: {data_path}")
    df = pl.read_parquet(data_path)
    return df
