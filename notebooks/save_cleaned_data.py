import polars as pl
from pathlib import Path


def save_cleaned_data(df: pl.DataFrame):
    """
    Save the cleaned data to a parquet file in ../data/processed.
    
    Args:
        df (pl.DataFrame): The cleaned data as a Polars DataFrame.
    """
    # Define the path to save the cleaned data
    save_path = Path("../data/processed/cleaned_data.parquet")
    
    # Ensure the directory exists
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the cleaned data to a parquet file
    df.write_parquet(save_path)

    return save_path


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

