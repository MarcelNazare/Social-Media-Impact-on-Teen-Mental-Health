help:
	@echo make csv2parquet - to convert the csv file into a parquet
	@echo make ruff - to run checks


csv2parquet:
	@ uv run ./scripts/csv_to_parquet.py

ruff:
	@uv run ruff check

degrade:
	@uv remove setuptools
	@uv add "setuptools<82.0.0"