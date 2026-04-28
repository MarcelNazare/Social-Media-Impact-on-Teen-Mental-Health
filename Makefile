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

freeze:
	@uv pip freeze > requirements.txt
	@uv sync

parquet2csv:
	@uv run ./scripts/parquet_to_csv.py -i "C:\Users\marcel\Documents\Data Analysis Projects\Social Media Impact on Teen Mental Health Project\Social Media Impact on Teen Mental Health\data\processed" -o "C:\Users\marcel\Documents\Data Analysis Projects\Social Media Impact on Teen Mental Health Project\Social Media Impact on Teen Mental Health\data\processed"