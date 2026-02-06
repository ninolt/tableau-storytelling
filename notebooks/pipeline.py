import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import numpy as np

    return mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Tableau Storytelling Data Pipeline

    This notebook serves as the interactive control center for the data pipeline.
    We follow a strict **English-only** naming convention and prioritize **type safety**.

    ## Pipeline Steps:
    1.  **Download**: Fetch INSEE (Reference Populations) and Data.gouv (Land Value Requests).
    2.  **Process**: Clean and format data using Polars batch/streaming logic.
    3.  **Join**: Aggregate and merge datasets by municipality.
    4.  **Export**: Save a single compact Parquet file.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Data Ingestion
    """)
    return


@app.cell
def _(pl):
    # This cell will handle the large datasets using batching/streaming
    def load_land_value_data(file_path: str) -> pl.LazyFrame:
        """
        Loads the geolocated land value dataset as a LazyFrame for streaming.
        """
        return pl.scan_csv(file_path, low_memory=True, rechunk=True)

    return


if __name__ == "__main__":
    app.run()
