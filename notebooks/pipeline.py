import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import numpy as np
    import requests
    import zipfile
    import gzip
    import shutil
    from pathlib import Path

    return Path, gzip, mo, pl, requests, shutil, zipfile


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
    mo.md(rf"""
    ## 1. Data Ingestion

    ### Datasets References
    - **Land Value Data (DVF)**: [Reference on data.gouv.fr](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees)
    - **Permanent Base of Facilities (BPE)**: [Source on insee.fr](https://www.insee.fr/fr/statistiques/8217532?sommaire=8217537) (Présence ou absence de type d'équipements par commune)
    """)
    return


@app.cell
def _(Path, requests, zipfile):
    def download_file(url: str, dest_path: Path):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if not dest_path.exists():
            print(f"Downloading {url} to {dest_path}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download complete.")
        else:
            print(f"File {dest_path} already exists.")

    def extract_zip(zip_path: Path, extract_to: Path):
        print(f"Extracting {zip_path} to {extract_to}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print("Extraction complete.")

    def extract_gzip(gz_path: Path, dest_path: Path):
        print(f"Extracting {gz_path} to {dest_path}...")
        with gzip.open(gz_path, "rb") as f_in:
            with open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Extraction complete.")

    DATA_DIR = Path(__file__).parent.parent / "data"

    # DVF Data
    dvf_url = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251105-140205/dvf.csv.gz"
    dvf_path = DATA_DIR / "dvf.csv.gz"

    # BPE Data
    bpe_url = "https://www.insee.fr/fr/statistiques/fichier/8217532/ds_bpe_evolution_com_2019_2024_geo_2025.zip"
    bpe_zip_path = DATA_DIR / "bpe.zip"
    bpe_extract_dir = DATA_DIR / "bpe"

    return (
        DATA_DIR,
        bpe_extract_dir,
        bpe_url,
        bpe_zip_path,
        download_file,
        dvf_path,
        dvf_url,
        extract_gzip,
        extract_zip,
    )


@app.cell
def _(
    bpe_extract_dir,
    bpe_url,
    bpe_zip_path,
    download_file,
    dvf_path,
    dvf_url,
    extract_gzip,
    extract_zip,
):
    # Execute downloads
    download_file(dvf_url, dvf_path)
    download_file(bpe_url, bpe_zip_path)

    # Extract BPE if not already done
    if not bpe_extract_dir.exists():
        extract_zip(bpe_zip_path, bpe_extract_dir)

    # Extract DVF if not already done
    dvf_csv_path = dvf_path.with_suffix("")
    if not dvf_csv_path.exists():
        extract_gzip(dvf_path, dvf_csv_path)
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
