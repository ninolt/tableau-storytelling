import gzip
import shutil
import zipfile
from pathlib import Path

import polars as pl
import requests

DOWNLOAD_CHUNK_SIZE = 8192


def download_file(url: str, dest_path: Path) -> None:
    """Download a file from URL to destination path with streaming."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if not dest_path.exists():
        print(f"Downloading {url} to {dest_path}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                f.write(chunk)
        print("Download complete.")
    else:
        print(f"File {dest_path} already exists.")


def extract_zip(zip_path: Path, extract_to: Path) -> None:
    """Extract a ZIP archive to the specified directory."""
    print(f"Extracting {zip_path} to {extract_to}...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    print("Extraction complete.")


def extract_gzip(gz_path: Path, dest_path: Path) -> None:
    """Extract a GZIP compressed file to destination path."""
    print(f"Extracting {gz_path} to {dest_path}...")
    with gzip.open(gz_path, "rb") as f_in:
        with open(dest_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("Extraction complete.")


def setup_data_directory() -> Path:
    """Return the data directory path relative to this utils file."""
    return Path(__file__).parent.parent / "data"


def download_dvf_dataset(data_dir: Path) -> Path:
    """Download and extract DVF (Land Value) dataset."""
    dvf_url = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251105-140205/dvf.csv.gz"
    dvf_gz_path = data_dir / "dvf.csv.gz"
    dvf_csv_path = data_dir / "dvf.csv"

    download_file(dvf_url, dvf_gz_path)

    if not dvf_csv_path.exists():
        extract_gzip(dvf_gz_path, dvf_csv_path)

    return dvf_csv_path


def download_bpe_dataset(data_dir: Path) -> tuple[Path, Path]:
    """Download and extract BPE (Facilities) dataset."""
    bpe_url = "https://www.insee.fr/fr/statistiques/fichier/8217527/DS_BPE_CSV_FR.zip"
    bpe_zip_path = data_dir / "bpe.zip"
    bpe_extract_dir = data_dir / "bpe"

    download_file(bpe_url, bpe_zip_path)

    if not bpe_extract_dir.exists():
        extract_zip(bpe_zip_path, bpe_extract_dir)

    bpe_data_file = bpe_extract_dir / "DS_BPE_2024_data.csv"
    bpe_metadata_file = bpe_extract_dir / "DS_BPE_2024_metadata.csv"

    if not bpe_data_file.exists():
        raise FileNotFoundError(f"BPE data file not found: {bpe_data_file}")
    if not bpe_metadata_file.exists():
        raise FileNotFoundError(f"BPE metadata file not found: {bpe_metadata_file}")

    return bpe_data_file, bpe_metadata_file


def download_communes_dataset(data_dir: Path) -> Path:
    """Download and extract communes (geographic coordinates) dataset."""
    communes_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/d488255f-7902-4a5d-8e46-e23309745fe5"
    )
    communes_gz_path = data_dir / "communes.csv.gz"
    communes_csv_path = data_dir / "communes.csv"

    download_file(communes_url, communes_gz_path)

    if not communes_csv_path.exists():
        extract_gzip(communes_gz_path, communes_csv_path)

    return communes_csv_path


def calculate_nearest_facility_distance_matrix(
    dataset: "pl.LazyFrame", facility_code: str
) -> "pl.LazyFrame":
    """
    Calculate distance to nearest facility of a given type for each municipality.

    For municipalities with the facility (count > 0), distance is 0.
    For municipalities without, calculates geodesic distance to nearest facility
    using Haversine approximation (1 degree ≈ 111 km).

    Args:
        dataset: LazyFrame with columns [code_commune, latitude, longitude, {facility_code}]
        facility_code: Name of the facility column to calculate distances for

    Returns:
        LazyFrame with added column: distance_{facility_code}
    """
    import numpy as np
    import polars as pl

    KM_PER_DEGREE = 111.0

    facility_coords = (
        dataset.filter(pl.col(facility_code) > 0)
        .select(["latitude", "longitude"])
        .unique()
        .collect()
    )

    with_facility = (
        dataset.filter(pl.col(facility_code) > 0)
        .select(["code_commune"])
        .unique()
        .with_columns(pl.lit(0.0).alias(f"distance_{facility_code}"))
    )

    without_facility = (
        dataset.filter(pl.col(facility_code) == 0)
        .select(["code_commune", "latitude", "longitude"])
        .unique()
        .collect()
    )

    facilities_array = facility_coords.select(["latitude", "longitude"]).to_numpy()
    communes_array = without_facility.select(["latitude", "longitude"]).to_numpy()

    squared_diff = (
        communes_array[:, np.newaxis, :] - facilities_array[np.newaxis, :, :]
    ) ** 2
    distances_matrix = np.sqrt(squared_diff.sum(axis=2)) * KM_PER_DEGREE

    min_distances = np.nanmin(distances_matrix, axis=1)

    without_facility_distances = pl.DataFrame(
        {
            "code_commune": without_facility.select("code_commune")
            .to_series()
            .to_list(),
            f"distance_{facility_code}": min_distances.tolist(),
        }
    )

    distances_complete = pl.concat(
        [with_facility.collect(), without_facility_distances]
    )

    return dataset.join(distances_complete.lazy(), on="code_commune", how="left")
