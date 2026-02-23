import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
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
    4.  **Export**: Save a single compact CSV file.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Data Acquisition & Setup

    ### Datasets References
    - **Land Value Data (DVF)**: [Reference on data.gouv.fr](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees)
    - **Permanent Base of Facilities (BPE)**: [Reference on insee.fr](https://www.insee.fr/fr/statistiques/8217527?sommaire=8217537) (Dénombrement des équipements par commune, intercommunalité, département, région...)
    - **Communes & Geographic Coordinates**: [Reference on data.gouv.fr](https://www.data.gouv.fr/datasets/communes-et-villes-de-france-en-csv-excel-json-parquet-et-feather)
    """)
    return


@app.cell
def _(Path, gzip, requests, shutil, zipfile):
    DOWNLOAD_CHUNK_SIZE = 8192

    def download_file(url: str, dest_path: Path) -> None:
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
        print(f"Extracting {zip_path} to {extract_to}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print("Extraction complete.")

    def extract_gzip(gz_path: Path, dest_path: Path) -> None:
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
    bpe_url = "https://www.insee.fr/fr/statistiques/fichier/8217527/DS_BPE_CSV_FR.zip"
    bpe_zip_path = DATA_DIR / "bpe.zip"
    bpe_extract_dir = DATA_DIR / "bpe"

    # Communes Geographic Coordinates
    communes_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/d488255f-7902-4a5d-8e46-e23309745fe5"
    )
    communes_gz_path = DATA_DIR / "communes.csv.gz"
    communes_path = DATA_DIR / "communes.csv"
    return (
        bpe_extract_dir,
        bpe_url,
        bpe_zip_path,
        communes_gz_path,
        communes_path,
        communes_url,
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
    communes_gz_path,
    communes_path,
    communes_url,
    download_file,
    dvf_path,
    dvf_url,
    extract_gzip,
    extract_zip,
):
    # Execute downloads
    download_file(dvf_url, dvf_path)
    download_file(bpe_url, bpe_zip_path)
    download_file(communes_url, communes_gz_path)

    # Extract BPE if not already done
    if not bpe_extract_dir.exists():
        extract_zip(bpe_zip_path, bpe_extract_dir)

    # Extract DVF if not already done
    dvf_csv_path = dvf_path.with_suffix("")
    if not dvf_csv_path.exists():
        extract_gzip(dvf_path, dvf_csv_path)

    # Extract Communes if not already done
    if not communes_path.exists():
        extract_gzip(communes_gz_path, communes_path)

    # Resolve BPE data file paths
    bpe_data_file = bpe_extract_dir / "DS_BPE_2024_data.csv"
    bpe_metadata_file = bpe_extract_dir / "DS_BPE_2024_metadata.csv"

    # Verify extracted files exist
    if not bpe_data_file.exists():
        raise FileNotFoundError(f"BPE data file not found: {bpe_data_file}")
    if not bpe_metadata_file.exists():
        raise FileNotFoundError(f"BPE metadata file not found: {bpe_metadata_file}")
    if not dvf_csv_path.exists():
        raise FileNotFoundError(f"DVF CSV file not found: {dvf_csv_path}")
    if not communes_path.exists():
        raise FileNotFoundError(f"Communes CSV file not found: {communes_path}")
    return bpe_data_file, bpe_metadata_file, dvf_csv_path


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Facility Types Filtering Configuration

    Define the strategic facilities to analyze across five market drivers.
    Each cluster of facility types influences property values in distinct ways.
    """)
    return


@app.cell
def _():
    MIN_SALES = 2

    SELECTED_FACILITY_TYPES: list[str] = [
        "B207",  # Boulangerie-pâtisserie (Indispensable)
        "B201",  # Supérette (Emergency shopping)
        "B202",  # Épicerie (Emergency shopping)
        "B105",  # Supermarché (Weekly groceries)
        "C107",  # École maternelle
        "C108",  # École primaire
        "D265",  # Médecin généraliste
        "D307",  # Pharmacie
        "A203",  # Banque, caisse d'épargne
        "A206",  # Bureau de poste
    ]
    return MIN_SALES, SELECTED_FACILITY_TYPES


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Real Estate Market Analysis (DVF)

    We process the land value data to analyze market trends between **2021 and 2024**.
    We focus on sales of houses and apartments, ensuring data continuity and statistical relevance.
    """)
    return


@app.cell
def _(dvf_csv_path, pl):
    dvf_base = (
        pl.scan_csv(dvf_csv_path, schema_overrides={"code_commune": pl.String})
        .select(
            [
                "id_mutation",
                "date_mutation",
                "nature_mutation",
                "valeur_fonciere",
                "code_commune",
                "code_type_local",
                "surface_reelle_bati",
            ]
        )
        .with_columns(pl.col("date_mutation").str.to_date())
        .filter(
            (pl.col("date_mutation") >= pl.date(2021, 1, 1))
            & (pl.col("date_mutation") <= pl.date(2024, 12, 31))
            & (pl.col("nature_mutation") == "Vente")
            & (pl.col("valeur_fonciere").is_not_null())
            & (pl.col("valeur_fonciere") > 0)
            & ~pl.col("code_commune").str.starts_with("97")
            & ~pl.col("code_commune").str.starts_with("98")
        )
        .drop("nature_mutation")
    )
    return (dvf_base,)


@app.cell
def _(dvf_base, pl):
    dvf_cleaned = (
        dvf_base.group_by("id_mutation")
        .agg(
            [
                pl.col("date_mutation").first(),
                pl.col("valeur_fonciere").first(),
                pl.col("code_commune").first(),
                (pl.col("code_type_local") == 4).any().alias("has_type_4"),
                pl.col("surface_reelle_bati")
                .fill_null(0)
                .sum()
                .alias("surface_reelle_bati"),
            ]
        )
        .filter(~pl.col("has_type_4") & (pl.col("surface_reelle_bati") > 0))
        .drop("has_type_4")
    )
    return (dvf_cleaned,)


@app.cell
def _(dvf_cleaned, pl):
    dvf_with_metrics = (
        dvf_cleaned.with_columns(
            [
                pl.col("date_mutation").dt.year().alias("year"),
                (pl.col("valeur_fonciere") / pl.col("surface_reelle_bati")).alias(
                    "prix_m2"
                ),
            ]
        )
        .drop(["date_mutation", "valeur_fonciere", "surface_reelle_bati"])
        .filter(pl.col("year").is_in([2021, 2024]))
    )
    return (dvf_with_metrics,)


@app.cell
def _(MIN_SALES, dvf_with_metrics, pl):
    dvf_agg = dvf_with_metrics.group_by(
        ["code_commune", "year"]
    ).agg(
        [
            pl.col("prix_m2").mean().alias("avg_prix_m2"),
            pl.col("prix_m2").median().alias("median_prix_m2"),
            pl.col("id_mutation").count().alias("count_sales"),
        ]
    ).filter(pl.col("count_sales") >= MIN_SALES)
    return (dvf_agg,)


@app.cell
def _(dvf_agg, pl):
    communes_with_both_years = (
        dvf_agg.group_by("code_commune")
        .agg(pl.col("year").unique().alias("years"))
        .filter(
            pl.col("years").list.contains(2021)
            & pl.col("years").list.contains(2024)
        )
        .select(["code_commune"])
    )

    dvf_complete_years = dvf_agg.join(
        communes_with_both_years, on="code_commune", how="semi"
    )
    return (dvf_complete_years,)


@app.cell
def _(dvf_complete_years, pl):
    # Collect and pivot on year to get 2021 and 2024 as separate columns
    dvf_collected = dvf_complete_years.collect()

    dvf_pivoted = dvf_collected.pivot(
        on="year",
        values=["avg_prix_m2", "median_prix_m2", "count_sales"],
        index="code_commune",
    )

    # Calculate growth and keep only 2024 data with growth metric
    dvf_final = (
        dvf_pivoted.with_columns(
            growth_prix_m2=(
                (pl.col("median_prix_m2_2024") - pl.col("median_prix_m2_2021"))
                / pl.col("median_prix_m2_2021")
                * 100
                / 4
            )
        )
        .filter(
            (pl.col("growth_prix_m2") >= -100)
            & (pl.col("growth_prix_m2") <= 200)
        )
        .select(
            [
                "code_commune",
                pl.col("avg_prix_m2_2024").alias("avg_prix_m2"),
                pl.col("median_prix_m2_2024").alias("median_prix_m2"),
                pl.col("count_sales_2024").alias("count_sales"),
                "growth_prix_m2",
            ]
        )
        .with_columns(
            (
                (pl.col("growth_prix_m2") - pl.col("growth_prix_m2").mean())
                / pl.col("growth_prix_m2").std()
            ).alias("growth_prix_m2_standardized")
        )
    )
    return (dvf_final,)


@app.cell
def _(dvf_final):
    dvf_final
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Facilities (BPE) Processing & Filtering

    Load and filter the Permanent Base of Facilities (BPE) to retain only
    the strategic facility types defined in the configuration above.
    """)
    return


@app.cell
def _(SELECTED_FACILITY_TYPES: list[str], bpe_data_file, pl):
    bpe_base = (
        pl.scan_csv(
            bpe_data_file,
            separator=";",
            schema_overrides={"GEO": pl.String},
        )
        .select(
            [
                "GEO",
                "GEO_OBJECT",
                "FACILITY_TYPE",
                "OBS_VALUE",
            ]
        )
        .filter(pl.col("GEO_OBJECT").is_in(["ARM", "COM"]))
        .filter(pl.col("FACILITY_TYPE").is_in(SELECTED_FACILITY_TYPES))
        .filter(
            ~pl.col("GEO").str.starts_with("97")
            & ~pl.col("GEO").str.starts_with("98")
        )
        .drop("GEO_OBJECT")
    )
    return (bpe_base,)


@app.cell
def _(SELECTED_FACILITY_TYPES: list[str], bpe_metadata_file, pl):
    facility_type_labels = (
        pl.read_csv(
            bpe_metadata_file,
            separator=";",
        )
        .filter(pl.col("COD_VAR") == "FACILITY_TYPE")
        .select(["COD_MOD", "LIB_MOD"])
        .filter(pl.col("COD_MOD").is_in(SELECTED_FACILITY_TYPES))
        .unique()
    )

    label_mapping = {
        row["COD_MOD"]: row["LIB_MOD"] for row in facility_type_labels.to_dicts()
    }
    return (label_mapping,)


@app.cell
def _(bpe_base, label_mapping, pl):
    bpe_final = (
        bpe_base.collect()
        .rename({"GEO": "code_commune"})
        .with_columns(pl.col("OBS_VALUE").cast(pl.Int64))
        .pivot(
            on="FACILITY_TYPE",
            values="OBS_VALUE",
            index="code_commune",
            aggregate_function="sum",
        )
        .fill_null(0)
        .rename(label_mapping)
        .with_columns(pl.sum_horizontal(pl.exclude("code_commune")).alias("Total"))
    )
    return (bpe_final,)


@app.cell
def _(bpe_final):
    bpe_final
    return


@app.cell
def _(bpe_final, communes_path, dvf_final, label_mapping, pl):
    bpe_with_normalized_total = bpe_final.with_columns(
        (
            (pl.col("Total") - pl.col("Total").mean())
            / pl.col("Total").std()
        ).alias("Total_normalized")
    )

    bpe_renamed = bpe_with_normalized_total.lazy()

    # Add all DVF communes (even those without BPE data), filling with 0
    dvf_communes_only = dvf_final.select("code_commune").unique().lazy()

    bpe_with_all_communes = dvf_communes_only.join(
        bpe_renamed, on="code_commune", how="left"
    ).fill_null(0)

    communes_gps = (
        pl.scan_csv(communes_path, schema_overrides={"code_insee": pl.Utf8})
        .select(["code_insee", "latitude_centre", "longitude_centre"])
        .rename({"code_insee": "code_commune"})
    )

    commune_facilities_with_gps = (
        bpe_with_all_communes.join(
            communes_gps,
            on="code_commune",
            how="left",
        )
        .with_columns(
            [
                pl.col("latitude_centre").alias("latitude"),
                pl.col("longitude_centre").alias("longitude"),
            ]
        )
        .drop(["latitude_centre", "longitude_centre"])
    )

    facility_columns = list(label_mapping.values())
    return commune_facilities_with_gps, facility_columns


@app.cell
def _(commune_facilities_with_gps, facility_columns, pl):
    def calculate_distance_to_nearest_facility(
        dataset: pl.LazyFrame, facility_code: str
    ) -> pl.LazyFrame:
        import numpy as np

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

        facilities_array = facility_coords.select(
            ["latitude", "longitude"]
        ).to_numpy()  # shape: [N_facilities, 2]
        communes_array = without_facility.select(
            ["latitude", "longitude"]
        ).to_numpy()  # shape: [M_communes, 2]

        squared_diff = (
            communes_array[:, np.newaxis, :] - facilities_array[np.newaxis, :, :]
        ) ** 2
        distances_matrix = np.sqrt(squared_diff.sum(axis=2)) * 111.0

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

    bpe_with_distances = commune_facilities_with_gps
    for facility_code in facility_columns:
        print(f"Calculating distance to {facility_code}...")
        bpe_with_distances = calculate_distance_to_nearest_facility(
            bpe_with_distances, facility_code
        )

    print("Distance calculations complete!")
    return (bpe_with_distances,)


@app.cell
def _(bpe_with_distances, dvf_final):
    bpe_to_join = bpe_with_distances.unique()

    final_dataset_with_distances = dvf_final.lazy().join(
        bpe_to_join, on="code_commune", how="inner"
    )
    return (final_dataset_with_distances,)


@app.cell
def _(Path, final_dataset_with_distances):
    output_file = Path(__file__).parent.parent / "data" / "final_dataset.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    collected_dataset = final_dataset_with_distances.collect()
    collected_dataset.write_csv(output_file)

    print(f"Final dataset with distances saved to {output_file}")
    print(f"Shape: {collected_dataset.shape}")
    print(f"Columns: {collected_dataset.columns}")
    return (collected_dataset,)


@app.cell
def _(collected_dataset):
    collected_dataset.head()
    return


if __name__ == "__main__":
    app.run()
