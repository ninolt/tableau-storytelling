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
    # Analysis period parameters
    ANALYSIS_START_YEAR = 2021
    ANALYSIS_END_YEAR = 2024
    YEARS_FOR_CONTINUITY = ANALYSIS_END_YEAR - ANALYSIS_START_YEAR + 1

    # Facility Type Filters by Strategic Cluster
    FACILITY_TYPES_NEIGHBORHOOD_ESSENTIALS: list[str] = [
        "B207",  # Boulangerie-pâtisserie (Indispensable)
        "A504",  # Restaurant-restauration rapide (Social life)
        "B201",  # Supérette (Emergency shopping)
        "B202",  # Épicerie (Emergency shopping)
        "B105",  # Supermarché (Weekly groceries)
        "B204",  # Boucherie charcuterie (Quality indicator)
        "A501",  # Coiffure (Proximity service)
        "B324",  # Librairie (Cultural marker)
    ]

    FACILITY_TYPES_FAMILY: list[str] = [
        "D502",  # Établissement d'accueil du jeune enfant (Crèches)
        "C107",  # École maternelle
        "C108",  # École primaire
        "C201",  # Collège
        "C301",  # Lycée
    ]

    FACILITY_TYPES_HEALTH: list[str] = [
        "D265",  # Médecin généraliste
        "D307",  # Pharmacie
        "D277",  # Chirurgien dentiste
        "D281",  # Infirmier
        "D106",  # Urgences
    ]

    FACILITY_TYPES_LIFESTYLE_SPORT: list[str] = [
        "F120",  # Salles de remise en forme (Fitness)
        "F101",  # Bassin de natation (Swimming pools)
        "F103",  # Tennis
        "F111",  # Plateaux et terrains de jeux (City stades)
        "F303",  # Cinéma
        "F307",  # Bibliothèque / Médiathèque
    ]

    FACILITY_TYPES_CONNECTIVITY: list[str] = [
        "E108",  # Gares régionales
        "E109",  # Gares locales
        "B326",  # Station de recharge de véhicules électriques
        "A203",  # Banque, caisse d'épargne
        "A206",  # Bureau de poste
    ]

    # Consolidated list of all selected facility types
    SELECTED_FACILITY_TYPES: list[str] = (
        FACILITY_TYPES_NEIGHBORHOOD_ESSENTIALS
        + FACILITY_TYPES_FAMILY
        + FACILITY_TYPES_HEALTH
        + FACILITY_TYPES_LIFESTYLE_SPORT
        + FACILITY_TYPES_CONNECTIVITY
    )
    return SELECTED_FACILITY_TYPES, YEARS_FOR_CONTINUITY


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
    MIN_SALES_HOUSE = 2
    MIN_SALES_APARTMENT = 3

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
                "surface_terrain",
                "nombre_pieces_principales",
            ]
        )
        .with_columns(
            [
                pl.col("date_mutation").str.to_date(),
                pl.col("surface_terrain").fill_null(0),
            ]
        )
        .filter(
            (pl.col("date_mutation") >= pl.date(2021, 1, 1))
            & (pl.col("date_mutation") <= pl.date(2024, 12, 31))
            & (pl.col("nature_mutation") == "Vente")
            & (pl.col("valeur_fonciere").is_not_null())
            & (pl.col("valeur_fonciere") > 0)
        )
        .drop("nature_mutation")
    )
    return MIN_SALES_APARTMENT, MIN_SALES_HOUSE, dvf_base


@app.cell
def _(dvf_base, pl):
    dvf_by_mutation = (
        dvf_base.filter(~(pl.col("code_type_local") == 4).any().over("id_mutation"))
        .filter(
            ~(pl.col("surface_reelle_bati").fill_null(0) == 0).all().over("id_mutation")
        )
        .group_by("id_mutation")
        .agg(
            [
                pl.col("date_mutation").first(),
                pl.col("valeur_fonciere").first(),
                pl.col("code_commune").first(),
                pl.col("surface_terrain").first(),
                pl.col("code_type_local").min(),
                pl.col("surface_reelle_bati").fill_null(0).sum(),
                pl.col("nombre_pieces_principales").fill_null(0).sum(),
            ]
        )
    )
    return (dvf_by_mutation,)


@app.cell
def _(dvf_by_mutation, pl):
    metrics_to_split = [
        "valeur_fonciere",
        "surface_terrain",
        "surface_reelle_bati",
        "nombre_pieces_principales",
        "prix_m2",
    ]

    dvf_with_metrics = (
        dvf_by_mutation.filter(pl.col("surface_reelle_bati") > 0)
        .with_columns(
            [
                pl.col("date_mutation").dt.year().alias("year"),
                (pl.col("valeur_fonciere") / pl.col("surface_reelle_bati")).alias(
                    "prix_m2"
                ),
            ]
        )
        .with_columns(
            [
                *[
                    pl.when(pl.col("code_type_local") == 1)
                    .then(pl.col(m))
                    .alias(f"{m}_house")
                    for m in metrics_to_split
                ],
                *[
                    pl.when(pl.col("code_type_local") == 2)
                    .then(pl.col(m))
                    .alias(f"{m}_apartment")
                    for m in metrics_to_split
                ],
            ]
        )
    )
    return dvf_with_metrics, metrics_to_split


@app.cell
def _(
    MIN_SALES_APARTMENT,
    MIN_SALES_HOUSE,
    dvf_with_metrics,
    metrics_to_split,
    pl,
):
    dvf_commune_agg = (
        dvf_with_metrics.group_by(["code_commune", "year"])
        .agg(
            [
                *[
                    pl.col(f"{m}_house").mean().alias(f"avg_{m}_house")
                    for m in metrics_to_split
                ],
                *[
                    pl.col(f"{m}_apartment").mean().alias(f"avg_{m}_apartment")
                    for m in metrics_to_split
                ],
                *[pl.col(m).mean().alias(f"avg_{m}_global") for m in metrics_to_split],
                pl.col("prix_m2_house").median().alias("median_prix_m2_house"),
                pl.col("prix_m2_apartment").median().alias("median_prix_m2_apartment"),
                pl.col("prix_m2").median().alias("median_prix_m2_global"),
                pl.col("id_mutation")
                .filter(pl.col("code_type_local") == 1)
                .count()
                .alias("count_sales_house"),
                pl.col("id_mutation")
                .filter(pl.col("code_type_local") == 2)
                .count()
                .alias("count_sales_apartment"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("count_sales_house") >= MIN_SALES_HOUSE)
                .then(pl.col("count_sales_house"))
                .otherwise(0)
                .alias("count_sales_house"),
                pl.when(pl.col("count_sales_apartment") >= MIN_SALES_APARTMENT)
                .then(pl.col("count_sales_apartment"))
                .otherwise(0)
                .alias("count_sales_apartment"),
            ]
        )
        .with_columns(
            [
                (pl.col("count_sales_house") + pl.col("count_sales_apartment")).alias(
                    "count_sales_global"
                )
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("count_sales_house") >= MIN_SALES_HOUSE)
                .then(pl.col("^(avg|median)_.*_house$"))
                .otherwise(None)
                .name.keep(),
                pl.when(pl.col("count_sales_apartment") >= MIN_SALES_APARTMENT)
                .then(pl.col("^(avg|median)_.*_apartment$"))
                .otherwise(None)
                .name.keep(),
            ]
        )
    )
    return (dvf_commune_agg,)


@app.cell
def _(YEARS_FOR_CONTINUITY, dvf_commune_agg, pl):
    dvf_filtered = (
        dvf_commune_agg.with_columns(
            [
                (
                    pl.col("avg_valeur_fonciere_house")
                    .is_not_null()
                    .sum()
                    .over("code_commune")
                    == YEARS_FOR_CONTINUITY
                ).alias("has_house_continuity"),
                (
                    pl.col("avg_valeur_fonciere_apartment")
                    .is_not_null()
                    .sum()
                    .over("code_commune")
                    == YEARS_FOR_CONTINUITY
                ).alias("has_apartment_continuity"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("has_house_continuity"))
                .then(pl.col("^(avg|median)_.*_house$"))
                .otherwise(None)
                .name.keep(),
                pl.when(pl.col("has_apartment_continuity"))
                .then(pl.col("^(avg|median)_.*_apartment$"))
                .otherwise(None)
                .name.keep(),
            ]
        )
        .filter(pl.col("has_house_continuity") | pl.col("has_apartment_continuity"))
        .drop(["has_house_continuity", "has_apartment_continuity"])
    )
    return (dvf_filtered,)


@app.cell
def _(dvf_filtered, pl):
    metrics = ["avg_prix_m2_global", "avg_prix_m2_apartment", "avg_prix_m2_house"]

    dvf_with_growth = (
        dvf_filtered.filter(pl.col("year").is_in([2021, 2024]))
        .with_columns(
            [
                pl.when(pl.col("year") == year)
                .then(pl.col(metric))
                .otherwise(None)
                .alias(f"{metric}_{year}")
                for year in [2021, 2024]
                for metric in metrics
            ]
        )
        .group_by("code_commune")
        .agg(
            [
                pl.col(f"{metric}_{year}").max()
                for year in [2021, 2024]
                for metric in metrics
            ]
        )
        .with_columns(
            [
                (
                    (pl.col(f"{metric}_2024") - pl.col(f"{metric}_2021"))
                    / pl.col(f"{metric}_2021")
                    * 100
                ).alias(f"price_growth_pct_{metric.split('_')[-1]}")
                for metric in metrics
            ]
        )
        .select(
            [
                "code_commune",
                "price_growth_pct_house",
                "price_growth_pct_apartment",
                "price_growth_pct_global",
            ]
        )
    )

    dvf_final = dvf_filtered.join(
        dvf_with_growth, on="code_commune", how="left"
    ).select(
        [
            "code_commune",
            "year",
            pl.col("^(avg|median|count)_.*_house$"),
            pl.col("^(avg|median|count)_.*_apartment$"),
            pl.col("^(avg|median|count)_.*_global$"),
            pl.col("^price_growth_pct_.*$"),
        ]
    )
    return (dvf_final,)


@app.cell
def _(dvf_final):
    dvf_final.limit(10).collect()
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
        .drop("GEO_OBJECT")
    )
    return (bpe_base,)


@app.cell
def _(bpe_base, dvf_final):
    bpe_filtered = bpe_base.join(
        dvf_final.select("code_commune").unique().lazy(),
        left_on="GEO",
        right_on="code_commune",
        how="semi",
    )
    return (bpe_filtered,)


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
def _(bpe_filtered, label_mapping, pl):
    bpe_final = (
        bpe_filtered.collect()
        .pivot(
            on="FACILITY_TYPE",
            values="OBS_VALUE",
            index="GEO",
            aggregate_function="sum",
        )
        .fill_null(0)
        .rename(label_mapping)
        .with_columns(pl.sum_horizontal(pl.exclude("GEO")).alias("Total"))
    )
    return (bpe_final,)


@app.cell
def _(bpe_final):
    bpe_final.limit(10)
    return


@app.cell
def _(bpe_final, communes_path, label_mapping, pl):
    bpe_renamed = bpe_final.rename({"GEO": "code_commune"}).lazy()

    communes_gps = (
        pl.scan_csv(communes_path, schema_overrides={"code_insee": pl.Utf8})
        .select(["code_insee", "latitude_centre", "longitude_centre"])
        .rename({"code_insee": "code_commune"})
    )

    commune_facilities_with_gps = (
        bpe_renamed.join(
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
    final_dataset_with_distances = bpe_with_distances.join(
        dvf_final, on="code_commune", how="left"
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
    return


@app.cell
def _(final_dataset_with_distances):
    final_dataset_with_distances.limit(5).collect()
    return


if __name__ == "__main__":
    app.run()
