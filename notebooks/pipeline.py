import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.utils import (
        setup_data_directory,
        download_dvf_dataset,
        download_bpe_dataset,
        download_communes_dataset,
        calculate_nearest_facility_distance_matrix,
    )

    return (
        Path,
        calculate_nearest_facility_distance_matrix,
        download_bpe_dataset,
        download_communes_dataset,
        download_dvf_dataset,
        mo,
        pl,
        setup_data_directory,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Real Estate vs. Essential Services Data Pipeline

    ## Project Context

    This pipeline processes and merges French real estate transactions (DVF) with public facilities data (BPE)
    to analyze the relationship between **property prices** and **access to essential services** at the municipal level.

    ### Research Question
    Does the price per square meter actually buy access to essential services, or do geographic prestige
    and market speculation dominate property values?

    ### Methodology

    Focus on **metropolitan France in 2024** (excluding overseas territories and Alsace-Moselle due to data unavailability).

    **10 Essential Services Selected:**
    - **Food**: Bakeries, convenience stores, grocery stores, supermarkets
    - **Education**: Kindergartens, primary schools
    - **Healthcare**: General practitioners, pharmacies
    - **Services**: Post offices, banks

    ### Output Dataset

    The final dataset contains one row per municipality with:
    - **Real estate metrics**: Median price per m², transaction volume
    - **Facilities counts**: Number of each service type present
    - **Proximity metrics**: Distance (km) to nearest facility for each service type (0 if present locally)
    - **Geographic identifiers**: INSEE code, GPS coordinates

    This data feeds visualizations exploring price-service correlations, service deserts, and geographic disparities.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Data Acquisition & Setup

    ### Data Sources (2024, Metropolitan France only)

    - **DVF (Land Value Requests)**: Real estate transactions geocoded by DGFIP
      [Source: data.gouv.fr](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees)
      *Note: Alsace-Moselle data unavailable (different cadastral system)*

    - **BPE (Permanent Base of Facilities)**: Census of public and private facilities by INSEE
      [Source: insee.fr](https://www.insee.fr/fr/statistiques/8217527?sommaire=8217537)

    - **Communes Database**: Geographic coordinates and administrative boundaries
      [Source: data.gouv.fr](https://www.data.gouv.fr/datasets/communes-et-villes-de-france-en-csv-excel-json-parquet-et-feather)
    """)
    return


@app.cell
def _(setup_data_directory):
    DATA_DIR = setup_data_directory()
    return (DATA_DIR,)


@app.cell
def _(
    DATA_DIR,
    download_bpe_dataset,
    download_communes_dataset,
    download_dvf_dataset,
):
    dvf_csv_path = download_dvf_dataset(DATA_DIR)
    bpe_data_file, bpe_metadata_file = download_bpe_dataset(DATA_DIR)
    communes_path = download_communes_dataset(DATA_DIR)
    return bpe_data_file, bpe_metadata_file, communes_path, dvf_csv_path


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Facility Types Filtering Configuration

    Define the **10 essential services** analyzed across four categories.

    These facility types are strategically chosen to represent baseline quality of life:
    access to food, education for families, basic healthcare, and administrative services.

    **Selection Rationale:**
    - **Food access**: From daily necessities (bakery) to weekly shopping (supermarket)
    - **Education**: Critical for families with young children
    - **Healthcare**: Primary care accessibility
    - **Services**: Financial and postal infrastructure
    """)
    return


@app.cell
def _():
    MIN_SALES = 2
    YEARS_BETWEEN_2021_2024 = 4
    MIN_GROWTH_PERCENT = -100
    MAX_GROWTH_PERCENT = 200

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
    return (
        MAX_GROWTH_PERCENT,
        MIN_GROWTH_PERCENT,
        MIN_SALES,
        SELECTED_FACILITY_TYPES,
        YEARS_BETWEEN_2021_2024,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Real Estate Market Analysis (DVF)

    ### Processing Strategy

    The DVF dataset contains all real estate transactions declared to the French tax authorities.
    This pipeline processes the raw data to extract **median price per m²** and **price growth trends** at the municipal level.

    **Temporal Scope**: 2021-2024 (to calculate 4-year growth rate)

    **Geographic Scope**: Metropolitan France only (codes not starting with 97/98)

    **Transaction Filtering**:
    - Sales only (exclude donations, exchanges)
    - Positive transaction value
    - Exclude "type 4" properties (dependencies: garages, parking, cellars)
    - Exclude transactions with zero built surface

    **Quality Thresholds**:
    - Minimum **2 sales per municipality per year** (statistical relevance)
    - Data continuity: keep only municipalities with data for **both 2021 and 2024**
    - Outlier filtering: growth rates between -100% and +200% (remove data errors)

    **Output**: Municipality-level statistics with price evolution metrics and z-score standardization.
    """)
    return


@app.cell
def _(dvf_csv_path, pl):
    dvf_raw_sales = (
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
    return (dvf_raw_sales,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 1: Aggregate by Transaction ID

    DVF raw data contains **multiple rows per sale** (one row per property lot).
    Group by `id_mutation` to get one row per transaction.

    Exclude transactions containing "type 4" properties (dependencies like garages, parking spots).
    """)
    return


@app.cell
def _(dvf_raw_sales, pl):
    dvf_by_transaction = (
        dvf_raw_sales.group_by("id_mutation")
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
    return (dvf_by_transaction,)


@app.cell
def _(dvf_by_transaction, mo):
    dvf_by_transaction_collected = dvf_by_transaction.collect()

    mo.md(f"""
    **Validation**: Aggregated to **{dvf_by_transaction_collected.shape[0]:,} transactions**  
    From {dvf_by_transaction_collected["code_commune"].n_unique():,} unique municipalities
    """)
    return (dvf_by_transaction_collected,)


@app.cell
def _(dvf_by_transaction_collected, pl):
    dvf_price_per_sqm = (
        dvf_by_transaction_collected.lazy()
        .with_columns(
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
    return (dvf_price_per_sqm,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 2: Calculate Price per Square Meter

    Compute `prix_m2` metric for each transaction, then filter to keep only **2021 and 2024** data.
    This reduces processing overhead by discarding intermediate years (2022, 2023).
    """)
    return


@app.cell
def _(MIN_SALES, dvf_price_per_sqm, pl):
    dvf_commune_yearly_stats = (
        dvf_price_per_sqm.group_by(["code_commune", "year"])
        .agg(
            [
                pl.col("prix_m2").mean().alias("avg_prix_m2"),
                pl.col("prix_m2").median().alias("median_prix_m2"),
                pl.col("id_mutation").count().alias("count_sales"),
            ]
        )
        .filter(pl.col("count_sales") >= MIN_SALES)
    )
    return (dvf_commune_yearly_stats,)


@app.cell
def _(dvf_commune_yearly_stats, mo, pl):
    stats_collected = dvf_commune_yearly_stats.collect()

    year_2021 = stats_collected.filter(pl.col("year") == 2021)
    year_2024 = stats_collected.filter(pl.col("year") == 2024)

    mo.md(f"""
    **Validation**: Aggregated by municipality and year
    - **2021**: {year_2021.shape[0]:,} municipalities, median price/m² = {year_2021["median_prix_m2"].median():.2f} €
    - **2024**: {year_2024.shape[0]:,} municipalities, median price/m² = {year_2024["median_prix_m2"].median():.2f} €
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 3: Aggregate by Municipality and Year

    Group transactions by `(code_commune, year)` to compute statistics.
    Filter out municipalities with insufficient sales volume (< MIN_SALES).
    """)
    return


@app.cell
def _(dvf_commune_yearly_stats, pl):
    communes_with_both_years = (
        dvf_commune_yearly_stats.group_by("code_commune")
        .agg(pl.col("year").unique().alias("years"))
        .filter(
            pl.col("years").list.contains(2021) & pl.col("years").list.contains(2024)
        )
        .select(["code_commune"])
    )

    dvf_complete_years = dvf_commune_yearly_stats.join(
        communes_with_both_years, on="code_commune", how="semi"
    )
    return (dvf_complete_years,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 4: Ensure Temporal Continuity

    Keep only municipalities with data for **both 2021 and 2024**.
    This ensures we can calculate a meaningful growth rate without missing baseline.
    """)
    return


@app.cell
def _(
    MAX_GROWTH_PERCENT,
    MIN_GROWTH_PERCENT,
    YEARS_BETWEEN_2021_2024,
    dvf_complete_years,
    pl,
):
    dvf_collected = dvf_complete_years.collect()

    dvf_pivoted = dvf_collected.pivot(
        on="year",
        values=["avg_prix_m2", "median_prix_m2", "count_sales"],
        index="code_commune",
    )

    dvf_final = (
        dvf_pivoted.with_columns(
            growth_prix_m2=(
                (pl.col("median_prix_m2_2024") - pl.col("median_prix_m2_2021"))
                / pl.col("median_prix_m2_2021")
                * 100
                / YEARS_BETWEEN_2021_2024
            )
        )
        .filter(
            (pl.col("growth_prix_m2") >= MIN_GROWTH_PERCENT)
            & (pl.col("growth_prix_m2") <= MAX_GROWTH_PERCENT)
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


@app.cell(hide_code=True)
def _(dvf_final, mo):
    mo.md(f"""
    **Validation**: Final DVF dataset ready
    - **{dvf_final.shape[0]:,} municipalities** with complete 2021-2024 data
    - Growth rate: mean = {dvf_final["growth_prix_m2"].mean():.2f}%, std = {dvf_final["growth_prix_m2"].std():.2f}%
    - Price range: {dvf_final["median_prix_m2"].min():.0f} € to {dvf_final["median_prix_m2"].max():.0f} € per m²
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 5: Pivot and Calculate Growth

    Transform from **long format** (rows per year) to **wide format** (2021 and 2024 as columns).

    Calculate **annualized growth rate** as percentage per year, then **standardize** (z-score)
    for cross-municipality comparability. Filter out extreme outliers.
    """)
    return


@app.cell
def _(dvf_final):
    dvf_final
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Facilities (BPE) Processing & Filtering

    ### BPE Dataset Overview

    The BPE (Base Permanente des Équipements) is an annual census by INSEE listing all public and private facilities
    across French territory. The dataset contains hundreds of facility types (schools, hospitals, shops, cultural venues, etc.).

    **Focus**: Extract and pivot only the **10 essential services** defined in configuration.

    **Geographic Scope**: Metropolitan France only (codes 97/98 excluded)

    **Aggregation Level**: Municipality (`COM`) and Arrondissement (`ARM`) for major cities

    **Output**: Wide-format table with one column per service type, plus a `Total` column summing all services.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 1: Filter BPE Data

    Load BPE raw data and apply filters to retain only relevant facility types and geographic scope.

    **Filters Applied**:
    - Geographic entities: `COM` (communes) and `ARM` (arrondissements)
    - Facility types: Only the 10 essential services from configuration
    - Geographic scope: Metropolitan France only
    """)
    return


@app.cell
def _(SELECTED_FACILITY_TYPES: list[str], bpe_data_file, pl):
    bpe_raw_facilities = (
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
            ~pl.col("GEO").str.starts_with("97") & ~pl.col("GEO").str.starts_with("98")
        )
        .drop("GEO_OBJECT")
    )
    return (bpe_raw_facilities,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 2: Load Facility Labels

    Extract human-readable labels from BPE metadata file to replace cryptic facility codes.

    **Example**: `D265` → `Médecin généraliste`

    Creates a mapping dictionary used in the pivot operation.
    """)
    return


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 3: Pivot to Wide Format

    **Transformation**: Convert from long format (one row per facility type) to wide format (one column per facility type).

    **Operations**:
    1. Rename `GEO` → `code_commune` for consistency with DVF
    2. Cast facility counts to Int64
    3. Pivot on `FACILITY_TYPE` with sum aggregation (handles duplicate codes)
    4. Fill nulls with 0 (municipality doesn't have this facility)
    5. Rename columns using label mapping
    6. Calculate `Total` = sum of all 10 service counts

    **Why collect here?** Pivot operation requires materialization in Polars.
    """)
    return


@app.cell
def _(bpe_raw_facilities, label_mapping, pl):
    bpe_by_commune = (
        bpe_raw_facilities.collect()
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
    return (bpe_by_commune,)


@app.cell(hide_code=True)
def _(bpe_by_commune, mo):
    mo.md(f"""
    **Validation**: BPE pivoted by municipality
    - **{bpe_by_commune.shape[0]:,} municipalities** in BPE dataset
    - Total services range: {bpe_by_commune["Total"].min()} to {bpe_by_commune["Total"].max()}
    - Median total services: {bpe_by_commune["Total"].median():.0f}
    - Municipalities with 0 services: {(bpe_by_commune["Total"] == 0).sum():,}
    """)
    return


@app.cell
def _(bpe_by_commune):
    bpe_by_commune
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Merging DVF and BPE Data

    ### Integration Strategy

    Combine three datasets to create a comprehensive view at the municipal level:

    1. **DVF** (real estate): Price metrics and market dynamics
    2. **BPE** (facilities): Service availability counts
    3. **Communes** (geography): GPS coordinates for distance calculations

    **Join Strategy**:
    - Start with DVF municipalities (those with transaction data)
    - Left join BPE data, filling missing facilities with 0 (service deserts)
    - Left join GPS coordinates for spatial analysis

    **Distance Calculation**:
    For each of the 10 service types, compute geodesic distance to the nearest municipality offering that service.
    Uses vectorized NumPy operations for performance on ~35,000 municipalities.
    """)
    return


@app.cell
def _(bpe_by_commune, communes_path, label_mapping, pl):
    communes_gps = (
        pl.scan_csv(communes_path, schema_overrides={"code_insee": pl.Utf8})
        .select(["code_insee", "latitude_centre", "longitude_centre"])
        .rename({"code_insee": "code_commune"})
    )

    bpe_with_gps = (
        bpe_by_commune.lazy()
        .join(
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
    return bpe_with_gps, facility_columns


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Prepare BPE for Merging with DVF

    **Normalize** the `Total` facilities count (z-score) for comparability.

    Add all DVF municipalities (even those without facilities, filled with 0).

    Join **geographic coordinates** from the communes dataset to enable distance calculations.
    """)
    return


@app.cell
def _(
    bpe_with_gps,
    calculate_nearest_facility_distance_matrix,
    facility_columns,
):
    bpe_with_distances = bpe_with_gps
    for facility_code in facility_columns:
        print(f"Calculating distance to {facility_code}...")
        bpe_with_distances = calculate_nearest_facility_distance_matrix(
            bpe_with_distances, facility_code
        )

    print("Distance calculations complete!")
    return (bpe_with_distances,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Calculate Distances to Nearest Facility

    For each municipality and each facility type, compute the **geodesic distance** (in km)
    to the nearest municipality that has that facility.

    If the municipality already has the facility, distance = 0.
    """)
    return


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


@app.cell(hide_code=True)
def _(collected_dataset, facility_columns, mo):
    distance_cols = [f"distance_{fc}" for fc in facility_columns]

    null_counts = {
        "Real Estate": collected_dataset.select(["median_prix_m2", "growth_prix_m2"])
        .null_count()
        .to_dicts()[0],
        "Facilities": collected_dataset.select(facility_columns)
        .null_count()
        .sum_horizontal()[0],
        "Distances": collected_dataset.select(distance_cols)
        .null_count()
        .sum_horizontal()[0],
    }

    mo.md(f"""
    ## Final Dataset Validation

    **Shape**: {collected_dataset.shape[0]:,} municipalities × {collected_dataset.shape[1]} columns

    **Null Values Check**:
    - Real estate metrics: {null_counts["Real Estate"]["median_prix_m2"]} nulls in median_prix_m2, {null_counts["Real Estate"]["growth_prix_m2"]} in growth_prix_m2
    - Facilities counts: {null_counts["Facilities"]} total nulls
    - Distance metrics: {null_counts["Distances"]} total nulls

    **Distance Statistics** (municipalities without local facility):
    - Max distance encountered: {collected_dataset.select(distance_cols).max().max_horizontal()[0]:.2f} km
    """)
    return


@app.cell
def _(collected_dataset):
    collected_dataset
    return


if __name__ == "__main__":
    app.run()
