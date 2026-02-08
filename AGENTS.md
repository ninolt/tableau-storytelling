# AGENTS.md

This document defines the constraints and standards for any AI agent working on the **Tableau Storytelling** project. Adherence to these rules is mandatory.

## Project Vision
A robust, "one-shot" but efficient data pipeline to collect, process, and join public datasets for Tableau (Salesforce) public projects.

## Technical Stack
- **Dependency Manager**: `uv`
- **Data Processing**: `Polars` (Primary), `NumPy`
- **Workflow / Notebooks**: `Marimo`
- **Output Format**: `Parquet` (Optimized for Tableau 2022.1+)
- **Linting/Formatting**: `Ruff`

## Core Development Rules

### 1. Linguistic Consistency
- **Language**: ALL code, function names, variable names, file names, and docstrings MUST be in **English**.
- **Communication**: While user prompts and chat may be in French, the codebase must remain strictly English.

### 2. Code Quality and Robustness
- **Typing**: Use strong Python typing (type hints) for all functions and complex variables to ensure robustness.
- **Naming**: Use highly explicit names for functions and variables. Prioritize "self-documenting code" over external comments.
- **Comments**: Minimize inline comments. Business logic and pipeline explanations should reside in Marimo's markdown cells.

### 3. Performance and Large Data Handling
- **Batching**: The datasets involve gigabytes of data. **Streaming and batch processing are CRITICAL.**
- **Polars**: Use Polars' lazy evaluation (`lazy()`) and streaming capabilities whenever possible to avoid memory overflows.
- **Iterative Control**: Use Marimo to create checkpoints. Display verification metrics (e.g., sum checks, distribution stats, or quick plots) to ensure data integrity at each step.

### 4. Mandatory Verification Workflow
Before marking a task as complete, the agent MUST run and pass the following commands:
1. `uv run marimo check notebooks/*.py`
2. `uv run ruff check .`
3. `uv run ruff format --check .`

## Prohibited
- **No Emojis**: Do not use emojis in code, commits, or documentation (unless explicitly requested).
- **No Placeholders**: Avoid dummy data or "TODO" items in final implementations.
- **No Direct pyproject.toml Edits**: Always use `uv add` or `uv remove` to manage dependencies.

## Learning & Knowledge Transfer

### Continuous Learning Approach
The developer is in their final year of an Engineering School specializing in Data Engineering and AI. They have solid foundational knowledge of data science concepts and Python. The focus is on **learning framework-specific syntax and patterns**, particularly Polars and Marimo.

**When to Explain (Framework Syntax & Patterns):**
- **Polars** operations (transformations, lazy evaluation, pivots, joins, aggregations)
- **Marimo** features (cells, reactive execution, markdown integration)
- Any framework-specific API calls, method chaining patterns, or idiomatic usage

**When NOT to Explain:**
- Basic Python syntax and control flow
- Data science fundamentals (statistical concepts, ML theory, etc.)
- Simple arithmetic or logic operations
- Standard library functions

### Explanation Style
- Keep explanations **focused on "how to do it in [Framework]"** and **why this approach is idiomatic**
- Use **concrete examples** from the codebase
- Break down complex method chains step-by-step
- Link framework patterns back to familiar Python concepts when relevant (e.g., "like pandas but faster")

## Project Structure

```text
/tableau-storytelling
├── AGENTS.md
├── data/
│   ├── bpe/
│   │   ├── DS_BPE_2024_data.csv
	│   └── DS_BPE_2024_metadata.csv
│   ├── dvf.csv
│   └── dvf.csv.gz
├── notebooks/
│   └── pipeline.py
├── pyproject.toml
└── uv.lock
```

## Dataset Previews

### Land Value Data (DVF)
Full column list and first row:
```text
id_mutation,date_mutation,numero_disposition,nature_mutation,valeur_fonciere,adresse_numero,adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,code_commune,nom_commune,code_departement,ancien_code_commune,ancien_nom_commune,id_parcelle,ancien_id_parcelle,numero_volume,lot1_numero,lot1_surface_carrez,lot2_numero,lot2_surface_carrez,lot3_numero,lot3_surface_carrez,lot4_numero,lot4_surface_carrez,lot5_numero,lot5_surface_carrez,nombre_lots,code_type_local,type_local,surface_reelle_bati,nombre_pieces_principales,code_nature_culture,nature_culture,code_nature_culture_speciale,nature_culture_speciale,surface_terrain,longitude,latitude
2020-1,2020-07-01,000001,Vente,31234.16,,,SAINT JULIEN,B064,01560,01367,Saint-Julien-sur-Reyssouze,01,,,013670000A0008,,,,,,,,,,,,,0,,,,,AB,terrains a bâtir,,,1192,5.109255,46.403019
```

### Facilities Data (BPE)
First rows of the main dataset (semicolon-separated, quoted values as in current CSV):
```text
"GEO";"GEO_OBJECT";"FACILITY_DOM";"FACILITY_SDOM";"FACILITY_TYPE";"BPE_MEASURE";"TIME_PERIOD";"OBS_VALUE"
"67";"DEP";"F";"F1";"F105";"FACILITIES";2024;3
"86";"DEP";"D";"D6";"D606";"FACILITIES";2024;3
"26";"DEP";"D";"D6";"D607";"FACILITIES";2024;4
"73";"DEP";"F";"F1";"F107";"FACILITIES";2024;33
"90";"DEP";"F";"F1";"F101";"FACILITIES";2024;5
```

### Geographical data
First row of the geographical dataset:
```
,code_insee,nom_standard,nom_sans_pronom,nom_a,nom_de,nom_sans_accent,nom_standard_majuscule,typecom,typecom_texte,reg_code,reg_nom,dep_code,dep_nom,canton_code,canton_nom,epci_code,epci_nom,academie_code,academie_nom,code_postal,codes_postaux,zone_emploi,code_insee_centre_zone_emploi,population,superficie_hectare,superficie_km2,densite,altitude_moyenne,altitude_minimale,altitude_maximale,latitude_mairie,longitude_mairie,latitude_centre,longitude_centre,grille_densite,gentile,url_wikipedia,url_villedereve
0,01001,L'Abergement-Clémenciat,Abergement-Clémenciat,à Abergement-Clémenciat,de l'Abergement-Clémenciat,l-abergement-clemenciat,L'ABERGEMENT-CLÉMENCIAT,COM,commune,84,Auvergne-Rhône-Alpes,01,Ain,0108,Châtillon-sur-Chalaronne,200069193,CC de la Dombes,10,Lyon,01400,01400,08405,01053,806,1565,16,50.53,242,206.0,272.0,46.153,4.926,46.153,4.926,Rural à habitat dispersé,,https://fr.wikipedia.org/wiki/fr:L'Abergement-Clémenciat,https://villedereve.fr/ville/01001-l-abergement-clemenciat
```