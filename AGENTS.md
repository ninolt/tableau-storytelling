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
