# Naming & Style Conventions — Disease Burden Lakehouse

## Column Naming
- `snake_case` throughout, no camelCase or PascalCase
- No abbreviations unless universally clear (`yr` → use `year`; `cty` → use `country_code`)
- No source-system prefixes retained past Bronze (e.g. don't carry `who_` prefixes into Silver/Gold — this was flagged in Bike Lakehouse feedback)
- Boolean/flag columns prefixed `is_` or `has_`
- Foreign-key-style reference columns suffixed `_code` or `_id` (e.g. `country_code`)
- Date/year columns: `year` (int) separate from any full `date` (date type) if both exist

## Table Naming
- Bronze: `bronze_<source>_<entity>` e.g. `bronze_who_hiv_mortality`
- Silver: `silver_<entity>` e.g. `silver_disease_burden`
- Gold: `gold_<purpose>` e.g. `gold_disease_burden_by_country`, `gold_yoy_trends`, `gold_country_dim`

## Notebook Naming
- `01_bronze_ingestion.py`
- `02_silver_cleaning.py`
- `03_silver_quality_checks.py`
- `04_gold_aggregations.py`
- `05_export_for_powerbi.py`
- Numbered prefixes so notebook execution order is self-evident to anyone browsing the repo

## Partitioning
- Silver and Gold tables partitioned by `country_code`, `year` (in that order) unless a specific table's grain doesn't require it — document any exception inline in the notebook
- `gold_country_dim` is an exception: it's a small dimension table (54 rows) with no meaningful partitioning need

## PySpark Style
- Use `.where()` instead of `.filter()` throughout — functionally identical, chosen as the project's standard for consistency and SQL-like readability
- Always use `mode("overwrite")` for Bronze/Silver/Gold writes unless a table is explicitly designed for incremental append — a `mode("append")` bug during Bronze ingestion caused real data contamination early in this project (see README) and is treated as a cautionary default from that point forward
- Prefer PySpark DataFrame API over RDD API (Databricks serverless compute limitations, cleaner readability)
- Avoid magic numbers/strings — define constants at the top of each notebook (e.g. `AFRICAN_ISO3_CODES`, `INDICATOR_MAP`)

## Git / GitHub
- Branch naming: `feature/<short-description>` e.g. `feature/silver-schema-design`
- Commit messages: imperative mood, present tense — "Add Bronze ingestion notebook", not "Added" or "Adding"
- One logical change per commit where practical; avoid single giant commits covering multiple phases
- Tag major milestones (`v0.1-bronze-complete`, `v0.2-silver-complete`, etc.)

## Documentation
- Every notebook starts with a markdown cell: purpose, inputs, outputs, and any assumptions made
- Data quality check functions include a docstring explaining what they check and why that check matters for this dataset specifically (not generic boilerplate)
- Any data quirk discovered during development (mislabeled categories, contamination, inconsistent spellings) gets logged in `data-dictionary.md`'s quirks table — not just fixed silently

## Code Style
- Follow PEP 8 for any raw Python
- Prefer PySpark DataFrame API over RDD API (documented reasoning: Databricks serverless compute limitations, cleaner readability)
- Avoid magic numbers/strings — define constants at the top of each notebook (e.g. list of valid country codes, indicator names)
