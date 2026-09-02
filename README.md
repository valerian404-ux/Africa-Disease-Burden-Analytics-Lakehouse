# Africa Disease Burden Analytics Lakehouse

An end-to-end Medallion lakehouse (Bronze → Silver → Gold) built on Databricks and PySpark, tracking five major causes of mortality across 54 African countries using WHO Global Health Observatory data feeding a Power BI dashboard.

## Problem Statement

Public health researchers, NGOs, and policymakers need a reliable, unified view of how disease burden has changed over time across African countries. WHO publishes this data, but it's spread across dozens of separate indicator datasets, inconsistently labeled, and mixed in with global (non-African) records which makes it hard to get a clean, continent-specific picture without significant cleaning work.

This project builds a pipeline that ingests, cleans, and validates WHO mortality data for five major indicators (HIV/AIDS, malaria, maternal mortality, TB, and under-5 mortality), producing an analysis-ready dataset and dashboard focused specifically on Africa. Nigeria is used as a spotlight country throughout, but the pipeline itself covers the full continent — 54 countries — which is what makes cross-country and year-over-year comparisons meaningful in the Gold layer.

## Architecture

```
WHO GHO API
      ↓
   Bronze (raw Delta tables, one per indicator)
      ↓
   Silver (cleaned, standardized, Africa-only, partitioned by country + year)
      ↓
   Gold (star schema: fact table + country dimension + YoY trends)
      ↓
   Power BI Dashboard
```

## Tech Stack
- Databricks Community Edition
- PySpark
- Delta Lake (Unity Catalog)
- Power BI Desktop / Power BI Service
- WHO Global Health Observatory (data source)

## Key Design Decisions

**Why Medallion architecture:** Separating raw, cleaned, and analytical layers meant every fix (see the data quality section below) could be traced back to a specific stage — when a data quality issue turned up in Silver, it was possible to go back to Bronze, find the root cause, and re-run just that layer, rather than untangling one giant transformation.

**Partitioning strategy (country + year):** Both Silver and Gold tables are partitioned by `country_code`, then `year`. Since almost every realistic query against this data filters by country (e.g. "show me Nigeria") or by year (e.g. "show me 2020"), this partitioning lets Spark skip reading irrelevant data files entirely rather than scanning the whole table.

**Data quality approach — what was checked and why:**
- **Censored value handling:** WHO sometimes reports values like `<25` instead of an exact number when confidence is low. These rows have a real `Value` string but a null `NumericValue`. They were dropped rather than kept as null or flagged, since they're modeled estimates to begin with and losing a censoring "floor" value has low analytical impact.
- **Custom African ISO3 country list, not WHO's region column:** WHO's own `ParentLocationCode` field misclassifies Egypt as "Eastern Mediterranean" rather than Africa. Filtering with a manually built list of the 54 recognized African ISO3 codes avoids silently losing a real African country.
- **Duplicate detection:** A `groupBy` on (`country_code`, `year`, `indicator`) with a count filter surfaced 81 duplicate rows, all concentrated in the HIV mortality indicator — see the bug write-up below.
- **Category-completeness checks:** two indicators (HIV mortality, under-5 mortality) required inspecting raw Bronze data directly to catch problems a simple null-check wouldn't have found (see below).

**Why Delta Lake time travel was used, and what it demonstrates:** After the Silver table was written, a second version was deliberately created (filtering to 2010+ only), then the original full version was retrieved using `versionAsOf` and restored using `RESTORE TABLE`. This demonstrates a realistic scenario — recovering a prior state of a table after an unwanted change — which is one of Delta Lake's core advantages over plain Parquet/CSV storage.

## Real Bugs Found and Fixed During Validation

This section exists because catching these wasn't part of a tutorial script — they turned up during actual data quality checks, and required tracing the problem back to its root cause rather than just patching the symptom.

**1. Bronze ingestion contamination (HIV mortality table).**
94% of the raw `bronze_who_hiv_mortality` table (7,878 of 8,374 rows) turned out to be maternal mortality data. Root cause: the Bronze ingestion notebook used `.mode("append")`, so an earlier run with a wrong file/indicator mix never got cleared out — every re-run just piled more data on top. Fixed by switching all five Bronze ingestion notebooks to `.mode("overwrite")` and re-ingesting cleanly.

**2. Silent zero-row bug (under-5 mortality).**
The Silver pipeline was filtering under-5 mortality on `Dim1 == 'BTSX'` to keep only "both sexes" rows, based on the pattern used elsewhere in the WHO data — but this table actually uses `'SEX_BTSX'`. The filter matched nothing, silently returning zero rows, with no error thrown. Caught by noticing the indicator was missing entirely from a `groupBy("indicator").count()` sanity check, then traced by inspecting the raw distinct values in `Dim1`.

**3. Hidden wealth-quintile breakdown (under-5 mortality).**
After fixing bug #2, under-5 mortality had ~3x more rows than the country/year grain should allow (11,758 rows against a mathematical maximum of 54 countries × 92 years ≈ 4,968). Inspecting `Dim2`/`Dim3` in the raw Bronze data revealed WHO splits under-5 mortality by wealth quintile (`WQ1`–`WQ5`, plus a `TOTL` combined value) — a breakdown that doesn't exist in the other four indicators. Fixed by filtering to `Dim3 == 'WEALTHQUINTILE_TOTL'` only.

## Data Source

WHO Global Health Observatory (GHO): https://www.who.int/data/gho

**Indicators used:**

| Indicator | WHO Code | Unit |
|---|---|---|
| HIV/AIDS mortality | `WHS2_138` | Number of deaths |
| Malaria mortality | `MALARIA_EST_MORTALITY` | Per 100,000 population |
| Maternal mortality ratio | `MDG_0000000026` | Per 100,000 live births |
| TB mortality (excl. HIV-TB) | `TB_e_mort_exc_tbhiv_num` | Number of deaths |
| Under-5 mortality rate | `MDG_0000000007` | Per 1,000 live births |

**Known limitations of the source data:**
- Indicators are a mix of rates and raw counts (see table above) — not directly comparable to each other without normalization, which was deliberately not attempted since denominators differ across indicators.
- Category labels (e.g. "both sexes") are not consistently spelled across indicator tables (`BTSX` vs `SEX_BTSX`), which caused one of the bugs documented above.
- Some values are censored (`<N` format) rather than exact, particularly in indicators covering smaller or lower-reporting-capacity countries.

## Setup / How to Run

1. Clone this repo and open the `notebooks/` folder in a Databricks workspace with Unity Catalog enabled.
2. Run notebooks in numbered order: `01_bronze_ingestion.py` → `02_silver_cleaning.py` → `03_silver_quality_checks.py` → `04_gold_aggregations.py` → `05_export_for_powerbi.py`.
3. Open `dashboard/disease_burden_dashboard.pbix` in Power BI Desktop (or connect Power BI directly to the Gold schema) to explore the visuals.

## Sample Output / Key Insights

_To be finalized once the Power BI dashboard is complete — early exploration already shows a clear, steady decline in Angola's malaria mortality rate from 2000 to roughly 2014, consistent with known malaria control program improvements in the region over that period._

## Project Board

Full task tracking: [Notion link](https://app.notion.com/p/3ad3f4a222ff81cf8883fec6c7024518)

## What I'd Do Differently / Next Steps

- **Incremental ingestion** was planned as a differentiator but not fully implemented — Bronze currently does a full overwrite on each run rather than only pulling new/changed records. A v2 would add a genuine incremental load pattern (e.g. tracking a last-updated watermark).
- Reusable, parameterized data quality check **functions** (with docstrings) would replace the ad hoc checks written during development, making the validation logic easier to re-run and extend.
- A wider set of WHO indicators could be added now that the pipeline pattern (Bronze → Silver → Gold, with per-indicator quirks handled explicitly) has proven itself on five.
