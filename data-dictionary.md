# Data Dictionary — Disease Burden Lakehouse

Single source of truth for column naming across Bronze → Silver → Gold.

## 1. WHO GHO Indicators in Scope

| Indicator (WHO name) | WHO Indicator Code | Description | Unit |
|---|---|---|---|
| HIV/AIDS mortality | WHS2_138 | Estimated deaths due to HIV/AIDS | Number of deaths |
| Malaria mortality | MALARIA_EST_MORTALITY | Estimated malaria mortality rate | Per 100,000 population |
| Maternal mortality ratio | MDG_0000000026 | Maternal deaths per live births, modeled estimate | Per 100,000 live births |
| TB mortality (excl. HIV-TB) | TB_e_mort_exc_tbhiv_num | Estimated number of TB deaths, HIV-negative | Number of deaths |
| Under-5 mortality rate | MDG_0000000007 | Probability of dying by age 5 | Per 1,000 live births |

> Note: TB and HIV mortality are raw death counts, while malaria, maternal, and under-5 mortality are rates. These are not directly comparable on the same chart scale without care.

## 2. Bronze Layer Schema

Raw, as-ingested from the WHO GHO OData API — column names kept exactly as WHO provides them, plus two ingestion-tracking columns added during load.

| Column Name | Data Type | Notes |
|---|---|---|
| Id | long | WHO's internal record ID |
| IndicatorCode | string | WHO's code for this indicator (should match one table = one code; see quirks log) |
| SpatialDim | string | Country/region code (ISO3 for countries) |
| SpatialDimType | string | `COUNTRY`, `REGION`, `GLOBAL`, etc. |
| TimeDim | int | Year |
| TimeDimType | string | Usually `YEAR` |
| TimeDimensionBegin / End / Value | string / string / string | Redundant with `TimeDim` for annual data |
| Dim1 / Dim1Type | string / string | Category breakdown 1 (e.g. sex) — spelling varies by indicator, see quirks log |
| Dim2 / Dim2Type | string / string | Category breakdown 2 (e.g. age group) — not empty for all indicators, see quirks log |
| Dim3 / Dim3Type | string / string | Category breakdown 3 (e.g. wealth quintile) — not empty for all indicators, see quirks log |
| Value | string | Display-formatted value, sometimes censored (`<25` style) |
| NumericValue | double | The real numeric value; null when censored |
| Low / High | double / double | Confidence interval bounds — out of scope for this project |
| ParentLocation / ParentLocationCode | string / string | WHO's region grouping — unreliable for Africa filtering, see quirks log |
| Date | timestamp | WHO's record date |
| Comments | string | Free text, mostly null |
| DataSourceDim / DataSourceDimType | string / string | Provenance metadata, not used |
| _injested_at | timestamp | Added during ingestion — when this row was loaded into Bronze |
| _source_file | string | Added during ingestion — which raw JSON file this row came from |

## 3. Silver Layer Schema

Cleaned, standardized, typed. One row = one country + one year + one indicator.

| Column Name | Data Type | Transformation Applied | Notes |
|---|---|---|---|
| country_code | string | From `SpatialDim`, filtered to `SpatialDimType == 'COUNTRY'`, then to the 54-country African ISO3 list | Custom list used instead of `ParentLocationCode` — see quirks log |
| year | int | From `TimeDim` | |
| indicator | string | Derived via table-name → label mapping (`INDICATOR_MAP`), not a raw WHO column | |
| value | double | From `NumericValue`; rows with null `NumericValue` (censored values) dropped | |

**Additional filters applied per indicator during Silver cleaning:**
- Under-5 mortality: `Dim1 == 'SEX_BTSX'` (both sexes) and `Dim3 == 'WEALTHQUINTILE_TOTL'` (combined wealth quintile) — see quirks log for why both were needed
- All indicators: `SpatialDimType == 'COUNTRY'`, then Africa ISO3 filter

**Confirmed row count after all Silver cleaning: 8,320 rows across 5 indicators, 54 countries.**

## 4. Gold Layer Schema

### Table: `gold_disease_burden_by_country`
Main fact table. Same grain as Silver, with rounded values for display.

| Column Name | Data Type | Description |
|---|---|---|
| country_code | string | ISO3 country code |
| year | int | Year of the record |
| indicator | string | Which of the 5 indicators this row is for |
| value | double | Rounded to 2 decimal places |

Partitioned by `country_code`, `year`.

### Table: `gold_yoy_trends`
Year-over-year change per country + indicator, built with a window function over `gold_disease_burden_by_country`.

| Column Name | Data Type | Description |
|---|---|---|
| country_code | string | ISO3 country code |
| year | int | Year of the record |
| indicator | string | Which of the 5 indicators this row is for |
| value | double | This year's value |
| previous_year_value | double | Prior year's value for the same country + indicator; null for each series' first year |
| yoy_change | double | `value - previous_year_value`, rounded |
| yoy_pct_change | double | Percent change vs. prior year, rounded |

Partitioned by `country_code`, `year`.

### Table: `gold_country_dim`
Dimension table mapping country codes to display names.

| Column Name | Data Type | Description |
|---|---|---|
| country_code | string | ISO3 country code |
| country_name | string | Full country name |

54 rows, one per African country. No partitioning (small lookup table).

## 5. Naming Convention Rules (see also conventions.md)
- snake_case for all column names
- No source system prefixes carried into Silver/Gold (lesson learned from Bike Lakehouse feedback)
- Country identifiers standardized to ISO3 by Silver layer, never left as free-text country names
- Boolean/flag columns prefixed `is_` or `has_`

## 6. Known Data Quirks Log

| Date Found | Issue | Affected Countries/Years | Resolution |
|---|---|---|---|
| 2026-08 | WHO's `ParentLocationCode` region field misclassifies Egypt under "Eastern Mediterranean" (EMR) rather than Africa | Egypt specifically | Built a custom 54-country African ISO3 list instead of trusting WHO's region column |
| 2026-08 | `bronze_who_hiv_mortality` contained 7,878 rows (94% of the table) of mislabeled maternal mortality data, caused by `.mode("append")` in Bronze ingestion never clearing prior/wrong runs | HIV mortality table, all countries/years until fixed | Switched all Bronze ingestion notebooks to `.mode("overwrite")`; re-ingested |
| 2026-08 | 81 exact-duplicate rows in Silver, all in HIV mortality, traced to the same append-mode ingestion bug above | HIV mortality, concentrated around 2001 and 2012 | Resolved automatically once Bronze was re-ingested with `overwrite` mode |
| 2026-08 | Under-5 mortality's "both sexes" category is spelled `SEX_BTSX` in this table, not `BTSX` as used elsewhere — caused the Silver filter to silently match zero rows | Under-5 mortality, all countries/years | Corrected the filter value to `SEX_BTSX` |
| 2026-08 | Under-5 mortality also splits by wealth quintile (`Dim3`: `WEALTHQUINTILE_WQ1`–`WQ5`, plus `TOTL`) — a breakdown not present in the other four indicators — inflating row count ~3x | Under-5 mortality, all countries/years | Added `Dim3 == 'WEALTHQUINTILE_TOTL'` filter to keep only the combined value |
