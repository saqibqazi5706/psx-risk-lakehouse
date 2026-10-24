# PSX Risk Lakehouse

An end-to-end data pipeline that ingests daily trading data from the **Pakistan Stock Exchange (PSX)**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold) with **PySpark and Delta Lake on Databricks**, and delivers volatility-based risk analytics through a Power BI dashboard.

> **Status:** Phase 3 complete — Gold star schema (2 dimensions, base + aggregated fact), end-to-end pipeline Raw → Bronze → Silver → Gold with logging at every layer, and a 3-page Power BI dashboard.

---

## Overview

Stock prices are hard to predict, but *risk* is not random: turbulent periods cluster together, and calm periods do too. This project turns raw PSX market data into a clean, analysis-ready view of **which stocks and sectors are in high-risk regimes, whether turbulence is market-wide or company-specific, and how volatility responds to monetary policy decisions.**

### Business questions the dashboard answers

1. Which stocks and sectors are currently in a high-volatility regime?
2. Is observed turbulence **systemic** (market-wide) or **idiosyncratic** (company-specific)?
3. How does market volatility behave around State Bank of Pakistan policy rate decisions?

---

## Architecture

```
 Source                   Bronze                        Silver                          Gold
┌────────────────┐   ┌──────────────────────────┐   ┌───────────────────────────┐   ┌──────────────────┐
│ PSX daily       │──▶│ bronze.mkt_summary        │──▶│ silver.daily_prices        │──▶│ star schema:     │
│ Market Summary  │   │ raw strings, 1 row/line,  │   │ typed, deduplicated,       │   │ dim_date,        │
│ (.Z per day)    │   │ + file date, source,      │   │ validated, MERGE INTO on   │   │ dim_stock, facts │
└────────────────┘   │   batch_id, load_timestamp│   │ (symbol, trade_date)       │   └────────┬─────────┘
                                                                                                      ▼
                                                                                              Power BI (SQL Warehouse)
                     └────────────┬─────────────┘   └─────────────┬─────────────┘
                                  │ bad lines                      │ cast failures
                                  ▼                                ▼
                     ┌─────────────────────────────────────────────────────────┐
                     │ ops.quarantine_mkt_summary   (layer = bronze | silver)   │
                     └─────────────────────────────────────────────────────────┘
                     ┌─────────────────────────────────────────────────────────┐
                     │ ops.pipeline_execution_logs  (one row per file / run)    │
                     └─────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Key processing |
|-------|---------|----------------|
| **Bronze** | Raw landing zone, faithful copy of the source | Explicit `StructType` (all strings, no `inferSchema`); lineage columns; one trading day is reloaded atomically with Delta `replaceWhere`; malformed lines quarantined |
| **Silver** | Cleansed, conformed data | Type casting (DATE, DOUBLE, BIGINT); futures removed; de-duplication; validity flags; cast failures quarantined; `MERGE INTO` upsert |
| **Gold** | Star schema for analysis and BI | Log returns on PSX's split-adjusted LDCP, 20/60-day annualised volatility, daily risk percentile and tier, market-level aggregate, SBP policy rate per day; facts upserted with `MERGE INTO`, liquid clustering |

---

## Data source

| Source | Content | Used in |
|--------|---------|---------|
| PSX Daily Downloads — Market Summary (Closing): `https://dps.psx.com.pk/download/mkt_summary/{YYYY-MM-DD}.Z` | End-of-day OHLCV for every traded security, one file per trading day, 2015 → present | Bronze, Silver |
| [SBP — Structure of Interest Rates](https://www.sbp.org.pk/assets/document/sir.pdf) | Policy rate decisions and effective dates | Gold (`ref_policy_rate` → `dim_date`) |
| PSX Data Portal sector summary | Official sector names for codes 0801–0839 | Gold (`ref_sector` → `dim_stock`) |

**File format.** Despite the `.Z` extension, files are GZIP (older years) or ZIP (newer years). Content is pipe-delimited, no header, Latin-1, 10 data fields followed by 3 empty fields, so a valid line has exactly **12 pipes**:

```
02JAN2015|HBL|0807|Habib Bank Ltd|190.5|193|189|192.1|350000|191|||
 date      sym sector name          open  high low close volume ldcp
```

### Extraction constraint

Databricks Free Edition has no outbound internet access to PSX, and PSX returns `403` to repeated automated requests from residential IPs. Files are therefore downloaded once with a rate-limited script on Google Colab (`download_mkt_summary.py`, one request per minute, identifying User-Agent), stored on Google Drive, and uploaded to the Unity Catalog volume `/Volumes/workspace/bronze/raw_landing/mkt_summary/{year}/{date}.Z`. The pipeline reads only from that volume. Weekends and market holidays have no file (PSX returns `404`), which the pipeline treats as normal.

### New, updated and deleted records

- **New:** each trading day adds one file and one row per traded symbol. Incremental Silver runs pick up only dates newer than the last loaded date.
- **Updated:** if PSX re-publishes a day, reloading that file replaces the day in Bronze, and Silver's `MERGE` updates only rows whose values changed (detected by a row hash), setting `updated_timestamp`.
- **Deleted / delisted:** a delisted company simply stops appearing in new files. Its history stays in Silver; Gold marks it `is_active = FALSE` in `dim_stock`.

---

## Data models

All tables are Delta tables in the Unity Catalog `workspace` catalog. All timestamps are **UTC** (Databricks default).

### `bronze.mkt_summary` — raw, one row per line of a source file

**Logical key:** `(file_trade_date, symbol)`

| Column | Type | Description |
|---|---|---|
| trade_date | STRING | Date as written in the file, e.g. `02JAN2015` |
| symbol | STRING | Ticker, e.g. `HBL` (futures contain `-`, e.g. `HBL-JAN`) |
| sector_code | STRING | PSX sector code |
| company_name | STRING | Company name |
| open_raw, high_raw, low_raw, close_raw | STRING | Prices as text |
| volume_raw | STRING | Shares traded as text |
| ldcp_raw | STRING | Last day close price as text |
| extra_1, extra_2, extra_3 | STRING | The three trailing empty fields |
| file_trade_date | DATE | Trading date taken from the file name |
| source_file | STRING | Full volume path of the source file |
| batch_id | STRING | Run identifier, `bronze_YYYY-MM-DD_HH-MM-SS` |
| load_timestamp | TIMESTAMP | When the row was written |

Everything is stored as STRING on purpose: Bronze never rejects a value because of its type. Typing happens in Silver.

### `silver.daily_prices` — typed, one row per stock per trading day

**Primary key:** `(symbol, trade_date)` — enforced by `MERGE` and `NOT NULL` constraints.

| Column | Type | Description |
|---|---|---|
| symbol | STRING NOT NULL | Trimmed ticker (futures excluded) |
| trade_date | DATE NOT NULL | Parsed from `ddMMMyyyy`; must equal the file date |
| sector_code | STRING | PSX sector code |
| company_name | STRING | Company name |
| open, high, low, close | DOUBLE | Prices in PKR (`open` may be NULL when PSX omits it) |
| volume | BIGINT | Shares traded |
| ldcp | DOUBLE | Last day close price |
| is_traded | BOOLEAN | `volume > 0` |
| is_ohlc_valid | BOOLEAN | `low > 0` and open/close within [low, high]; NULL if a price is missing |
| row_hash | STRING | SHA-256 of the business columns, used to detect real changes |
| source_file | STRING | Lineage back to the raw file |
| bronze_batch_id | STRING | Bronze run that produced the row |
| batch_id | STRING | Silver run that last inserted or updated the row |
| load_timestamp | TIMESTAMP | When the row was first inserted (never changed by updates) |
| updated_timestamp | TIMESTAMP | When the row was last updated; NULL if never |

### Gold layer — star schema

```
            dim_date (+ SBP policy rate)
             /                  \
fact_daily_stock (base)    fact_market_daily (aggregated from base)
            |
        dim_stock (+ sector name, security type)
```

Scope: listed equities and funds only. Debt instruments (sector `36`, `3610`, NULL), preference shares (`…PS`) and futures (`…-`) are excluded.

#### `gold.dim_date` — one row per calendar day, 2015–2027 (overwrite each run)

**Primary key:** `date_key`

| Column | Type | Description |
|---|---|---|
| date_key | INT | `yyyyMMdd`, e.g. 20250428 |
| full_date | DATE | Calendar date |
| year, quarter, month, week_of_year | INT | Calendar parts |
| month_name, year_month, day_name | STRING | Labels for charts |
| is_weekend | BOOLEAN | Saturday or Sunday |
| is_trading_day | BOOLEAN | PSX published data that day |
| policy_rate_pct | DOUBLE | SBP policy rate in force (carried forward from the last decision) |
| rate_change_bps | INT | Change announced that day (0 otherwise) |
| is_policy_decision_day | BOOLEAN | SBP decision took effect that day |
| load_timestamp | TIMESTAMP | When the row was written |

#### `gold.dim_stock` — one row per listed security (overwrite each run)

**Primary key:** `stock_key`

| Column | Type | Description |
|---|---|---|
| stock_key | BIGINT | `xxhash64(symbol)` — deterministic surrogate key |
| symbol | STRING | Ticker (natural key) |
| company_name | STRING | Latest name, `XD/XB` suffixes removed |
| sector_code | STRING | Latest PSX sector code |
| sector_name | STRING | From `ref_sector`; old pre-June-2015 codes → "Legacy sector" |
| security_type | STRING | Equity / Closed-End Fund / ETF / REIT / Legacy |
| first_trade_date, last_trade_date | DATE | First and last appearance |
| trading_days | INT | Days the symbol appeared |
| is_active | BOOLEAN | Seen in the last 30 days of data |
| load_timestamp | TIMESTAMP | When the row was written |

#### `gold.fact_daily_stock` — base fact, one stock on one trading day (MERGE)

**Primary key:** `(date_key, stock_key)`  ·  **Foreign keys:** `date_key` → `dim_date`, `stock_key` → `dim_stock`  ·  `CLUSTER BY (date_key, stock_key)`

| Column | Type | Description |
|---|---|---|
| date_key | INT | FK → dim_date |
| stock_key | BIGINT | FK → dim_stock |
| open, high, low, close, ldcp | DOUBLE | Prices (PKR) |
| volume | BIGINT | Shares traded |
| daily_return | DOUBLE | `ln(close / ldcp)` |
| is_extreme_move | BOOLEAN | \|return\| > 15%; excluded from volatility |
| vol_20d, vol_60d | DOUBLE | Std. dev. of last 20 / 60 valid returns × √252 (NULL if < 15 / 45 returns) |
| avg_volume_20d | DOUBLE | 20-day average volume (liquidity) |
| risk_percentile | DOUBLE | Rank of vol_20d among rated stocks that day (0–1) |
| risk_tier | STRING | High (≥ 0.8) / Medium (≥ 0.4) / Low / Not Rated |
| is_traded, is_ohlc_valid | BOOLEAN | Quality flags from Silver |
| load_timestamp | TIMESTAMP | When the row was written |

#### `gold.fact_market_daily` — aggregated fact, whole market on one trading day (MERGE)

**Primary key / foreign key:** `date_key` → `dim_date`  ·  Built from `fact_daily_stock`  ·  `CLUSTER BY (date_key)`

| Column | Type | Description |
|---|---|---|
| date_key | INT | FK → dim_date |
| stocks_traded, advancers, decliners | INT | Market breadth |
| total_volume | BIGINT | Shares traded in the market |
| market_return | DOUBLE | Equal-weighted average daily return |
| market_vol_20d | DOUBLE | 20-day volatility of the market return × √252 (systemic risk) |
| median_stock_vol_20d | DOUBLE | Typical stock's volatility |
| pct_high_risk | DOUBLE | Share of rated stocks in the High tier |
| load_timestamp | TIMESTAMP | When the row was written |

#### Reference tables (built from code in `03_gold/00_reference_data`)

| Table | Content |
|---|---|
| `gold.ref_sector` | 38 PSX sector codes → official name and security type |
| `gold.ref_policy_rate` | 40 SBP policy rate decisions (effective date, rate, change in bps, direction) |

### `ops.pipeline_execution_logs` — one row per Bronze file, Silver run or Gold step

| Column | Type | Description |
|---|---|---|
| batch_id | STRING | Run identifier |
| layer | STRING | `raw_to_bronze`, `bronze_to_silver` or `silver_to_gold` |
| load_type | STRING | `full`, `incremental` or `backfill` |
| parameter | STRING | Source file path (Bronze), date range (Silver) or Gold table + range |
| start_time, end_time | TIMESTAMP | Run window |
| duration_seconds | DOUBLE | `end_time - start_time` |
| status | STRING | `SUCCESS` or `FAILURE` |
| rows_inserted, rows_updated, rows_quarantined | BIGINT | Row counts |
| error_message | STRING | First 1,000 characters of the error on failure |
| load_timestamp | TIMESTAMP | When the log row was written |

### `ops.quarantine_mkt_summary` — rejected records from either layer

| Column | Type | Description |
|---|---|---|
| raw_record | STRING | The original line, pipes preserved |
| reason | STRING | Why it was rejected |
| layer | STRING | `bronze` or `silver` |
| source_file | STRING | Source file |
| file_trade_date | DATE | Trading date of the file |
| batch_id | STRING | Run that rejected it |
| load_timestamp | TIMESTAMP | When it was quarantined |

---

## Pipeline behaviour

### Idempotency

- **Bronze:** each file is written with `mode("overwrite").option("replaceWhere", "file_trade_date = '<date>'")`. Reloading a day replaces exactly that day, never duplicates it.
- **Silver:** rows are upserted with `MERGE INTO ... ON symbol AND trade_date`. Matched rows are updated **only if `row_hash` differs**, so rerunning the same range reports `0 inserted, 0 updated`.
- **Gold:** dimensions are rebuilt with overwrite; facts are upserted with `MERGE INTO` on their grain. Re-running a backfill inserts 0 rows and only refreshes values.
- **Quarantine:** before writing, each run deletes its own earlier quarantine rows for the same dates and layer, so reruns do not pile up duplicates.

### Schema drift

| Kind of drift | Caught in | Handling |
|---|---|---|
| **Structural** — extra or missing columns (pipe count ≠ 12) | Bronze | Line goes to quarantine with `reason`; the rest of the file still loads |
| **Type** — text where a number or date should be (e.g. `close = 'abc'`) | Silver | `try_cast` / `try_to_timestamp` return NULL instead of failing; the row goes to quarantine with `reason`; the existing good Silver row is left untouched |
| **Empty values** — PSX sometimes leaves `open` blank | Silver | Allowed; stored as NULL and `is_ohlc_valid` is NULL |

Quarantined rows can be reviewed with:

```sql
SELECT layer, reason, COUNT(*) FROM workspace.ops.quarantine_mkt_summary GROUP BY layer, reason;
```

Once the cause is fixed, rerunning the affected dates clears and re-evaluates them.

### Failure handling

Every file (Bronze) and every run (Silver) is wrapped in `try/except`. A failure writes a `FAILURE` log row with the error message and the run continues with the next file.

---

## Repository structure

```
psx-risk-lakehouse/
├── README.md
├── docs/                          # Phase 1 proposal
├── samples/                       # Small sample payloads (full data is not committed)
├── scripts/
│   └── download_mkt_summary.py    # Rate-limited Colab downloader
└── notebooks/
    ├── 00_setup                   # Creates schemas and tables (run once)
    ├── 00_utils                   # Imports, StructType schemas, write_log()
    ├── 01_bronze/
    │   ├── bronze_functions       # load_to_bronze(), load_range()
    │   ├── 01_raw_to_bronze       # Runner with widgets
    │   └── 02_bronze_proofs       # Schema, rerun, drift, failure proofs (saved outputs)
    ├── 02_silver/
    │   ├── silver_functions       # build_silver(), load_silver()
    │   ├── 01_bronze_to_silver    # Runner with widgets
    │   └── 02_silver_proofs       # Incremental, rerun, update, type-drift proofs (saved outputs)
    ├── 03_gold/
    │   ├── 00_reference_data      # ref_sector, ref_policy_rate
    │   ├── gold_functions         # build_dim_date/stock, build_fact_daily_stock/market_daily, load_gold()
    │   └── 01_silver_to_gold      # Runner with widgets + OPTIMIZE + checks
    └── 04_pipeline/
        └── run_pipeline           # End-to-end Raw → Bronze → Silver → Gold
dashboard/
└── PSX_Risk_Dashboard.pbit        # Power BI template (full .pbix submitted separately)
docs/
└── PSX_Phase3_Final_Report.pdf    # Architecture, decision log, DAG analysis, FinOps
```

---

## Run guide

### One-time setup

1. Clone this repo into Databricks as a Git folder.
2. Run `notebooks/00_setup` to create the `bronze`, `silver`, `gold`, `ops` schemas, the volume and the Bronze/Silver/ops tables.
3. Run `notebooks/03_gold/00_reference_data` once to create `ref_sector` and `ref_policy_rate` (Gold fact tables are created on the first Gold run).
4. Upload raw files to `/Volumes/workspace/bronze/raw_landing/mkt_summary/{year}/{YYYY-MM-DD}.Z`.

### Recommended: one end-to-end run

`notebooks/04_pipeline/run_pipeline` takes three widgets (`start_date`, `end_date`, `load_type`) and runs Bronze → Silver → Gold in order, logging every step.

| Scenario | start_date | end_date | load_type |
|---|---|---|---|
| Daily incremental (new files uploaded) | first new day | last new day | `incremental` — Silver and Gold work out their own range |
| Backfill an old period | e.g. `2020-03-02` | `2020-03-06` | `backfill` |
| Full history | `2015-01-01` | latest day | `full` |

### Layer-by-layer runners

Each layer also has its own runner (`01_raw_to_bronze`, `01_bronze_to_silver`, `01_silver_to_gold`) with the same three widgets.

| load_type | Bronze | Silver | Gold | Use it for |
|---|---|---|---|---|
| `full` | Loads every weekday file in the range | Processes the range | Rebuilds dims, MERGEs facts for the range | First load of the whole history |
| `incremental` | Loads the given new days | From last Silver date + 1 to newest Bronze date | From last Gold date + 1 to newest Silver date | Daily runs |
| `backfill` | Reloads the range, replacing those days | Reprocesses the range | Recomputes the range (with lookback windows) | Re-running a past period |

### Examples

**Initial full load**
```
01_raw_to_bronze     start_date=2015-01-01  end_date=2026-10-02  load_type=full
01_bronze_to_silver  start_date=2015-01-01  end_date=2026-10-02  load_type=full
```

**Daily incremental** (after uploading the new day's file)
```
01_raw_to_bronze     start_date=<new day>   end_date=<new day>   load_type=incremental
01_bronze_to_silver  load_type=incremental   (dates are worked out automatically)
```

**Backfill one month**
```
01_raw_to_bronze     start_date=2020-03-01  end_date=2020-03-31  load_type=backfill
01_bronze_to_silver  start_date=2020-03-01  end_date=2020-03-31  load_type=backfill
```

### Checking a run

```sql
SELECT batch_id, layer, load_type, parameter, status,
       rows_inserted, rows_updated, rows_quarantined, error_message
FROM workspace.ops.pipeline_execution_logs
ORDER BY start_time DESC;
```

---

## Dashboard

Power BI Desktop connects to the Databricks SQL Warehouse (Get Data → Databricks, server hostname + HTTP path, personal access token; Import mode). The model mirrors the star schema: `fact_daily_stock` and `fact_market_daily` → `dim_date` (many-to-one), `fact_daily_stock` → `dim_stock` (many-to-one); the ops tables are standalone audit tables.

| Page | Answers | Visuals |
|---|---|---|
| Risk Overview | Q1 — which stocks/sectors are high-risk | KPI cards, volatility by sector, top-10 volatile stocks, year slicer |
| Market Risk & Policy | Q2 systemic vs idiosyncratic, Q3 policy impact | Market volatility over time, SBP policy rate over time, market vs typical-stock volatility by year |
| Pipeline Audit | Operations | Runs, failed runs, average duration, runs by layer/status, latest runs from the log |

The template is in `dashboard/PSX_Risk_Dashboard.pbit` (opening it reconnects to Databricks and loads the data). The full `.pbix` with data is submitted separately because it exceeds GitHub's 25 MB web-upload limit.

---

## Proof of execution

The proof notebooks below were run in Databricks and their outputs are saved in the workspace. (Databricks Git folders do not commit notebook outputs, so the code is in this repository and the results are shown in the workspace and the submission.)

| Requirement | Where |
|---|---|
| Explicit schemas | `02_bronze_proofs` and `02_silver_proofs`: `DESCRIBE TABLE` |
| Rerun creates no duplicates | Bronze: before/after counts + asserts; Silver: second run reports `0 inserted, 0 updated`; duplicate-key query returns 0 rows |
| Backfill | Bronze and Silver backfill runs over a fixed date range |
| Incremental | Silver incremental run picks its own range and inserts only the new day |
| MERGE update path | A changed Bronze value updates one Silver row (`updated_timestamp` set, `load_timestamp` unchanged) |
| Schema drift | Bronze: extra-column lines quarantined; Silver: `close = 'abc'` quarantined, good row untouched |
| Failure logging | Corrupt file produces a `FAILURE` log row with the error |
| Execution log | Log table queried at the end of each proof notebook |

---

## Data quality notes

- **Futures** (symbols containing `-`) are kept in Bronze but excluded from Silver.
- **Missing open price:** some rows (e.g. on 1 Jan 2015) have no open price; these are kept with `open = NULL`.
- **Sector codes** changed on 3 June 2015 (old 4-digit scheme → `0801`–`0839`); `dim_stock` uses each stock's latest code, and code `0839` (Apparel) appeared in 2026.
- **Splits / bonus issues:** PSX adjusts LDCP on the ex-date (verified on LUCK's 5:1 split, 28 Apr 2025), so `ln(close/ldcp)` gives the true return.
- **Right shares** (`…R1`) are excluded on the dashboard; excluding them in Gold is future work.
- **Odd source days:** four weekdays returned a non-archive response from PSX during download and were retried.

---

## Security & compliance

The data contains **no personally identifiable information (PII)**: it is aggregated, exchange-level market data with no individual investors, accounts or trades. PSX data is used strictly for **academic purposes**; only small sample files are committed, and full datasets are excluded via `.gitignore`. Downloads are rate-limited and identify the project in the User-Agent.

---

## Tech stack

- **Processing:** PySpark on Databricks Free Edition (serverless)
- **Storage:** Delta Lake, Unity Catalog volumes
- **Extraction:** Python (`requests`) on Google Colab
- **Visualization:** Power BI Desktop (Databricks SQL Warehouse connector)
- **Version control:** Git & GitHub (Databricks Git folder)

---

## Roadmap

- [x] **Phase 1** — Proposal, data source validation, sample payloads
- [x] **Phase 2** — Bronze and Silver layers with full, incremental and backfill loads
- [x] **Phase 3** — Gold star schema, end-to-end pipeline, Power BI dashboard, final report

---

## Team

- Saqib — _role_
- _Teammate name_ — _role_

Course: Data Visualization and Analysis — FAST-NUCES Lahore
