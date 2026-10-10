# PSX Risk Lakehouse

An end-to-end data pipeline that ingests daily trading data from the **Pakistan Stock Exchange (PSX)**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold) with **PySpark and Delta Lake on Databricks**, and will deliver volatility-based risk analytics through a Power BI dashboard.

> **Status:** Phase 2 complete — Bronze and Silver layers with full, incremental and backfill loads, idempotent reruns, schema-drift quarantine and execution logging.

---

## Overview

Stock prices are hard to predict, but *risk* is not random: turbulent periods cluster together, and calm periods do too. This project turns raw PSX market data into a clean, analysis-ready view of **which stocks and sectors are in high-risk regimes, whether turbulence is market-wide or company-specific, and how volatility responds to monetary policy decisions.**

### Business questions the dashboard will answer

1. Which stocks and sectors are currently in a high-volatility regime?
2. Is observed turbulence **systemic** (market-wide) or **idiosyncratic** (company-specific)?
3. How does market volatility behave around State Bank of Pakistan policy rate decisions?

---

## Architecture

```
 Source                   Bronze                        Silver                          Gold (Phase 3)
┌────────────────┐   ┌──────────────────────────┐   ┌───────────────────────────┐   ┌──────────────────┐
│ PSX daily       │──▶│ bronze.mkt_summary        │──▶│ silver.daily_prices        │──▶│ Volatility, risk │
│ Market Summary  │   │ raw strings, 1 row/line,  │   │ typed, deduplicated,       │   │ tiers, sector &  │
│ (.Z per day)    │   │ + file date, source,      │   │ validated, MERGE INTO on   │   │ event aggregates │
└────────────────┘   │   batch_id, load_timestamp│   │ (symbol, trade_date)       │   └──────────────────┘
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
| **Gold** *(Phase 3)* | Analyst-ready models | Daily log returns, split/bonus adjustment, rolling volatility, risk tiers, beta vs KSE-100, sector aggregates, rate-decision event windows |

---

## Data source

| Source | Content | Used in |
|--------|---------|---------|
| PSX Daily Downloads — Market Summary (Closing): `https://dps.psx.com.pk/download/mkt_summary/{YYYY-MM-DD}.Z` | End-of-day OHLCV for every traded security, one file per trading day, 2015 → present | Bronze, Silver |
| [SBP — Structure of Interest Rates](https://www.sbp.org.pk/assets/document/sir.pdf) | Policy rate decisions and effective dates | Gold *(Phase 3)* |

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
- **Deleted / delisted:** a delisted company simply stops appearing in new files. Its history stays in Silver; Gold will mark it inactive.

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

### `ops.pipeline_execution_logs` — one row per Bronze file or Silver run

| Column | Type | Description |
|---|---|---|
| batch_id | STRING | Run identifier |
| layer | STRING | `raw_to_bronze` or `bronze_to_silver` |
| load_type | STRING | `full`, `incremental` or `backfill` |
| parameter | STRING | Source file path (Bronze) or date range `start..end` (Silver) |
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
    └── 02_silver/
        ├── silver_functions       # build_silver(), load_silver()
        ├── 01_bronze_to_silver    # Runner with widgets
        └── 02_silver_proofs       # Incremental, rerun, update, type-drift proofs (saved outputs)
```

---

## Run guide

### One-time setup

1. Clone this repo into Databricks as a Git folder.
2. Run `notebooks/00_setup` to create the `bronze`, `silver`, `ops` schemas, the volume and all tables.
3. Upload raw files to `/Volumes/workspace/bronze/raw_landing/mkt_summary/{year}/{YYYY-MM-DD}.Z`.

### Load types

Both runner notebooks take three widgets: `start_date`, `end_date`, `load_type`.

| load_type | Bronze (`01_raw_to_bronze`) | Silver (`01_bronze_to_silver`) | Use it for |
|---|---|---|---|
| `full` | Loads every weekday file in the widget range | Processes the widget range | First load of the whole history |
| `incremental` | Loads the widget range (normally just the new days) | **Ignores the widgets**: processes from the day after the last Silver date to the newest Bronze date | Daily runs |
| `backfill` | Reloads the widget range, replacing those days | Reprocesses the widget range | Re-running a past period after a fix or a re-published file |

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
- **Sector codes** changed over time (e.g. mutual funds `8980` in 2015 vs `0806` later); Gold will map them to a single sector dimension.
- **Odd source days:** four weekdays returned a non-archive response from PSX during download and were retried.

---

## Security & compliance

The data contains **no personally identifiable information (PII)**: it is aggregated, exchange-level market data with no individual investors, accounts or trades. PSX data is used strictly for **academic purposes**; only small sample files are committed, and full datasets are excluded via `.gitignore`. Downloads are rate-limited and identify the project in the User-Agent.

---

## Tech stack

- **Processing:** PySpark on Databricks Free Edition (serverless)
- **Storage:** Delta Lake, Unity Catalog volumes
- **Extraction:** Python (`requests`) on Google Colab
- **Visualization:** Power BI *(Phase 3)*
- **Version control:** Git & GitHub (Databricks Git folder)

---

## Roadmap

- [x] **Phase 1** — Proposal, data source validation, sample payloads
- [x] **Phase 2** — Bronze and Silver layers with full, incremental and backfill loads
- [ ] **Phase 3** — Gold layer models and BI dashboard

