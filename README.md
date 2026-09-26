# PSX Risk Lakehouse

An end-to-end data pipeline that ingests daily trading data from the **Pakistan Stock Exchange (PSX)**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold) on **Apache Spark**, and delivers volatility-based risk analytics through a business intelligence dashboard.

> **Status:** Phase 1 — Project proposal & data source validation

---

## Overview

Stock prices are hard to predict, but *risk* is not random: turbulent periods cluster together, and calm periods do too. This project turns raw PSX market data into a clean, analysis-ready view of **which stocks and sectors are in high-risk regimes, whether turbulence is market-wide or company-specific, and how volatility responds to monetary policy decisions.**

Pakistan's recent market history makes this an unusually rich dataset: policy rates moved from 7% to a record 22% and back to ~11.5% between 2020 and 2026, alongside a major equity rally and several shock events.

### Business questions the dashboard will answer

1. Which stocks and sectors are currently in a high-volatility regime?
2. Is observed turbulence **systemic** (market-wide) or **idiosyncratic** (company-specific)?
3. How does market volatility behave around State Bank of Pakistan policy rate decisions?

---

## Architecture

```
 Sources                 Bronze                 Silver                   Gold                  BI
┌──────────────┐    ┌───────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌───────────┐
│ PSX historical│──▶│ Raw files, as │──▶│ Typed, deduped,  │──▶│ Volatility &     │──▶│ Dashboard │
│ PSX daily     │   │ fetched, with │   │ validated, split-│   │ risk tiers,      │   │           │
│ SBP rates     │   │ ingest metadata│   │ adjusted prices  │   │ sector & event   │   │           │
└──────────────┘    └───────────────┘    └──────────────────┘    │ aggregates       │    └───────────┘
                                                                  └──────────────────┘
```

| Layer  | Purpose | Key processing |
|--------|---------|----------------|
| **Bronze** | Raw, immutable landing zone | Append-only; ingestion timestamp, batch ID, and source recorded; partitioned by ingest date |
| **Silver** | Cleansed, conformed data | Type casting, chronological ordering, de-duplication, OHLC validity checks, corporate-action (split/bonus) adjustment, daily log returns |
| **Gold** | Analyst-ready models | Rolling volatility (5/10/20-day), per-stock risk tiers, beta vs KSE-100, sector risk aggregates, rate-decision event windows |

---

## Data sources

| Source | Content | Load type |
|--------|---------|-----------|
| [PSX Data Portal — Historical Data](https://dps.psx.com.pk/historical), via [`psxdata`](https://github.com/mtauha/psxdata) | Daily OHLCV history per listed company and for the KSE-100 index | Full load |
| PSX Daily Downloads — Market Summary (Closing): `https://dps.psx.com.pk/download/mkt_summary/{YYYY-MM-DD}.Z` | End-of-day summary of all traded securities, one file per trading day | Incremental load |
| [PSX symbol directory](https://dps.psx.com.pk/symbols), via `psxdata.symbols()` | Listed securities with company names and sectors | Reference data |
| [SBP — Structure of Interest Rates](https://www.sbp.org.pk/assets/document/sir.pdf) | Policy rate decisions and effective dates | Reference data |

### Handling new, updated, and deleted records

- **New:** each trading day adds one record per active symbol.
- **Updated:** corporate actions restate historical prices. Example: Lucky Cement (LUCK) had a 5:1 split/bonus adjustment on 28 April 2025 that appeared in raw data as an impossible −80% one-day return. The pipeline detects implausible returns and reprocesses affected history.
- **Deleted:** delisted or suspended companies stop appearing in daily data; the pipeline marks them inactive instead of silently dropping them.

---

## Security & compliance

The data contains **no personally identifiable information (PII)**. It is aggregated, exchange-level market data (prices and volumes per company) with no individual investors, accounts, or trades.

PSX market data is used here strictly for **academic purposes**. Only small sample files are committed to this repository; full datasets are excluded via `.gitignore` and are not redistributed.

---

## Repository structure

```
psx-risk-lakehouse/
├── README.md
├── docs/              # Proposal and project documentation
├── samples/
│   ├── full_load/     # Sample full-load payloads
│   └── incremental/   # Sample incremental payload
├── notebooks/
│   ├── 01_bronze/     # Ingestion
│   ├── 02_silver/     # Cleansing & conformance
│   └── 03_gold/       # Aggregations & models
└── dashboard/         # BI dashboard files
```

---

## Tech stack

- **Processing:** Apache Spark (Databricks Free Edition)
- **Storage format:** Delta Lake
- **Ingestion:** Python (`psxdata`, `pandas`)
- **Visualization:** Power BI
- **Version control:** Git & GitHub

---

## Roadmap

- [ ] **Phase 1** — Proposal, data source validation, sample payloads
- [ ] **Phase 2** — Bronze and Silver layers with full + incremental loads
- [ ] **Phase 3** — Gold layer models and BI dashboard

---

## Team

- Saqib Qazi
- Hassan Shakil Pasha

Course: Data Visualization and Analysis — FAST-NUCES Lahore
