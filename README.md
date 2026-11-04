# Lakehouse Financial Transactions Pipeline

An end-to-end Lakehouse batch pipeline built with PySpark, Delta Lake, and Databricks Asset Bundles (DABs), following the Medallion Architecture (Bronze, Silver, Gold).

---

## Architecture Overview

```
[Synthetic JSONL Data] 
       │
       ▼
[Bronze Layer]   ── Raw ingestion, schema tracking, and transaction append
       │
       ▼
[Silver Layer]   ── Deduplication, schema enforcement, and PII masking
       │
       ▼
[Gold Layer]     ── Aggregations, business KPIs, and analytics tables
```

* **Data Generation:** Synthetic transactional events with UTC timestamps, merchant categorization, amounts, and customer attributes.
* **Bronze Layer:** Ingestion of raw events directly into Delta format preserving source lineage.
* **Silver Layer:** Data quality enforcement, deduplication, schema validation, and PII anonymization.
* **Gold Layer:** Business-level metrics and financial summaries optimized for analytics and consumption.

---

## Tech Stack

* **Language:** Python 3.10+
* **Processing Engine:** Apache Spark (`pyspark==3.5.0`)
* **Storage Layer:** Delta Lake (`delta-spark==3.2.0`)
* **Mock Data Generator:** `faker`, `uuid`, `random`
* **Orchestration & Packaging:** Databricks Asset Bundles (DABs)
* **Testing:** `pytest`

---

## Project Structure

```text
lakehouse-transactions-bundle/
├── data/
│   └── raw/                   # Ignored locally; generated synthetic JSON Lines
├── scripts/
│   └── generate_mock_data.py  # CLI script to generate transactional mock batches
├── src/                       # Lakehouse pipeline modules (Bronze, Silver, Gold)
├── tests/                     # Unit and integration tests
├── .gitignore
├── README.md
└── requirements.txt
```

---

## Getting Started

### 1. Environment Setup

Clone the repository and set up a Python virtual environment:

```powershell
# Create virtual environment
python -m venv .venv

# Activate on Windows PowerShell
.\.venv\Scripts\Activate.ps1

# Install project dependencies
pip install -r requirements.txt
```

### 2. Generate Synthetic Transaction Data

Run the CLI script to produce raw transactional events:

```powershell
# Default run (generates records into data/raw/)
python .\scripts\generate_mock_data.py

# Custom execution example
python .\scripts\generate_mock_data.py --records 500 --output-dir data/raw
```

---

## Conventions & Standards

* **Timezone:** Strict **UTC** (`timezone.utc`) for all transactional records and event timestamps.
* **Encoding:** Universal **UTF-8** across all file outputs and stream readers.
* **File Formats:** Raw data saved as **JSON Lines (`.json`)**; storage layers persisted in **Delta Lake**.
* **Commit Guidelines:** Follows the [Conventional Commits](https://www.conventionalcommits.org/) specification (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`).