# YottaFlex Data Quality Validation Engine

A local, configurable data-quality pipeline built for the YottaFlex.ai 72-hour Data Engineering challenge.

## Features
- Completeness, uniqueness, referential-integrity, range, freshness, and custom business-rule checks.
- YAML-driven dataset and rule configuration.
- CSV ingestion, valid-record publishing, and rejected-record quarantine.
- SQLite audit tables for runs, rule results, rejects, and target orders.
- Stable run IDs, source-row traceability, logging, and idempotent target publishing.
- Reproducible synthetic data generator (fixed random seed).
- Pytest automated tests.

## Architecture
CSV inputs -> ingestion/schema validation -> configurable rule engine -> valid target + quarantine -> SQLite audit/reporting.

## Requirements
- Python 3.9+
- pip

## Setup
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate
pip install -r requirements.txt
```

## Run with sample data
```bash
python run_pipeline.py --generate --orders 1000
```

Generate a larger dataset:
```bash
python run_pipeline.py --generate --orders 100000 --seed 42
```

Run against existing CSV files:
```bash
python run_pipeline.py
```

## Tests
```bash
pytest -q
```

## SQLite tables
- `runs`: run status, start/end time, input/valid/rejected counts, duration, failure reason.
- `rule_results`: dataset, rule, severity, checked rows, failed rows, pass/fail and details.
- `rejects`: original record JSON, source row number, and failed rule names.
- `target_orders`: published valid orders; `order_id` is the primary key.

## Configuration
Edit `config/rules.yaml` to add datasets and rules. Supported types:
- `completeness`: null check.
- `uniqueness`: duplicate check (first occurrence is retained; later duplicates fail).
- `range`: inclusive minimum/maximum check.
- `referential_integrity`: value must exist in a referenced dataset column.
- `freshness`: timestamp must be parseable and no older than `max_age_days`.
- `custom`: supported named expressions `email` and `quantity_amount`.

Custom expressions are intentionally allow-listed; arbitrary YAML/Python execution is not supported.

## Design choices and trade-offs
- Pandas + SQLite keep the project easy to run locally without cloud credentials.
- All input datasets are loaded before rules run so cross-dataset reference checks work regardless of YAML order.
- A row is quarantined if it fails at least one configured rule. Rule-level results still show every individual failure.
- Target publishing uses `order_id` as a stable key and SQLite UPSERT, so rerunning the same input does not add duplicate target records.
- Duplicate IDs: the first occurrence passes uniqueness; subsequent occurrences are rejected.
- The current implementation uses in-memory Pandas DataFrames. For production-scale data beyond local memory, replace Pandas with Spark/Databricks and SQLite with Delta Lake or a warehouse.
- Freshness is measured against the execution time in UTC. Adjust `max_age_days` for the business SLA.
- For a production deployment, add secrets management, alerting, orchestration, partitioning, and role-based access control.

## Data dictionary
### customers
| Column | Type | Meaning |
|---|---|---|
| customer_id | string | Stable customer key |
| customer_name | string | Customer display name |
| email | string | Customer email |
| age | integer | Customer age |
| created_at | ISO timestamp | Customer creation time |

### products
| Column | Type | Meaning |
|---|---|---|
| product_id | string | Stable product key |
| product_name | string | Product display name |
| category | string | Product category |
| unit_price | decimal | Unit selling price |

### orders
| Column | Type | Meaning |
|---|---|---|
| order_id | string | Stable order/event key |
| customer_id | string | Customer foreign key |
| product_id | string | Product foreign key |
| quantity | integer | Units ordered |
| amount | decimal | Total order amount |
| order_timestamp | ISO timestamp | Order event time |

## Example validation/report queries
```sql
-- Latest pipeline runs
SELECT * FROM runs ORDER BY started_at DESC;

-- Rules with failures
SELECT dataset, rule_name, severity, failed_rows
FROM rule_results
WHERE failed_rows > 0;

-- Rejected records by dataset
SELECT dataset, COUNT(*) AS rejected_count
FROM rejects
GROUP BY dataset;

-- Target reconciliation
SELECT COUNT(*) AS published_orders,
       ROUND(SUM(amount), 2) AS published_revenue
FROM target_orders;
```

## Performance measurement
The run summary prints elapsed seconds and row counts. For the challenge, execute with 1,000 and 100,000 orders and record the observed duration on your machine. One simple optimization is to use vectorized Pandas checks and set-based reference lookups; for larger production workloads, use Spark distributed processing.

## Submission checklist
- [ ] Run sample pipeline successfully.
- [ ] Run `pytest -q` and capture passing test output.
- [ ] Run the 100,000-row generator and record runtime.
- [ ] Review README and configuration.
- [ ] Push the project to a **public** GitHub repository.
- [ ] Paste the repository URL into the challenge form only after verifying the public link.
