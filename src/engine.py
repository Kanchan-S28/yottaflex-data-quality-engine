import json
import os
import sqlite3
import uuid
import logging
from datetime import datetime, timezone
import pandas as pd
import yaml

SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1}

class DataQualityEngine:
    def __init__(self, config_path="config/rules.yaml", db_path="data/quality.db"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_db()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT,
                status TEXT, input_rows INTEGER, valid_rows INTEGER,
                rejected_rows INTEGER, duration_seconds REAL, error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS rule_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT,
                dataset TEXT, rule_name TEXT, severity TEXT,
                checked_rows INTEGER, failed_rows INTEGER, passed INTEGER,
                details TEXT
            );
            CREATE TABLE IF NOT EXISTS rejects (
                reject_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT,
                dataset TEXT, source_row INTEGER, record_json TEXT, reasons TEXT
            );
            CREATE TABLE IF NOT EXISTS target_orders (
                order_id TEXT PRIMARY KEY, customer_id TEXT, product_id TEXT,
                quantity INTEGER, amount REAL, order_timestamp TEXT, run_id TEXT
            );
            """)

    def _load(self, name, spec):
        path = spec["file"]
        if not os.path.exists(path):
            raise FileNotFoundError(f"Input file not found: {path}")
        return pd.read_csv(path)

    def _evaluate_rule(self, df, rule, datasets):
        kind = rule["type"]
        name = rule["name"]
        if kind == "completeness":
            failed = df[rule["column"]].isna()
        elif kind == "uniqueness":
            failed = df[rule["column"]].duplicated(keep="first")
        elif kind == "range":
            col = pd.to_numeric(df[rule["column"]], errors="coerce")
            failed = col.isna() | (col < rule["min"]) | (col > rule["max"])
        elif kind == "referential_integrity":
            ref = datasets[rule["reference_dataset"]][rule["reference_column"]].dropna()
            failed = df[rule["column"]].isna() | ~df[rule["column"]].isin(set(ref))
        elif kind == "freshness":
            parsed = pd.to_datetime(df[rule["column"]], errors="coerce", utc=True)
            age_days = (pd.Timestamp.now(tz="UTC") - parsed).dt.total_seconds() / 86400
            failed = parsed.isna() | (age_days > rule["max_age_days"])
        elif kind == "custom":
            if rule["expression"] == "email":
                failed = ~df[rule["check"]].astype("string").str.match(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", na=False)
            elif rule["expression"] == "quantity_amount":
                qty = pd.to_numeric(df["quantity"], errors="coerce")
                amount = pd.to_numeric(df["amount"], errors="coerce")
                failed = qty.isna() | amount.isna() | (amount < qty * 0.01)
            else:
                raise ValueError(f"Unsupported custom expression: {rule['expression']}")
        else:
            raise ValueError(f"Unsupported rule type: {kind}")
        return failed.fillna(True).astype(bool)

    def run(self):
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)
        datasets = {}
        results = []
        rejects = []
        input_rows = valid_rows = rejected_rows = 0
        status, error_message = "SUCCESS", None
        try:
            # Load all sources first so reference checks are order-independent.
            for name, spec in self.config["datasets"].items():
                df = self._load(name, spec)
                missing_cols = sorted(set(spec["required_columns"]) - set(df.columns))
                if missing_cols:
                    raise ValueError(f"{name}: missing required columns {missing_cols}")
                datasets[name] = df

            for name, spec in self.config["datasets"].items():
                df = datasets[name].copy()
                input_rows += len(df)
                row_reasons = {idx: [] for idx in df.index}
                for rule in spec.get("rules", []):
                    failed = self._evaluate_rule(df, rule, datasets)
                    failed_indices = list(df.index[failed])
                    for idx in failed_indices:
                        row_reasons[idx].append(rule["name"])
                    results.append({
                        "dataset": name, "rule_name": rule["name"],
                        "severity": rule.get("severity", "medium"),
                        "checked_rows": len(df), "failed_rows": len(failed_indices),
                        "passed": int(len(failed_indices) == 0),
                        "details": json.dumps({"failed_source_rows": [int(i) + 2 for i in failed_indices[:100]]})
                    })

                # A row is rejected when any configured rule fails.
                valid_mask = pd.Series([len(row_reasons[i]) == 0 for i in df.index], index=df.index)
                valid_df = df[valid_mask].copy()
                rejected_df = df[~valid_mask].copy()
                valid_rows += len(valid_df)
                rejected_rows += len(rejected_df)
                for idx, row in rejected_df.iterrows():
                    rejects.append((run_id, name, int(idx) + 2,
                                    json.dumps(row.where(pd.notna(row), None).to_dict(), default=str),
                                    json.dumps(row_reasons[idx])))

                if name == "orders" and not valid_df.empty:
                    self._publish_orders(valid_df, run_id)

            ended = datetime.now(timezone.utc)
            duration = (ended - started).total_seconds()
            with self._connect() as conn:
                conn.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, started.isoformat(), ended.isoformat(), status, input_rows,
                     valid_rows, rejected_rows, duration, error_message))
                conn.executemany("""INSERT INTO rule_results
                    (run_id,dataset,rule_name,severity,checked_rows,failed_rows,passed,details)
                    VALUES (?,?,?,?,?,?,?,?)""", [
                    (run_id, r["dataset"], r["rule_name"], r["severity"], r["checked_rows"],
                     r["failed_rows"], r["passed"], r["details"]) for r in results])
                conn.executemany("""INSERT INTO rejects
                    (run_id,dataset,source_row,record_json,reasons) VALUES (?,?,?,?,?)""", rejects)
            return {"run_id": run_id, "status": status, "input_rows": input_rows,
                    "valid_rows": valid_rows, "rejected_rows": rejected_rows,
                    "duration_seconds": round(duration, 4)}
        except Exception as exc:
            status, error_message = "FAILED", str(exc)
            ended = datetime.now(timezone.utc)
            duration = (ended - started).total_seconds()
            with self._connect() as conn:
                conn.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, started.isoformat(), ended.isoformat(), status, input_rows,
                     valid_rows, rejected_rows, duration, error_message))
            raise

    def _publish_orders(self, df, run_id):
        # Stable order_id primary key makes repeat publication idempotent.
        with self._connect() as conn:
            for _, r in df.iterrows():
                conn.execute("""INSERT INTO target_orders
                    (order_id,customer_id,product_id,quantity,amount,order_timestamp,run_id)
                    VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT(order_id) DO UPDATE SET
                    customer_id=excluded.customer_id, product_id=excluded.product_id,
                    quantity=excluded.quantity, amount=excluded.amount,
                    order_timestamp=excluded.order_timestamp, run_id=excluded.run_id
                    """, (r["order_id"], r["customer_id"], r["product_id"],
                          int(r["quantity"]), float(r["amount"]),
                          str(r["order_timestamp"]), run_id))

def report(db_path="data/quality.db"):
    with sqlite3.connect(db_path) as conn:
        runs = pd.read_sql_query("SELECT * FROM runs ORDER BY started_at DESC", conn)
        rules = pd.read_sql_query("SELECT dataset,rule_name,severity,checked_rows,failed_rows,passed FROM rule_results ORDER BY dataset,rule_name", conn)
        rejects = pd.read_sql_query("SELECT dataset, COUNT(*) AS rejected_records FROM rejects GROUP BY dataset", conn)
    print("\nLATEST RUNS\n", runs.head(5).to_string(index=False))
    print("\nRULE RESULTS\n", rules.to_string(index=False))
    print("\nREJECTED RECORDS\n", rejects.to_string(index=False))
    return runs, rules, rejects
