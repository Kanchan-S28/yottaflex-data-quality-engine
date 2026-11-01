
import json
import os
import sqlite3
import uuid
import logging
import time

from datetime import datetime, timezone

import pandas as pd
import yaml


SEVERITY_ORDER = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1
}


class DataQualityEngine:

    def __init__(
        self,
        config_path="config/rules.yaml",
        db_path="data/quality.db",
        db_retries=3
    ):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s"
        )

        self.logger = logging.getLogger(__name__)

        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        self.db_path = db_path
        self.db_retries = db_retries

        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        self._init_db()

        self.logger.info(
            "Data Quality Engine initialized. Database: %s",
            self.db_path
        )

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=10)

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                ended_at TEXT,
                status TEXT,
                input_rows INTEGER,
                valid_rows INTEGER,
                rejected_rows INTEGER,
                duration_seconds REAL,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS rule_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                dataset TEXT,
                rule_name TEXT,
                severity TEXT,
                checked_rows INTEGER,
                failed_rows INTEGER,
                passed INTEGER,
                details TEXT
            );

            CREATE TABLE IF NOT EXISTS rejects (
                reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                dataset TEXT,
                source_row INTEGER,
                record_json TEXT,
                reasons TEXT
            );

            CREATE TABLE IF NOT EXISTS target_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT,
                product_id TEXT,
                quantity INTEGER,
                amount REAL,
                order_timestamp TEXT,
                run_id TEXT
            );
            """)

        self.logger.info("Database tables initialized.")

    def _load(self, name, spec):
        path = spec["file"]

        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Input file not found: {path}"
            )

        self.logger.info(
            "Loading dataset '%s' from %s",
            name,
            path
        )

        df = pd.read_csv(path)

        self.logger.info(
            "Dataset '%s' loaded successfully. Rows: %s",
            name,
            len(df)
        )

        return df

    def _evaluate_rule(self, df, rule, datasets):
        kind = rule["type"]

        if kind == "completeness":
            failed = df[rule["column"]].isna()

        elif kind == "uniqueness":
            failed = df[rule["column"]].duplicated(
                keep="first"
            )

        elif kind == "range":
            col = pd.to_numeric(
                df[rule["column"]],
                errors="coerce"
            )

            failed = (
                col.isna()
                | (col < rule["min"])
                | (col > rule["max"])
            )

        elif kind == "referential_integrity":
            ref = datasets[
                rule["reference_dataset"]
            ][rule["reference_column"]].dropna()

            failed = (
                df[rule["column"]].isna()
                | ~df[rule["column"]].isin(set(ref))
            )

        elif kind == "freshness":
            parsed = pd.to_datetime(
                df[rule["column"]],
                errors="coerce",
                utc=True
            )

            age_days = (
                pd.Timestamp.now(tz="UTC") - parsed
            ).dt.total_seconds() / 86400

            failed = (
                parsed.isna()
                | (age_days > rule["max_age_days"])
            )

        elif kind == "custom":

            if rule["expression"] == "email":
                failed = ~df[rule["check"]].astype(
                    "string"
                ).str.match(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
                    na=False
                )

            elif rule["expression"] == "quantity_amount":
                qty = pd.to_numeric(
                    df["quantity"],
                    errors="coerce"
                )

                amount = pd.to_numeric(
                    df["amount"],
                    errors="coerce"
                )

                failed = (
                    qty.isna()
                    | amount.isna()
                    | (amount < qty * 0.01)
                )

            else:
                raise ValueError(
                    f"Unsupported custom expression: "
                    f"{rule['expression']}"
                )

        else:
            raise ValueError(
                f"Unsupported rule type: {kind}"
            )

        return failed.fillna(True).astype(bool)

    def run(self):
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)

        datasets = {}
        results = []
        rejects = []

        input_rows = 0
        valid_rows = 0
        rejected_rows = 0

        status = "SUCCESS"
        error_message = None

        self.logger.info(
            "Starting data quality run: %s",
            run_id
        )

        try:
            # Load all datasets first.
            # This allows reference checks to work
            # independently of dataset order.

            for name, spec in self.config["datasets"].items():

                df = self._load(name, spec)

                missing_cols = sorted(
                    set(spec["required_columns"])
                    - set(df.columns)
                )

                if missing_cols:
                    raise ValueError(
                        f"{name}: missing required columns "
                        f"{missing_cols}"
                    )

                datasets[name] = df

            self.logger.info(
                "All input datasets loaded successfully."
            )

            # Validate each dataset.

            for name, spec in self.config["datasets"].items():

                self.logger.info(
                    "Starting validation for dataset: %s",
                    name
                )

                df = datasets[name].copy()

                input_rows += len(df)

                row_reasons = {
                    idx: [] for idx in df.index
                }

                for rule in spec.get("rules", []):

                    failed = self._evaluate_rule(
                        df,
                        rule,
                        datasets
                    )

                    failed_indices = list(
                        df.index[failed]
                    )

                    for idx in failed_indices:
                        row_reasons[idx].append(
                            rule["name"]
                        )

                    results.append({
                        "dataset": name,
                        "rule_name": rule["name"],
                        "severity": rule.get(
                            "severity", "medium"
                        ),
                        "checked_rows": len(df),
                        "failed_rows": len(failed_indices),
                        "passed": int(
                            len(failed_indices) == 0
                        ),
                        "details": json.dumps({
                            "failed_source_rows": [
                                int(i) + 2
                                for i in failed_indices[:100]
                            ]
                        })
                    })

                    self.logger.info(
                        "Rule '%s' on '%s': "
                        "%s checked, %s failed, severity=%s",
                        rule["name"],
                        name,
                        len(df),
                        len(failed_indices),
                        rule.get("severity", "medium")
                    )

                # Reject a row if any rule fails.

                valid_mask = pd.Series(
                    [
                        len(row_reasons[i]) == 0
                        for i in df.index
                    ],
                    index=df.index
                )

                valid_df = df[valid_mask].copy()
                rejected_df = df[~valid_mask].copy()

                valid_rows += len(valid_df)
                rejected_rows += len(rejected_df)

                # Save rejected records for quarantine.

                for idx, row in rejected_df.iterrows():

                    rejects.append((
                        run_id,
                        name,
                        int(idx) + 2,
                        json.dumps(
                            row.where(
                                pd.notna(row),
                                None
                            ).to_dict(),
                            default=str
                        ),
                        json.dumps(row_reasons[idx])
                    ))

                self.logger.info(
                    "Dataset '%s' completed: "
                    "%s valid, %s rejected",
                    name,
                    len(valid_df),
                    len(rejected_df)
                )

                # Publish valid orders.

                if name == "orders" and not valid_df.empty:

                    self._publish_orders(
                        valid_df,
                        run_id
                    )

            # Persist run metrics and audit results.

            ended = datetime.now(timezone.utc)

            duration = (
                ended - started
            ).total_seconds()

            with self._connect() as conn:

                conn.execute(
                    """
                    INSERT INTO runs
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        started.isoformat(),
                        ended.isoformat(),
                        status,
                        input_rows,
                        valid_rows,
                        rejected_rows,
                        duration,
                        error_message
                    )
                )

                conn.executemany(
                    """
                    INSERT INTO rule_results
                    (
                        run_id,
                        dataset,
                        rule_name,
                        severity,
                        checked_rows,
                        failed_rows,
                        passed,
                        details
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            run_id,
                            r["dataset"],
                            r["rule_name"],
                            r["severity"],
                            r["checked_rows"],
                            r["failed_rows"],
                            r["passed"],
                            r["details"]
                        )
                        for r in results
                    ]
                )

                conn.executemany(
                    """
                    INSERT INTO rejects
                    (
                        run_id,
                        dataset,
                        source_row,
                        record_json,
                        reasons
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    rejects
                )

            self.logger.info(
                "Run completed successfully. "
                "Run ID: %s | Input: %s | Valid: %s | "
                "Rejected: %s | Duration: %.4f seconds",
                run_id,
                input_rows,
                valid_rows,
                rejected_rows,
                duration
            )

            return {
                "run_id": run_id,
                "status": status,
                "input_rows": input_rows,
                "valid_rows": valid_rows,
                "rejected_rows": rejected_rows,
                "duration_seconds": round(duration, 4)
            }

        except Exception as exc:

            status = "FAILED"
            error_message = str(exc)

            ended = datetime.now(timezone.utc)

            duration = (
                ended - started
            ).total_seconds()

            self.logger.exception(
                "Data quality run failed. Run ID: %s",
                run_id
            )

            # Record failed run metrics.

            with self._connect() as conn:

                conn.execute(
                    """
                    INSERT INTO runs
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        started.isoformat(),
                        ended.isoformat(),
                        status,
                        input_rows,
                        valid_rows,
                        rejected_rows,
                        duration,
                        error_message
                    )
                )

            raise

    def _publish_orders(self, df, run_id):
        """
        Publish valid orders using an idempotent upsert.

        Retry temporary SQLite database-lock errors.
        """

        records = [
            (
                r["order_id"],
                r["customer_id"],
                r["product_id"],
                int(r["quantity"]),
                float(r["amount"]),
                str(r["order_timestamp"]),
                run_id
            )
            for _, r in df.iterrows()
        ]

        query = """
            INSERT INTO target_orders
            (
                order_id,
                customer_id,
                product_id,
                quantity,
                amount,
                order_timestamp,
                run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(order_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                product_id = excluded.product_id,
                quantity = excluded.quantity,
                amount = excluded.amount,
                order_timestamp = excluded.order_timestamp,
                run_id = excluded.run_id
        """

        for attempt in range(self.db_retries + 1):

            try:
                with self._connect() as conn:

                    conn.executemany(
                        query,
                        records
                    )

                self.logger.info(
                    "Published %s valid orders for run %s",
                    len(records),
                    run_id
                )

                return

            except sqlite3.OperationalError as exc:

                error_text = str(exc).lower()

                is_temporary_lock = (
                    "locked" in error_text
                    or "busy" in error_text
                )

                if (
                    not is_temporary_lock
                    or attempt >= self.db_retries
                ):
                    self.logger.exception(
                        "Order publishing failed after %s attempts.",
                        attempt + 1
                    )
                    raise

                wait_seconds = 2 ** attempt

                self.logger.warning(
                    "Database temporarily locked. "
                    "Retrying attempt %s of %s in %s seconds.",
                    attempt + 1,
                    self.db_retries,
                    wait_seconds
                )

                time.sleep(wait_seconds)


def report(db_path="data/quality.db"):

    with sqlite3.connect(db_path) as conn:

        runs = pd.read_sql_query(
            """
            SELECT *
            FROM runs
            ORDER BY started_at DESC
            """,
            conn
        )

        if runs.empty:
            print("\nNo pipeline runs found.")

            return (
                runs,
                pd.DataFrame(),
                pd.DataFrame()
            )

        latest_run_id = runs.iloc[0]["run_id"]

        rules = pd.read_sql_query(
            """
            SELECT
                dataset,
                rule_name,
                severity,
                checked_rows,
                failed_rows,
                passed
            FROM rule_results
            WHERE run_id = ?
            ORDER BY dataset, rule_name
            """,
            conn,
            params=(latest_run_id,)
        )

        rejects = pd.read_sql_query(
            """
            SELECT
                dataset,
                COUNT(*) AS rejected_records
            FROM rejects
            WHERE run_id = ?
            GROUP BY dataset
            """,
            conn,
            params=(latest_run_id,)
        )

    print(
        "\nLATEST RUNS\n",
        runs.head(5).to_string(index=False)
    )

    print(
        "\nRULE RESULTS\n",
        rules.to_string(index=False)
    )

    print(
        "\nREJECTED RECORDS\n",
        rejects.to_string(index=False)
    )

    return runs, rules, rejects
