
import sqlite3
import yaml
import pandas as pd
import pytest

from src.generator import generate_data
from src.engine import DataQualityEngine


def setup_project(tmp_path, orders=30):
    data_dir = tmp_path / "data" / "input"

    generate_data(
        str(data_dir),
        customers=10,
        products=5,
        orders=orders,
        seed=7,
        inject_issues=True
    )

    config = {
        "datasets": {
            "customers": {
                "file": str(data_dir / "customers.csv"),
                "primary_key": "customer_id",
                "required_columns": [
                    "customer_id", "customer_name", "email",
                    "age", "created_at"
                ],
                "rules": [
                    {
                        "name": "customer_id_unique",
                        "type": "uniqueness",
                        "column": "customer_id",
                        "severity": "critical"
                    }
                ]
            },
            "products": {
                "file": str(data_dir / "products.csv"),
                "primary_key": "product_id",
                "required_columns": [
                    "product_id", "product_name",
                    "category", "unit_price"
                ],
                "rules": [
                    {
                        "name": "product_id_unique",
                        "type": "uniqueness",
                        "column": "product_id",
                        "severity": "critical"
                    }
                ]
            },
            "orders": {
                "file": str(data_dir / "orders.csv"),
                "primary_key": "order_id",
                "required_columns": [
                    "order_id", "customer_id", "product_id",
                    "quantity", "amount", "order_timestamp"
                ],
                "rules": [
                    {
                        "name": "order_id_unique",
                        "type": "uniqueness",
                        "column": "order_id",
                        "severity": "critical"
                    },
                    {
                        "name": "quantity_positive",
                        "type": "range",
                        "column": "quantity",
                        "min": 1,
                        "max": 10000,
                        "severity": "high"
                    },
                    {
                        "name": "customer_reference",
                        "type": "referential_integrity",
                        "column": "customer_id",
                        "reference_dataset": "customers",
                        "reference_column": "customer_id",
                        "severity": "critical"
                    },
                    {
                        "name": "product_reference",
                        "type": "referential_integrity",
                        "column": "product_id",
                        "reference_dataset": "products",
                        "reference_column": "product_id",
                        "severity": "critical"
                    }
                ]
            }
        }
    }

    config_path = tmp_path / "rules.yaml"
    config_path.write_text(
        yaml.safe_dump(config),
        encoding="utf-8"
    )

    return config_path, tmp_path / "quality.db"


def test_pipeline_rejects_bad_rows_and_publishes_good_rows(tmp_path):
    config, db = setup_project(tmp_path)

    result = DataQualityEngine(str(config), str(db)).run()

    assert result["status"] == "SUCCESS"
    assert result["input_rows"] == 45
    assert result["rejected_rows"] >= 3

    with sqlite3.connect(db) as conn:
        target_count = conn.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

        reject_count = conn.execute(
            "SELECT COUNT(*) FROM rejects WHERE dataset='orders'"
        ).fetchone()[0]

    assert target_count > 0
    assert reject_count >= 3


def test_rerun_is_idempotent_for_target_orders(tmp_path):
    config, db = setup_project(tmp_path)

    engine = DataQualityEngine(str(config), str(db))

    first = engine.run()

    with sqlite3.connect(db) as conn:
        before = conn.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    second = engine.run()

    with sqlite3.connect(db) as conn:
        after = conn.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert after == before
    assert first["status"] == second["status"] == "SUCCESS"


def test_missing_input_fails_clearly(tmp_path):
    config, db = setup_project(tmp_path)

    raw = yaml.safe_load(config.read_text())
    raw["datasets"]["customers"]["file"] = str(
        tmp_path / "missing.csv"
    )

    config.write_text(yaml.safe_dump(raw))

    with pytest.raises(FileNotFoundError):
        DataQualityEngine(str(config), str(db)).run()


def test_completeness_rule_rejects_null_values(tmp_path):
    config, db = setup_project(tmp_path)

    raw = yaml.safe_load(config.read_text(encoding="utf-8"))

    raw["datasets"]["customers"]["rules"].append({
        "name": "customer_id_not_null",
        "type": "completeness",
        "column": "customer_id",
        "severity": "critical"
    })

    config.write_text(
        yaml.safe_dump(raw),
        encoding="utf-8"
    )

    customers_file = (
        tmp_path / "data" / "input" / "customers.csv"
    )

    df = pd.read_csv(customers_file)
    df.loc[0, "customer_id"] = None
    df.to_csv(customers_file, index=False)

    result = DataQualityEngine(str(config), str(db)).run()

    assert result["status"] == "SUCCESS"

    with sqlite3.connect(db) as conn:
        rejected = conn.execute(
            "SELECT COUNT(*) FROM rejects WHERE dataset='customers'"
        ).fetchone()[0]

    assert rejected >= 1


def test_email_format_rule_rejects_invalid_email(tmp_path):
    config, db = setup_project(tmp_path)

    raw = yaml.safe_load(config.read_text(encoding="utf-8"))

    raw["datasets"]["customers"]["rules"].append({
        "name": "email_format",
        "type": "custom",
        "expression": "email",
        "check": "email",
        "severity": "medium"
    })

    config.write_text(
        yaml.safe_dump(raw),
        encoding="utf-8"
    )

    customers_file = (
        tmp_path / "data" / "input" / "customers.csv"
    )

    df = pd.read_csv(customers_file)
    df.loc[0, "email"] = "invalid-email"
    df.to_csv(customers_file, index=False)

    result = DataQualityEngine(str(config), str(db)).run()

    assert result["status"] == "SUCCESS"

    with sqlite3.connect(db) as conn:
        rejected = conn.execute(
            "SELECT COUNT(*) FROM rejects WHERE dataset='customers'"
        ).fetchone()[0]

    assert rejected >= 1


def test_freshness_rule_rejects_old_timestamp(tmp_path):
    config, db = setup_project(tmp_path)

    raw = yaml.safe_load(config.read_text(encoding="utf-8"))

    raw["datasets"]["orders"]["rules"].append({
        "name": "order_freshness",
        "type": "freshness",
        "column": "order_timestamp",
        "max_age_days": 3650,
        "severity": "low"
    })

    config.write_text(yaml.safe_dump(raw), encoding="utf-8")

    orders_file = tmp_path / "data" / "input" / "orders.csv"
    df = pd.read_csv(orders_file)

    df.loc[0, "order_timestamp"] = "2000-01-01T00:00:00"
    df.to_csv(orders_file, index=False)

    result = DataQualityEngine(str(config), str(db)).run()

    assert result["status"] == "SUCCESS"

    with sqlite3.connect(db) as conn:
        rejected = conn.execute(
            "SELECT COUNT(*) FROM rejects WHERE dataset='orders'"
        ).fetchone()[0]

    assert rejected >= 1


def test_quantity_amount_rule_rejects_invalid_order(tmp_path):
    config, db = setup_project(tmp_path)

    raw = yaml.safe_load(config.read_text(encoding="utf-8"))

    raw["datasets"]["orders"]["rules"].append({
        "name": "quantity_amount_business_rule",
        "type": "custom",
        "expression": "quantity_amount",
        "severity": "high"
    })

    config.write_text(yaml.safe_dump(raw), encoding="utf-8")

    orders_file = tmp_path / "data" / "input" / "orders.csv"
    df = pd.read_csv(orders_file)

    df.loc[0, "quantity"] = 100
    df.loc[0, "amount"] = 0.01
    df.to_csv(orders_file, index=False)

    result = DataQualityEngine(str(config), str(db)).run()

    assert result["status"] == "SUCCESS"

    with sqlite3.connect(db) as conn:
        rejected = conn.execute(
            "SELECT COUNT(*) FROM rejects WHERE dataset='orders'"
        ).fetchone()[0]

    assert rejected >= 1
