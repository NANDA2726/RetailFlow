import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

REPORT_PATH = Path("outputs/dq_report.json")


class DataQualityError(Exception):
    pass


def _p(layer: str, name: str) -> str:
    return f"read_parquet('{(Path('data') / layer / f'{name}.parquet').as_posix()}')"


def _count_diff(a: str, b: str) -> str:
    """Violations = absolute difference in row counts between two tables."""
    return f"SELECT abs((SELECT count(*) FROM {a}) - (SELECT count(*) FROM {b}))"


def _dups(layer: str, name: str, cols: list) -> str:
    c = ", ".join(cols)
    return (
        f"SELECT count(*) FROM "
        f"(SELECT {c} FROM {_p(layer, name)} GROUP BY {c} HAVING count(*) > 1)"
    )


def _nulls(layer: str, name: str, col: str) -> str:
    return f"SELECT count(*) FROM {_p(layer, name)} WHERE {col} IS NULL"


def _orphans(fact: str, fk: str, dim: str, dk: str) -> str:
    return (
        f"SELECT count(*) FROM {_p('gold', fact)} f "
        f"LEFT JOIN {_p('gold', dim)} d ON f.{fk} = d.{dk} "
        f"WHERE f.{fk} IS NOT NULL AND d.{dk} IS NULL"
    )


def _build_checks() -> list:
    """Each check is (name, severity, SQL returning the number of violations; 0 = pass)."""
    checks = []

    # 1. Bronze -> Silver reconciliation (tables where no rows should be dropped)
    bronze_names = {
        "customers": "olist_customers_dataset",
        "orders": "olist_orders_dataset",
        "order_items": "olist_order_items_dataset",
        "order_payments": "olist_order_payments_dataset",
        "products": "olist_products_dataset",
        "sellers": "olist_sellers_dataset",
    }
    for s_name, b_name in bronze_names.items():
        checks.append((
            f"rowcount bronze=silver: {s_name}", "error",
            _count_diff(_p("bronze", b_name), _p("silver", s_name)),
        ))

    # 2. Silver -> Gold reconciliation
    checks += [
        ("rowcount silver=gold: fact_order_items", "error",
         _count_diff(_p("silver", "order_items"), _p("gold", "fact_order_items"))),
        ("rowcount silver=gold: fact_payments", "error",
         _count_diff(_p("silver", "order_payments"), _p("gold", "fact_payments"))),
        ("rowcount silver=gold: fact_reviews", "error",
         _count_diff(_p("silver", "order_reviews"), _p("gold", "fact_reviews"))),
        ("rowcount silver=gold: dim_customer", "error",
         _count_diff(_p("silver", "customers"), _p("gold", "dim_customer"))),
        ("rowcount silver=gold: dim_product", "error",
         _count_diff(_p("silver", "products"), _p("gold", "dim_product"))),
        ("rowcount silver=gold: dim_seller", "error",
         _count_diff(_p("silver", "sellers"), _p("gold", "dim_seller"))),
    ]

    # 3. Uniqueness of primary / grain keys
    checks += [
        ("unique: silver.orders(order_id)", "error", _dups("silver", "orders", ["order_id"])),
        ("unique: dim_customer(customer_key)", "error", _dups("gold", "dim_customer", ["customer_key"])),
        ("unique: dim_product(product_key)", "error", _dups("gold", "dim_product", ["product_key"])),
        ("unique: dim_seller(seller_key)", "error", _dups("gold", "dim_seller", ["seller_key"])),
        ("unique: dim_date(date_key)", "error", _dups("gold", "dim_date", ["date_key"])),
        ("unique grain: fact_order_items(order_id, order_item_id)", "error",
         _dups("gold", "fact_order_items", ["order_id", "order_item_id"])),
        ("unique grain: fact_payments(order_id, payment_sequential)", "error",
         _dups("gold", "fact_payments", ["order_id", "payment_sequential"])),
        ("unique grain: fact_reviews(review_id)", "error",
         _dups("gold", "fact_reviews", ["review_id"])),
    ]

    # 4. Completeness of foreign keys in facts
    for col in ["customer_key", "product_key", "seller_key", "purchase_date_key"]:
        checks.append((f"not null: fact_order_items.{col}", "error",
                       _nulls("gold", "fact_order_items", col)))

    # 5. Referential integrity (no orphan foreign keys)
    checks += [
        ("fk: fact_order_items.customer_key -> dim_customer", "error",
         _orphans("fact_order_items", "customer_key", "dim_customer", "customer_key")),
        ("fk: fact_order_items.product_key -> dim_product", "error",
         _orphans("fact_order_items", "product_key", "dim_product", "product_key")),
        ("fk: fact_order_items.seller_key -> dim_seller", "error",
         _orphans("fact_order_items", "seller_key", "dim_seller", "seller_key")),
        ("fk: fact_order_items.purchase_date_key -> dim_date", "error",
         _orphans("fact_order_items", "purchase_date_key", "dim_date", "date_key")),
        ("fk: fact_payments.customer_key -> dim_customer", "error",
         _orphans("fact_payments", "customer_key", "dim_customer", "customer_key")),
    ]

    # 6. Business rules
    checks += [
        ("range: price >= 0", "error",
         f"SELECT count(*) FROM {_p('gold', 'fact_order_items')} WHERE price < 0"),
        ("range: freight_value >= 0", "error",
         f"SELECT count(*) FROM {_p('gold', 'fact_order_items')} WHERE freight_value < 0"),
        ("range: payment_value >= 0", "error",
         f"SELECT count(*) FROM {_p('gold', 'fact_payments')} WHERE payment_value < 0"),
        ("range: review_score between 1 and 5", "error",
         f"SELECT count(*) FROM {_p('gold', 'fact_reviews')} "
         f"WHERE review_score IS NULL OR review_score NOT BETWEEN 1 AND 5"),
    ]

    # 7. Financial reconciliation (cents-level tolerance)
    checks += [
        ("revenue silver=gold: sum(price)", "error",
         f"SELECT CAST(abs((SELECT sum(price) FROM {_p('silver', 'order_items')}) - "
         f"(SELECT sum(price) FROM {_p('gold', 'fact_order_items')})) > 0.01 AS INTEGER)"),
        ("payments silver=gold: sum(payment_value)", "error",
         f"SELECT CAST(abs((SELECT sum(payment_value) FROM {_p('silver', 'order_payments')}) - "
         f"(SELECT sum(payment_value) FROM {_p('gold', 'fact_payments')})) > 0.01 AS INTEGER)"),
    ]

    # 8. Known source-data quirks: warnings, not failures
    checks += [
        ("quirk: delivered orders with no delivery timestamp", "warn",
         f"SELECT count(*) FROM {_p('silver', 'orders')} "
         f"WHERE order_status = 'delivered' AND order_delivered_customer_ts IS NULL"),
        ("quirk: orders with no line items", "warn",
         f"SELECT count(*) FROM {_p('silver', 'orders')} o "
         f"WHERE NOT EXISTS (SELECT 1 FROM {_p('silver', 'order_items')} i "
         f"WHERE i.order_id = o.order_id)"),
    ]

    return checks


def run_quality_checks() -> list:
    con = duckdb.connect()
    results = []

    for name, severity, sql in _build_checks():
        violations = con.execute(sql).fetchone()[0] or 0
        if violations == 0:
            status = "PASS"
        else:
            status = "FAIL" if severity == "error" else "WARN"
        results.append({
            "check": name,
            "severity": severity,
            "status": status,
            "violations": int(violations),
        })
        level = logging.INFO if status == "PASS" else (
            logging.ERROR if status == "FAIL" else logging.WARNING)
        logger.log(level, "DQ | %-4s | %-60s | violations: %d", status, name, violations)

    con.close()

    Path("outputs").mkdir(exist_ok=True)
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "total": len(results),
        "passed": sum(r["status"] == "PASS" for r in results),
        "warnings": sum(r["status"] == "WARN" for r in results),
        "failed": sum(r["status"] == "FAIL" for r in results),
        "results": results,
    }
    REPORT_PATH.write_text(json.dumps(summary, indent=2))
    logger.info(
        "DQ summary | total: %d | passed: %d | warnings: %d | failed: %d",
        summary["total"], summary["passed"], summary["warnings"], summary["failed"],
    )

    if summary["failed"] > 0:
        raise DataQualityError(f"{summary['failed']} data quality check(s) failed")

    return results