import logging
from pathlib import Path

import duckdb

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")

logger = logging.getLogger(__name__)


def silver(name: str) -> str:
    return f"read_parquet('{(SILVER_DIR / f'{name}.parquet').as_posix()}')"


def gold(name: str) -> str:
    return f"read_parquet('{(GOLD_DIR / f'{name}.parquet').as_posix()}')"


def _write(con, name: str, query: str) -> int:
    out = GOLD_DIR / f"{name}.parquet"
    con.execute(f"COPY ({query}) TO '{out.as_posix()}' (FORMAT PARQUET)")
    n = con.execute(f"SELECT COUNT(*) FROM {gold(name)}").fetchone()[0]
    logger.info("Gold   | %-18s | %9d rows", name, n)
    return n


def build_gold() -> dict:
    """Build the star schema from silver. Rebuilt from scratch each run."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    counts = {}

    # ---------- Dimensions (surrogate keys via row_number) ----------
    counts["dim_customer"] = _write(con, "dim_customer", f"""
        SELECT row_number() OVER (ORDER BY c.customer_id) AS customer_key,
               c.customer_id,
               c.customer_unique_id,
               c.customer_zip_code_prefix AS zip_code_prefix,
               c.customer_city            AS city,
               c.customer_state           AS state,
               g.lat,
               g.lng
        FROM {silver('customers')} c
        LEFT JOIN {silver('geolocation')} g
               ON c.customer_zip_code_prefix = g.zip_code_prefix
    """)

    counts["dim_product"] = _write(con, "dim_product", f"""
        SELECT row_number() OVER (ORDER BY product_id) AS product_key,
               product_id,
               category_english AS category,
               product_name_length,
               product_description_length,
               product_photos_qty,
               product_weight_g,
               product_length_cm,
               product_height_cm,
               product_width_cm
        FROM {silver('products')}
    """)

    counts["dim_seller"] = _write(con, "dim_seller", f"""
        SELECT row_number() OVER (ORDER BY s.seller_id) AS seller_key,
               s.seller_id,
               s.seller_zip_code_prefix AS zip_code_prefix,
               s.seller_city            AS city,
               s.seller_state           AS state,
               g.lat,
               g.lng
        FROM {silver('sellers')} s
        LEFT JOIN {silver('geolocation')} g
               ON s.seller_zip_code_prefix = g.zip_code_prefix
    """)

    lo, hi = con.execute(f"""
        SELECT CAST(min(order_purchase_ts) AS DATE),
               CAST(max(order_purchase_ts) AS DATE)
        FROM {silver('orders')}
    """).fetchone()

    counts["dim_date"] = _write(con, "dim_date", f"""
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
               CAST(d AS DATE)                        AS full_date,
               year(d)                                AS year,
               quarter(d)                             AS quarter,
               month(d)                               AS month,
               monthname(d)                           AS month_name,
               day(d)                                 AS day,
               isodow(d)                              AS day_of_week,
               dayname(d)                             AS day_name,
               isodow(d) >= 6                         AS is_weekend
        FROM generate_series(DATE '{lo}', DATE '{hi}', INTERVAL 1 DAY) AS t(d)
    """)

    # ---------- Facts ----------
    counts["fact_order_items"] = _write(con, "fact_order_items", f"""
        SELECT oi.order_id,
               oi.order_item_id,
               dc.customer_key,
               dp.product_key,
               ds.seller_key,
               CAST(strftime(o.order_purchase_ts, '%Y%m%d') AS INTEGER) AS purchase_date_key,
               o.order_status,
               oi.price,
               oi.freight_value,
               oi.price + oi.freight_value                              AS item_total,
               o.order_purchase_ts,
               o.order_delivered_customer_ts,
               o.order_estimated_delivery_ts,
               date_diff('day', o.order_purchase_ts, o.order_delivered_customer_ts)
                                                                        AS delivery_days,
               CASE WHEN o.order_delivered_customer_ts IS NULL THEN NULL
                    ELSE o.order_delivered_customer_ts > o.order_estimated_delivery_ts
               END                                                      AS is_late
        FROM {silver('order_items')} oi
        JOIN {silver('orders')} o         ON oi.order_id = o.order_id
        LEFT JOIN {gold('dim_customer')} dc ON o.customer_id = dc.customer_id
        LEFT JOIN {gold('dim_product')}  dp ON oi.product_id = dp.product_id
        LEFT JOIN {gold('dim_seller')}   ds ON oi.seller_id   = ds.seller_id
    """)

    counts["fact_payments"] = _write(con, "fact_payments", f"""
        SELECT p.order_id,
               p.payment_sequential,
               p.payment_type,
               p.payment_installments,
               p.payment_value,
               dc.customer_key,
               CAST(strftime(o.order_purchase_ts, '%Y%m%d') AS INTEGER) AS purchase_date_key
        FROM {silver('order_payments')} p
        JOIN {silver('orders')} o         ON p.order_id = o.order_id
        LEFT JOIN {gold('dim_customer')} dc ON o.customer_id = dc.customer_id
    """)

    counts["fact_reviews"] = _write(con, "fact_reviews", f"""
        SELECT r.review_id,
               r.order_id,
               r.review_score,
               r.review_comment_message IS NOT NULL                     AS has_comment,
               CAST(strftime(r.review_creation_ts, '%Y%m%d') AS INTEGER) AS review_date_key
        FROM {silver('order_reviews')} r
    """)

    con.close()
    return counts