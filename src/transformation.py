import logging
from pathlib import Path

import duckdb

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

logger = logging.getLogger(__name__)


def bronze(table: str) -> str:
    path = (BRONZE_DIR / f"{table}.parquet").as_posix()
    return f"read_parquet('{path}')"


# silver_table_name: (bronze_source_table, SQL using {src})
SILVER_TABLES = {
    "customers": (
        "olist_customers_dataset",
        """
        SELECT customer_id,
               customer_unique_id,
               customer_zip_code_prefix,
               lower(trim(customer_city))  AS customer_city,
               upper(trim(customer_state)) AS customer_state
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
        """,
    ),
    "orders": (
        "olist_orders_dataset",
        """
        SELECT order_id,
               customer_id,
               lower(trim(order_status)) AS order_status,
               TRY_CAST(order_purchase_timestamp      AS TIMESTAMP) AS order_purchase_ts,
               TRY_CAST(order_approved_at             AS TIMESTAMP) AS order_approved_ts,
               TRY_CAST(order_delivered_carrier_date  AS TIMESTAMP) AS order_delivered_carrier_ts,
               TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
               TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY order_id) = 1
        """,
    ),
    "order_items": (
        "olist_order_items_dataset",
        """
        SELECT order_id,
               TRY_CAST(order_item_id AS INTEGER)       AS order_item_id,
               product_id,
               seller_id,
               TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
               TRY_CAST(price         AS DECIMAL(10,2)) AS price,
               TRY_CAST(freight_value AS DECIMAL(10,2)) AS freight_value
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY order_id, order_item_id ORDER BY order_id) = 1
        """,
    ),
    "order_payments": (
        "olist_order_payments_dataset",
        """
        SELECT order_id,
               TRY_CAST(payment_sequential   AS INTEGER)       AS payment_sequential,
               lower(trim(payment_type))                       AS payment_type,
               TRY_CAST(payment_installments AS INTEGER)       AS payment_installments,
               TRY_CAST(payment_value        AS DECIMAL(10,2)) AS payment_value
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY order_id, payment_sequential ORDER BY order_id) = 1
        """,
    ),
    "order_reviews": (
        "olist_order_reviews_dataset",
        """
        SELECT review_id,
               order_id,
               TRY_CAST(review_score AS INTEGER)               AS review_score,
               NULLIF(trim(review_comment_title), '')          AS review_comment_title,
               NULLIF(trim(review_comment_message), '')        AS review_comment_message,
               TRY_CAST(review_creation_date    AS TIMESTAMP)  AS review_creation_ts,
               TRY_CAST(review_answer_timestamp AS TIMESTAMP)  AS review_answer_ts
        FROM {src}
        QUALIFY row_number() OVER (
            PARTITION BY review_id
            ORDER BY TRY_CAST(review_answer_timestamp AS TIMESTAMP) DESC
        ) = 1
        """,
    ),
    "products": (
        "olist_products_dataset",
        """
        SELECT p.product_id,
               p.product_category_name,
               COALESCE(t.product_category_name_english,
                        p.product_category_name,
                        'unknown')                                  AS category_english,
               TRY_CAST(p.product_name_lenght        AS INTEGER)    AS product_name_length,
               TRY_CAST(p.product_description_lenght AS INTEGER)    AS product_description_length,
               TRY_CAST(p.product_photos_qty         AS INTEGER)    AS product_photos_qty,
               TRY_CAST(p.product_weight_g           AS INTEGER)    AS product_weight_g,
               TRY_CAST(p.product_length_cm          AS INTEGER)    AS product_length_cm,
               TRY_CAST(p.product_height_cm          AS INTEGER)    AS product_height_cm,
               TRY_CAST(p.product_width_cm           AS INTEGER)    AS product_width_cm
        FROM {src} p
        LEFT JOIN {translation} t
               ON p.product_category_name = t.product_category_name
        QUALIFY row_number() OVER (PARTITION BY p.product_id ORDER BY p.product_id) = 1
        """,
    ),
    "sellers": (
        "olist_sellers_dataset",
        """
        SELECT seller_id,
               seller_zip_code_prefix,
               lower(trim(seller_city))  AS seller_city,
               upper(trim(seller_state)) AS seller_state
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY seller_id ORDER BY seller_id) = 1
        """,
    ),
    "geolocation": (
        "olist_geolocation_dataset",
        """
        SELECT geolocation_zip_code_prefix                         AS zip_code_prefix,
               avg(TRY_CAST(geolocation_lat AS DOUBLE))            AS lat,
               avg(TRY_CAST(geolocation_lng AS DOUBLE))            AS lng,
               mode(lower(trim(geolocation_city)))                 AS city,
               mode(upper(trim(geolocation_state)))                AS state
        FROM {src}
        GROUP BY geolocation_zip_code_prefix
        """,
    ),
}


def transform_to_silver() -> dict:
    """Clean and type every bronze table into silver. Rebuilt from scratch each run."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    results = {}

    for name, (source, sql) in SILVER_TABLES.items():
        query = sql.format(
            src=bronze(source),
            translation=bronze("product_category_name_translation"),
        )
        out = SILVER_DIR / f"{name}.parquet"
        con.execute(f"COPY ({query}) TO '{out.as_posix()}' (FORMAT PARQUET)")

        rows_in = con.execute(f"SELECT COUNT(*) FROM {bronze(source)}").fetchone()[0]
        rows_out = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')"
        ).fetchone()[0]
        results[name] = {"rows_in": rows_in, "rows_out": rows_out}
        logger.info(
            "Silver | %-15s | in: %9d | out: %9d | removed: %9d",
            name, rows_in, rows_out, rows_in - rows_out,
        )

    con.close()
    return results