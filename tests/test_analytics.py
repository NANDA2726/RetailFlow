from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src import analytics

ROOT = Path(__file__).resolve().parent.parent


def _write(con, path, select_sql):
    con.execute(f"COPY ({select_sql}) TO '{path.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture(scope="module")
def results(tmp_path_factory):
    """Build a tiny fake gold layer, run all 5 queries, return {name: DataFrame}."""
    base = tmp_path_factory.mktemp("analytics")
    gold, out = base / "gold", base / "out"
    gold.mkdir()
    con = duckdb.connect()

    _write(con, gold / "dim_date.parquet", """
        SELECT * FROM (VALUES (20170115, 2017, 1), (20170120, 2017, 1),
                              (20170210, 2017, 2), (20170220, 2017, 2))
        t(date_key, year, month)""")

    # c1 and c3 are the SAME person (u1) with different per-order customer_ids
    _write(con, gold / "dim_customer.parquet", """
        SELECT * FROM (VALUES (1, 'u1', 'SP'), (2, 'u2', 'RJ'),
                              (3, 'u1', 'SP'), (4, 'u3', 'MG'))
        t(customer_key, customer_unique_id, state)""")

    _write(con, gold / "dim_product.parquet", """
        SELECT * FROM (VALUES (1, 'P1', 'toys'), (2, 'P2', 'toys'), (3, 'P3', 'toys'),
                              (4, 'P4', 'toys'), (5, 'P5', 'toys'))
        t(product_key, product_id, category)""")

    _write(con, gold / "dim_seller.parquet", """
        SELECT * FROM (VALUES (1, 'S1', 'SP')) t(seller_key, seller_id, state)""")

    # o4 is canceled with a huge price: it must never show up in results
    _write(con, gold / "fact_order_items.parquet", """
        SELECT order_id, order_item_id, customer_key, product_key, seller_key,
               price, order_status, purchase_date_key,
               CAST(order_purchase_ts AS TIMESTAMP) AS order_purchase_ts,
               is_late
        FROM (VALUES
            ('o1', 1, 1, 1, 1, 100.0, 'delivered', 20170115, '2017-01-15 10:00:00', false),
            ('o2', 1, 2, 2, 1,  50.0, 'delivered', 20170210, '2017-02-10 10:00:00', true),
            ('o3', 1, 3, 3, 1,  30.0, 'delivered', 20170220, '2017-02-20 10:00:00', false),
            ('o4', 1, 2, 4, 1, 999.0, 'canceled',  20170115, '2017-01-15 12:00:00', NULL),
            ('o5', 1, 4, 5, 1,  10.0, 'delivered', 20170120, '2017-01-20 10:00:00', false)
        ) t(order_id, order_item_id, customer_key, product_key, seller_key,
            price, order_status, purchase_date_key, order_purchase_ts, is_late)""")

    _write(con, gold / "fact_reviews.parquet", """
        SELECT * FROM (VALUES ('o1', 5), ('o2', 3)) t(order_id, review_score)""")
    con.close()

    # point the runner at the fake data, but use the real SQL files
    mp = pytest.MonkeyPatch()
    mp.setattr(analytics, "GOLD_DIR", gold)
    mp.setattr(analytics, "OUT_DIR", out)
    mp.setattr(analytics, "SQL_DIR", ROOT / "sql")
    counts = analytics.run_analytics()
    mp.undo()

    frames = {p.stem: pd.read_csv(p) for p in out.glob("*.csv")}
    frames["_counts"] = counts
    return frames


def test_all_five_queries_run(results):
    assert set(results["_counts"]) == {
        "customer_retention", "monthly_revenue", "revenue_by_state",
        "seller_performance", "top_products_per_category",
    }


def test_monthly_revenue_excludes_canceled_and_computes_growth(results):
    df = results["monthly_revenue"].set_index("month_start")
    assert df.loc["2017-01-01", "revenue"] == pytest.approx(110.0)   # not 1109
    assert df.loc["2017-02-01", "revenue"] == pytest.approx(80.0)
    assert df.loc["2017-02-01", "orders"] == 2
    assert df.loc["2017-02-01", "revenue_3m_avg"] == pytest.approx(95.0)
    assert df.loc["2017-02-01", "mom_growth_pct"] == pytest.approx(-27.3)


def test_revenue_by_state_shares(results):
    df = results["revenue_by_state"].set_index("state")
    assert df.loc["SP", "revenue"] == pytest.approx(130.0)
    assert df.loc["SP", "revenue_share_pct"] == pytest.approx(68.42)
    assert "RJ" in df.index and df.loc["RJ", "revenue"] == pytest.approx(50.0)  # o4 excluded
    assert df["revenue_share_pct"].sum() == pytest.approx(100.0, abs=0.05)


def test_retention_uses_unique_customer_not_per_order_id(results):
    df = results["customer_retention"]
    jan = df[df["cohort_month"].str.startswith("2017-01")].set_index("month_offset")
    # Jan cohort = u1 and u3. u1 comes back in Feb, so month 1 retention is 50%.
    # Using customer_id instead of customer_unique_id would give 3 customers.
    assert jan.loc[0, "cohort_customers"] == 2
    assert jan.loc[0, "retention_pct"] == pytest.approx(100.0)
    assert jan.loc[1, "active_customers"] == 1
    assert jan.loc[1, "retention_pct"] == pytest.approx(50.0)


def test_top_products_capped_at_three_and_canceled_excluded(results):
    df = results["top_products_per_category"]
    assert len(df) == 3
    assert list(df["product_id"]) == ["P1", "P2", "P3"]
    assert list(df["category_rank"]) == [1, 2, 3]
    assert "P4" not in set(df["product_id"])        # canceled, would have ranked first


def test_seller_performance_applies_min_30_orders(results):
    # our fake seller has only 4 delivered orders, so HAVING COUNT(*) >= 30 drops it
    assert len(results["seller_performance"]) == 0