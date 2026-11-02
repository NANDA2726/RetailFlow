import json
from pathlib import Path

import duckdb
import pytest

from src import data_quality as dq


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """Run each test inside an empty temp folder so relative paths (data/, outputs/) are isolated."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def make_table(layer, name, select_sql):
    path = Path("data") / layer / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY ({select_sql}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def scalar(sql):
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


# ---------- the SQL helper builders ----------

def test_dups_counts_duplicated_keys(workdir):
    make_table("gold", "t", "SELECT * FROM (VALUES (1), (1), (2), (3), (3), (3)) t(id)")
    assert scalar(dq._dups("gold", "t", ["id"])) == 2      # ids 1 and 3 are duplicated


def test_dups_zero_when_unique(workdir):
    make_table("gold", "t", "SELECT * FROM (VALUES (1), (2), (3)) t(id)")
    assert scalar(dq._dups("gold", "t", ["id"])) == 0


def test_nulls_counts_only_nulls(workdir):
    make_table("gold", "t", "SELECT * FROM (VALUES (1), (NULL), (NULL)) t(x)")
    assert scalar(dq._nulls("gold", "t", "x")) == 2


def test_orphans_finds_missing_dimension_rows_and_ignores_null_fks(workdir):
    make_table("gold", "fact", "SELECT * FROM (VALUES (1), (2), (NULL)) t(dim_key)")
    make_table("gold", "dim", "SELECT * FROM (VALUES (1)) t(dim_key)")
    # key 2 has no dimension row; the NULL is not counted (that is the not-null check's job)
    assert scalar(dq._orphans("fact", "dim_key", "dim", "dim_key")) == 1


def test_count_diff_is_absolute_difference(workdir):
    make_table("silver", "a", "SELECT * FROM range(10) t(i)")
    make_table("gold", "a", "SELECT * FROM range(7) t(i)")
    assert scalar(dq._count_diff(dq._p("silver", "a"), dq._p("gold", "a"))) == 3
    assert scalar(dq._count_diff(dq._p("gold", "a"), dq._p("silver", "a"))) == 3


# ---------- run_quality_checks: the quality gate ----------

def fake_checks(monkeypatch, checks):
    monkeypatch.setattr(dq, "_build_checks", lambda: checks)


def test_all_passing_returns_results_and_writes_report(workdir, monkeypatch):
    fake_checks(monkeypatch, [("a", "error", "SELECT 0"), ("b", "error", "SELECT 0")])

    results = dq.run_quality_checks()

    assert [r["status"] for r in results] == ["PASS", "PASS"]
    report = json.loads((workdir / "outputs" / "dq_report.json").read_text())
    assert report["total"] == 2 and report["passed"] == 2 and report["failed"] == 0


def test_warning_severity_does_not_stop_the_pipeline(workdir, monkeypatch):
    fake_checks(monkeypatch, [("ok", "error", "SELECT 0"), ("quirk", "warn", "SELECT 8")])

    results = dq.run_quality_checks()          # must NOT raise

    by_name = {r["check"]: r for r in results}
    assert by_name["quirk"]["status"] == "WARN"
    assert by_name["quirk"]["violations"] == 8
    report = json.loads((workdir / "outputs" / "dq_report.json").read_text())
    assert report["warnings"] == 1 and report["failed"] == 0


def test_error_severity_failure_raises_and_still_writes_report(workdir, monkeypatch):
    fake_checks(monkeypatch, [
        ("ok", "error", "SELECT 0"),
        ("bad", "error", "SELECT 2"),
        ("quirk", "warn", "SELECT 5"),
    ])

    with pytest.raises(dq.DataQualityError, match="1 data quality check"):
        dq.run_quality_checks()

    # the report is written BEFORE raising, so you can inspect what failed
    report = json.loads((workdir / "outputs" / "dq_report.json").read_text())
    assert report["failed"] == 1 and report["passed"] == 1 and report["warnings"] == 1


def test_revenue_mismatch_is_caught(workdir, monkeypatch):
    """Automated version of the fault injection: gold price doubled vs silver."""
    make_table("silver", "order_items", "SELECT * FROM (VALUES (10.0), (20.0)) t(price)")
    make_table("gold", "fact_order_items", "SELECT * FROM (VALUES (20.0), (40.0)) t(price)")
    sql = (
        f"SELECT CAST(abs((SELECT sum(price) FROM {dq._p('silver', 'order_items')}) - "
        f"(SELECT sum(price) FROM {dq._p('gold', 'fact_order_items')})) > 0.01 AS INTEGER)"
    )
    fake_checks(monkeypatch, [("revenue silver=gold", "error", sql)])

    with pytest.raises(dq.DataQualityError):
        dq.run_quality_checks()