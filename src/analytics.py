import logging
from pathlib import Path

import duckdb

GOLD_DIR = Path("data/gold")
SQL_DIR = Path("sql")
OUT_DIR = Path("outputs/analytics")

logger = logging.getLogger(__name__)


def run_analytics() -> dict:
    """Run every .sql file in sql/ against the gold layer and save results as CSV."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # Expose each gold parquet file as a view named after the file
    for p in GOLD_DIR.glob("*.parquet"):
        con.execute(f"CREATE VIEW {p.stem} AS SELECT * FROM read_parquet('{p.as_posix()}')")

    results = {}
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        df = con.execute(sql_file.read_text(encoding="utf-8")).df()
        df.to_csv(OUT_DIR / f"{sql_file.stem}.csv", index=False)
        results[sql_file.stem] = len(df)
        logger.info("Analytics | %-28s | %5d rows", sql_file.stem, len(df))
        logger.info("\n%s", df.head(5).to_string(index=False))

    con.close()
    return results