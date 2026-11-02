import logging
from pathlib import Path

import duckdb

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")

logger = logging.getLogger(__name__)


def ingest_to_bronze() -> dict:
    """Convert every raw CSV into a Parquet file in the bronze layer.

    Rebuilds from scratch on every run, so it is idempotent.
    Returns {table_name: row_count}.
    """
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR.resolve()}")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    counts = {}

    for csv in csv_files:
        table = csv.stem
        out = BRONZE_DIR / f"{table}.parquet"

        con.execute(f"""
            COPY (
                SELECT *,
                       '{csv.name}'       AS _source_file,
                       current_timestamp  AS _ingested_at
                FROM read_csv('{csv.as_posix()}', header = true, all_varchar = true)
            ) TO '{out.as_posix()}' (FORMAT PARQUET)
        """)

        n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')"
        ).fetchone()[0]
        counts[table] = n
        logger.info("Bronze | %-45s | %9d rows", table, n)

    con.close()
    return counts