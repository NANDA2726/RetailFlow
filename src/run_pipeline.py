import logging
import sys
from pathlib import Path
from src.transformation import transform_to_silver
from src.ingestion import ingest_to_bronze
from src.warehouse import build_gold
from src.data_quality import run_quality_checks
from src.analytics import run_analytics

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")


def main():
    logger.info("Pipeline started")
    bronze_counts = ingest_to_bronze()
    logger.info("Bronze complete: %d tables", len(bronze_counts))

    silver_results = transform_to_silver()
    logger.info("Silver complete: %d tables", len(silver_results))

    gold_counts = build_gold()
    logger.info("Gold complete: %d tables", len(gold_counts))

    run_quality_checks()
    logger.info("Pipeline finished successfully")
    analytics_results = run_analytics()
    logger.info("Analytics complete: %d queries", len(analytics_results))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)