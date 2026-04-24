# ================================================================
# run.py — Module 03 Entry Point
# ================================================================
# python run.py
#
# WHAT HAPPENS:
#   1. Demo SQL concepts (basics, aggregation, joins) in the terminal
#   2. Run the full extraction query (05_extract_raw_data.sql)
#   3. Save raw-data.csv to data/
# ================================================================

import sys, pathlib
_root = pathlib.Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import INDUSTRY, DB_AVAILABLE, logger
from src.query_runner  import SQLQueryRunner
from src.data_extractor import DataExtractor


def main() -> None:
    logger.info("=" * 60)
    logger.info(f"  MODULE 03 — SQL AND POSTGRESQL")
    logger.info(f"  Industry: {INDUSTRY}")
    logger.info(f"  DB Available: {DB_AVAILABLE}")
    logger.info("=" * 60)

    # ── PART 1: SQL Demonstrations ──────────────────────────────────
    # These run during the teaching session to show SQL concepts live.
    runner = SQLQueryRunner()

    print("\n── DEMO 1: Basic SELECT and WHERE")
    runner.demo_basics()

    print("\n── DEMO 2: GROUP BY and Aggregation")
    runner.demo_aggregation()

    print("\n── DEMO 3: JOIN — patients with appoitments")
    runner.demo_joins()

    # ── PART 2: Production Extraction ──────────────────────────────
    # This is the REAL deliverable — produces raw-data.csv for Module 05
    logger.info("\n[EXTRACT] Starting production extraction...")
    extractor = DataExtractor()
    extractor.extract().save().report()


if __name__ == "__main__":
    main()
