import argparse
import logging
import sys
import time
from pathlib import Path

# Ensures the root path of the project is in sys.path to allow for relative imports
root_path = Path(__file__).resolve().parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from src.pipelines.bronze_ingestion import ingest_raw_to_bronze
from src.pipelines.silver_transformation import ingest_bronze_to_silver
from src.pipelines.gold_aggregation import aggregate_gold_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
log = logging.getLogger("LakehouseOrchestrator")

# Executes a task with logging and timing
def execute_task(task_name: str, action, **params):
    log.info(f"Starting: {task_name}")
    start = time.perf_counter()
    try:
        action(**params)
        duration = time.perf_counter() - start
        log.info(f"Finished {task_name} in {duration:.2f}s")
    except Exception as err:
        log.erro(f"Error in {task_name}: {err}, exec_info=True")
        sys.exit(1)

# Main function to orchestrate the pipeline
def main():
    parser = argparse.ArgumentParser(description="Medalion Pipeline Orquestrator (Bronze -> Silver -> Gold)")
    parser.add_argument(
        "--layer",
        choices=["all","bronze","silver","gold"],
        default="all",
        help="Especific layer for execution (default: all)"
    )
    parser.add_argument("--raw-path", default="data/raw", help="Path to the raw data (Bronze entrance)")
    parser.add_argument("--bronze-path", default="data/bronze", help="Path to the Bronze layer")
    parser.add_argument("--silver-path", default="data/silver", help="Path to the Silver layer")
    parser.add_argument("--gold-path", default="data/gold", help="Path to the Gold layer")

    args = parser.parse_args()

    pipeline_start = time.perf_counter()
    log.info(f"=== Starting the execution of the Lakehouse Pipeline (mode: {args.layer.upper()}) ===")


    if args.layer in ("all", "bronze"):
        execute_task(
            "Bronze Ingestion",
            ingest_raw_to_bronze,
            input_path=args.raw_path,
            output_path=args.bronze_path
        )

    if args.layer in ("all", "silver"):
        execute_task(
            "Silver Transformation",
            ingest_bronze_to_silver,
            input_path=args.bronze_path,
            output_path=args.silver_path
        )

    if args.layer in ("all", "gold"):
        execute_task(
            "Gold Aggregation",
            aggregate_gold_data,
            input_path=args.silver_path,
            output_path=args.gold_path
        )

    total_elapsed = time.perf_counter() - pipeline_start
    log.info(f"=== Pipeline ended sucessfully in {total_elapsed:.2f}s ===")  

if __name__ == "__main__":
    main()
