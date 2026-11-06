import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
import pyspark.sql.functions as F

# Import the root path of the project to the sys.path to allow for relative imports
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from src.common.spark_session import get_spark_session

def ingest_raw_to_bronze(input_path: str, output_path: str):
    """Ingests raw data from the specified input path and writes it to the bronze layer in Delta format."""
    spark = get_spark_session("BronzeIngestion")

    # Read raw data
    print(f"Reading raw data from: {input_path}")
    raw_df = spark.read.format("json").load(input_path)

    record_count = raw_df.count()
    print(f"Number of records read: {record_count}")

    # Add ingestion timestamp and source file name columns
    bronze_df = (
        raw_df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Write to Delta format in the bronze layer 
    print(f"Writing data to bronze layer at: {output_path}")
    (
        bronze_df
        .write.format("delta")
        .mode("append") # Use append mode to add new data to the existing Delta table
        .save(output_path)
    )

parser = argparse.ArgumentParser(description="Ingests raw data into the bronze layer in Delta format.")
parser.add_argument("--input-path", default="data/raw", help="Path to the input data")
parser.add_argument("--output-path", default="data/bronze", help="Path to the output data")

args = parser.parse_args()

ingest_raw_to_bronze(
    input_path=args.input_path,
    output_path=args.output_path
)