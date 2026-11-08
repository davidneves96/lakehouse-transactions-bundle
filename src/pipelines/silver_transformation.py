import argparse
import sys
from pathlib import Path
import pyspark.sql.functions as F


# Import the root path of the project to the sys.path to allow for relative imports
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from src.common.spark_session import get_spark_session


def ingest_bronze_to_silver (input_path: str, output_path: str):
    """Ingests bronze data from the specified input path and writes it to the silver layer in Delta format."""
    spark = get_spark_session("SilverIngestion")

    # Read bronze data
    print(f"Reading bronze data from: {input_path}")
    read_df = spark.read.format("delta").load(input_path)

    
    print(f"Number of records read: {read_df.count()}")

    # Transforming the data to silver layer
    print(f"Transforming data to silver layer and writing to: {output_path}")
    silver_df = (
        read_df
        .dropDuplicates(["transaction_id"]) # Drop duplicates based on transaction_id
        .withColumn("created_at", F.to_timestamp(F.col("created_at"))) # Convert created_at to timestamp type
        .withColumn("transaction_date",F.to_date(F.col("created_at"))) # Create a new column transaction_date from created_at, making it easier to query by date
        .withColumn("amount", F.col("amount").cast("decimal(18,2)")) # Converts the amount column to DecimalType with precision 18 and scale 2, ensuring accurate representation of monetary values
        .withColumns({
                    "status" : F.upper(F.col("status")),
                    "currency" : F.upper(F.col("currency"))
                    }) # Standardizes the status and currency columns to uppercase for consistency
        .withColumn("customer_name", F.sha2(F.col("customer_name"), 256)) # Hashes the customer_name column using SHA-2 for privacy and security
        .withColumn("is_valid",                                                             # Adds a new column is_valid to indicate 
                    F.when((F.col("amount") > 0) &                                            # Whether the transaction amount is positive
                           (F.col("currency").isin(["USD", "EUR", "BRL", "CAD"])) &           # Whether the currency is one of the accepted values
                           (F.col("status").isin(["APPROVED", "DECLINED", "FRAUD_ALERT"])) &  # Whether the status is one of the accepted values
                           (F.col("transaction_id").isNotNull()) &                            # Whether the transaction_id is not null
                           (F.col("_ingestion_timestamp").isNotNull())                        # Whether the _ingestion_timestamp is not null
                           , True).otherwise(False)         
                        # Data Engineer commentary: Why not use .when separately for each condition? Using a single .when() with multiple conditions combined using the & operator is more efficient and concise. It allows us to evaluate all the conditions in one go, rather than creating multiple columns and then combining them later. This approach reduces the number of transformations and can improve performance, especially on large datasets.
                    )
    )

    # Write to Delta format in the silver layer
    print(f"Writing data to silver layer at: {output_path}")
    (
        silver_df
        .write
        .format("delta")
        .mode("append") # Use append mode to add new data to the existing Delta table
        .save(output_path) 
    )
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingests bronze data into the silver layer in Delta format.")
    parser.add_argument("--input-path", default="data/bronze", help="Path to the input data")
    parser.add_argument("--output-path", default="data/silver", help="Path to the output data")

    args = parser.parse_args()

# Call the function to ingest bronze data into the silver layer
ingest_bronze_to_silver(
    input_path=args.input_path,
    output_path=args.output_path
)
