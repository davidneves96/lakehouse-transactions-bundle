import argparse
import sys
from pathlib import Path
import pyspark.sql.functions as F
from delta.tables import DeltaTable


# Import the root path of the project to the sys.path to allow for relative imports
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from src.common.spark_session import get_spark_session

# Function to aggregate gold data
def transform_gold_data(silver_df: F.DataFrame) -> F.DataFrame:
    """Transforms the silver layer DataFrame into an aggregated gold layer DataFrame."""
    # Filter the data to include only valid transactions
    print(f"Filtering valid transactions from silver data")
    valid_transactions_df = silver_df.filter(F.col("is_valid") == True)

    return(
        valid_transactions_df
        .groupBy("transaction_date", "currency")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_ticket"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.count_if(F.col("status") == "APPROVED").alias("total_approved"),
            F.count_if(F.col("status") == "DECLINED").alias("total_declined"),
            F.count_if(F.col("status").contains("FRAUD")).alias("total_fraud_alerts"),
            F.coalesce(F.sum(F.when(F.upper(F.col("status")) == 'APPROVED', F.col("amount"))), F.lit(0)).cast("decimal(18,2)").alias("approved_amount"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("year_month", F.date_format("transaction_date", "yyyy-MM"))
    )

# Function to orchestrate I/O operations
def aggregate_gold_data(input_path: str, output_path: str):
    """Aggregates silver data from the specified input path and writes it to the gold layer in Delta format."""
    spark = get_spark_session("GoldAggregation")

    # Read silver data
    print(f"Reading silver data from: {input_path}")
    read_df = spark.read.format("delta").load(input_path)

    agg_df = transform_gold_data(read_df)

    target_path = f"{output_path}/daily_transactions_summary"    
    # Write the aggregated data to the gold layer in Delta format
    if DeltaTable.isDeltaTable(spark, target_path):
        print(f"Delta table already exists at {target_path}. Merging new aggregated data into existing Delta table.")
        delta_table = DeltaTable.forPath(spark, target_path)

        merge_condition = (
            "(target.transaction_date = source.transaction_date) AND "
            "(target.currency = source.currency)"
        )

        (
            delta_table.alias("target")
            .merge(
                source = agg_df.alias("source"),
                condition = merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        print(f"No existing Delta table found at {target_path}. Creating a new Gold Delta table.")
        (
            agg_df.write.format("delta").mode("overwrite").partitionBy("year_month").save(f"{target_path}")
        )   
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregates gold data in Delta format.")
    parser.add_argument("--input-path", default="data/silver", help="Path to the input data")
    parser.add_argument("--output-path", default="data/gold", help="Path to the output data")

    args = parser.parse_args()

    # Call the function to aggregate gold data
    aggregate_gold_data(
        input_path=args.input_path,
        output_path=args.output_path
    )
