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
def aggregate_gold_data(input_path, output_path):
    """Aggregates data from the silver layer and writes it to the gold layer in Delta format."""
    spark = get_spark_session("GoldAggregation")

    read_df = get_spark_session("GoldAggregation").read.format("delta").load(input_path)

    # Filter the data to include only valid transactions
    print(f"Filtering valid transactions from the silver layer data at: {input_path}")

    valid_transactions_df = read_df.filter(F.col("is_valid") == True)

    # Aggregate the data by transaction_date and currency, calculating various metrics
    print(f"Aggregating data to gold layer and writing to: {output_path}")
    agg_df = (
        valid_transactions_df
        .groupBy("transaction_date", "currency")
        .agg(
            F.count("transaction_id").alias("total_transactions"), # Count of total transactions
            F.sum("amount").alias("total_amount"), # Sum of transaction amounts
            F.round(F.avg("amount"), 2).alias("avg_ticket"), # Average transaction amount
            F.min("amount").alias("min_amount"), # Minimum transaction amount
            F.max("amount").alias("max_amount"), # Maximum transaction amount
            F.count_if(F.col("status") == "APPROVED").alias("total_approved"), # Count of approved transactions
            F.count_if(F.col("status") == "DECLINED").alias("total_declined"), # Count of declined transactions
            F.count_if(F.col("status").contains("FRAUD")).alias("total_fraud_alerts"), # Count of fraud alerts
            F.coalesce(F.sum(F.when(F.upper(F.col("status")) == 'APPROVED', F.col("amount"))), F.lit(0.00)).alias("approved_amount"), # Sum of approved transaction amounts
            F.countDistinct("customer_id").alias("unique_customers") # Count of unique customers
        )
        .withColumn("year_month", F.date_format("transaction_date", "yyyy-MM")) # Add a year_month column for easier time-based analysis and partitioning
    )

    # Write the aggregated data to the gold layer in Delta format
    if DeltaTable.isDeltaTable(spark, output_path):
        print(f"Delta table already exists at {output_path}. Merging new aggregated data into existing Delta table.")
        delta_table = DeltaTable.forPath(spark, output_path)

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
        print(f"No existing Delta table found at {output_path}. Creating a new Gold Delta table.")
        (
            agg_df.write.format("delta").mode("overwrite").partitionBy("year_month").save(f"{output_path}/daily_transactions_summary")
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
