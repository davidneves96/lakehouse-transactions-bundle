from datetime import date
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, BooleanType
from src.pipelines.gold_aggregation import transform_gold_data


def test_transform_gold_data_aggregations_and_filters(spark):
 #   1. Arrange: Creates an input DataFrame with simulated data
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("is_valid", BooleanType(), True),
    ])

    mock_data = [

        ("tx_1", "CUST_1", Decimal("100.00"), "USD", "APPROVED", date(2026, 11, 10), True),
        ("tx_2", "CUST_2", Decimal("50.00"), "USD", "DECLINED", date(2026, 11, 10), True),
        ("tx_3", "CUST_3", Decimal("999.00"), "USD", "APPROVED", date(2026, 11, 10), False),  # Invalid
        # Day 1 - BRL: 1 FRAUD_ALERT transaction (without approved transactions -> approved_amount must be 0.00)
        ("tx_4", "CUST_1", Decimal("200.00"), "BRL", "FRAUD_ALERT", date(2026, 11, 10), True),
    ]

    input_df = spark.createDataFrame(mock_data, schema)

    # 2. Act: Executes the pure transformation
    result_df = transform_gold_data(input_df)
    results = {row["currency"]: row for row in result_df.collect()}

    # 3. Assert: Validates the business calculations
    assert len(results) == 2

    # USD validation (ignored tx_3)
    usd = results["USD"]
    assert usd["total_transactions"] == 2
    assert usd["total_amount"] == Decimal("150.00")
    assert usd["avg_ticket"] == Decimal("75.00")
    assert usd["total_approved"] == 1
    assert usd["total_declined"] == 1
    assert usd["approved_amount"] == Decimal("100.00")
    assert usd["unique_customers"] == 2
    assert usd["year_month"] == "2026-11"

    # BRL validation (no approved transactions -> testing coalesce to 0.00)
    brl = results["BRL"]
    assert brl["total_transactions"] == 1
    assert brl["total_fraud_alerts"] == 1
    assert brl["approved_amount"] == Decimal("0.00")