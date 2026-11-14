import hashlib
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from src.pipelines.silver_transformation import transform_silver_data


def test_transform_silver_data_cleaning_validation_and_pii(spark):
    # 1. Arrange: Build scenarios covering duplicates, spaces/lowercase, PII, and invalid records
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_job", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
    ])

    ingestion_ts = datetime(2026, 11, 13, 10, 0, 0)

    mock_data = [
        # Record 1: Valid, but with spaces and lowercase in status/currency
        ("tx_1", "CUST_1", "Jhon Wick", 150.50, " brl ", "CREDIT_CARD", " approved ", "Data Engineer", "2026-11-10T14:30:00Z", ingestion_ts),
        # Record 2: Exact duplicate of tx_1 (must be removed by dropDuplicates)
        ("tx_1", "CUST_1", "Jhon Wick", 150.50, " brl ", "CREDIT_CARD", " approved ", "Data Engineer", "2026-11-10T14:30:00Z", ingestion_ts),
        # Record 3: Invalid because amount is negative
        ("tx_2", "CUST_2", "Jax Teller", -20.00, "USD", "DEBIT_CARD", "APPROVED", "Artist", "2026-11-10T15:00:00Z", ingestion_ts),
        # Record 4: Invalid because currency is not supported (GBP)
        ("tx_3", "CUST_3", "Jhonny Cash", 500.00, "GBP", "ACH", "DECLINED", "Guard", "2026-11-10T16:00:00Z", ingestion_ts),
        # Record 5: Invalid because _ingestion_timestamp is null
        ("tx_4", "CUST_4", "Test Client", 80.00, "EUR", "WIRE_TRANSFER", "FRAUD_ALERT", "Analyst", "2026-11-10T17:00:00Z", None),
    ]

    bronze_df = spark.createDataFrame(mock_data, schema)

    # 2. Act: Run the transformation
    result_df = transform_silver_data(bronze_df)
    results = {row["transaction_id"]: row for row in result_df.collect()}

    # 3. Assert: Validations
    # Deduplication: 5 input rows should become 4 unique ones
    assert len(results) == 4

    # Validation tx_1 (Cleaning, typing, PII, and is_valid=True)
    tx1 = results["tx_1"]
    assert tx1["currency"] == "BRL"
    assert tx1["status"] == "APPROVED"
    assert tx1["amount"] == Decimal("150.50")
    assert tx1["transaction_date"] == date(2026, 11, 10)
    assert tx1["is_valid"] is True

    # PII validation (SHA-256)
    expected_hash = hashlib.sha256("Jhon Wick".encode("utf-8")).hexdigest()
    assert tx1["customer_name"] == expected_hash
    assert tx1["customer_name"] != "Jhon Wick" 

    # Validate quality rules for invalid records
    assert results["tx_2"]["is_valid"] is False  # amount <= 0
    assert results["tx_3"]["is_valid"] is False  # invalid currency (GBP)
    assert results["tx_4"]["is_valid"] is False  # null _ingestion_timestamp