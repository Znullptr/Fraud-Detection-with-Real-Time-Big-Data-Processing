from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType

class Schema:
    transaction_structure_name = "transaction"

    transaction_schema = StructType() \
        .add("cc_num", StringType(), True) \
        .add("first", StringType(), True) \
        .add("last", StringType(), True) \
        .add("trans_num", StringType(), True) \
        .add("trans_date", StringType(), True) \
        .add("trans_time", StringType(), True) \
        .add("unix_time", LongType(), True) \
        .add("category", StringType(), True) \
        .add("merchant", StringType(), True) \
        .add("amt", DoubleType(), True) \
        .add("merch_lat", DoubleType(), True) \
        .add("merch_long", DoubleType(), True)

    # Transaction schema with fraud check
    fraud_checked_transaction_schema = transaction_schema.add("is_fraud", DoubleType(), True)

    customer_structure_name = "customer"
    customer_schema = StructType() \
        .add("cc_num", StringType(), True) \
        .add("first", StringType(), True) \
        .add("last", StringType(), True) \
        .add("gender", StringType(), True) \
        .add("street", StringType(), True) \
        .add("city", StringType(), True) \
        .add("state", StringType(), True) \
        .add("zip", StringType(), True) \
        .add("lat", DoubleType(), True) \
        .add("long", DoubleType(), True) \
        .add("job", StringType(), True) \
        .add("dob", TimestampType(), True)

    kafka_transaction_structure_name = transaction_structure_name
    kafka_transaction_schema = StructType() \
        .add("cc_num", StringType(), True) \
        .add("first", StringType(), True) \
        .add("last", StringType(), True) \
        .add("trans_num", StringType(), True) \
        .add("trans_time", TimestampType(), True) \
        .add("category", StringType(), True) \
        .add("merchant", StringType(), True) \
        .add("amt", StringType(), True) \
        .add("merch_lat", StringType(), True) \
        .add("merch_long", StringType(), True)
