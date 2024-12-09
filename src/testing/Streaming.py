from pyspark.sql import SparkSession
from pyspark.sql.functions import split, concat_ws, unix_timestamp, col, max as ps_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, LongType, TimestampType
import json

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Streaming") \
    .getOrCreate()

# Define the schema (uncomment and customize as needed)
fraud_checked_transaction_schema = StructType([
    StructField("cc_num", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("transactionId", StringType(), True),
    StructField("transactionDate", StringType(), True),
    StructField("transactionTime", StringType(), True),
    StructField("unixTime", StringType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("merchlat", DoubleType(), True),
    StructField("merchlong", DoubleType(), True),
    StructField("distance", DoubleType(), True),
    StructField("age", DoubleType(), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True)
])

# Read offsets from Cassandra
def read_offset(keyspace: str, table: str):
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=keyspace, table=table, pushdown="true") \
        .load() \
        .select("partition", "offset") \
        .filter(col("partition").isNotNull())

    if df.rdd.isEmpty():
        return "startingOffsets", "earliest"
    else:
        offset_df = df.groupBy("partition").agg(ps_max("offset").alias("offset"))
        return "startingOffsets", transform_kafka_metadata_array_to_json(offset_df.collect())

# Transform Kafka metadata array to JSON
def transform_kafka_metadata_array_to_json(array):
    partition_offset = ", ".join(
        [f'"{row["partition"]}":{row["offset"]}' for row in array]
    )
    print("Offset: ", partition_offset)
    return json.dumps({
        "creditTransaction": json.loads(f'{{{partition_offset}}}')
    })

# Main processing function
def main(input_path):
    # Read input CSV
    df = spark.read \
        .option("header", "true") \
        .schema(fraud_checked_transaction_schema) \
        .csv(input_path)

    df.printSchema()
    df.show(truncate=False)

    # Process date and time columns
    df2 = df.withColumn("trans_date", split(col("trans_date"), "T").getItem(0)) \
        .withColumn("trans_time", concat_ws(" ", col("trans_date"), col("trans_time"))) \
        .withColumn("unix_time", unix_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

    df2.show(truncate=False)

# Run the main function
if __name__ == "__main__":
    input_path = "path/to/input.csv"  # Replace with your input file path
    main(input_path)
