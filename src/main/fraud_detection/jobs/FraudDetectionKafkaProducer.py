import time
from pyspark.sql import SparkSession
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.creditcard.Schema import Schema  
from src.main.fraud_detection.helpers import read_from_csv
from src.main.fraud_detection.utils import get_distance
from pyspark.sql.functions import current_date, datediff, lit, round, udf, broadcast, to_date
from pyspark.sql.types import  IntegerType



def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Initialize Kafka Producer") \
        .getOrCreate()

    # Read transaction data
    transaction_df = read_from_csv(
        config.kafka_config.transaction_datasource,
        Schema.transaction_schema,
        spark
    )
    customer_df = read_from_csv(config.kafka_config.customer_datasource,Schema.customer_schema, spark)

    customer_age_df = customer_df.withColumn("age", (datediff(current_date(), to_date("dob")) / 365).cast(IntegerType()))
    # UDF to calculate distance
    distance_udf = udf(get_distance)

    # Join transaction and customer data
    processed_transaction_df = transaction_df.join(broadcast(customer_age_df), "cc_num") \
        .withColumn("distance", lit(round(distance_udf("lat", "long", "merch_lat", "merch_long"), 2))) \
        .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")
    processed_transaction_df.cache()

    split_dfs = processed_transaction_df.randomSplit([100.0] * int(processed_transaction_df.count() / 100))

    for batch_df in split_dfs:
        batch_df.selectExpr(
            "CAST(cc_num AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.kafka_config.kafka_params["kafka.bootstrap.servers"]) \
            .option("topic", config.kafka_config.kafka_params["subscribe"]) \
            .save()
        batch_df.show()
        time.sleep(12)

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
