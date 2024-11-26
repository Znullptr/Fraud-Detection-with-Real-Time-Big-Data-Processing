import time
from pyspark.sql import SparkSession
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.creditcard.Schema import Schema  
from src.main.fraud_detection.helpers import read_from_csv


def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Initialize Kafka Producer") \
        .config("spark.cassandra.connection.host", config.cassandra_config.host) \
        .getOrCreate()

    # Read transaction data
    transaction_df = read_from_csv(
        config.kafka_config.transaction_datasource,
        Schema.transaction_schema,
        spark
    )

    split_dfs = transaction_df.randomSplit([100.0] * int(transaction_df.count() / 100))

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
        time.sleep(2)

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
