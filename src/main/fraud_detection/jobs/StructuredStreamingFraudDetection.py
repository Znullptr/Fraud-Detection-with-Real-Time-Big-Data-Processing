from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round, udf, to_timestamp, datediff, current_date, broadcast
from pyspark.sql.types import DoubleType, IntegerType
from src.main.fraud_detection.creditcard.Schema import Schema  
from src.main.fraud_detection.cassandra.driver import CassandraDriver
from src.main.fraud_detection.config import Config
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json
from pyspark.ml.classification import RandomForestClassificationModel
from src.main.fraud_detection.utils import get_distance
from src.main.fraud_detection.helpers import read_from_cassandra, handle_graceful_shutdown


# Initialize Spark session
spark = SparkSession.builder.appName("Start Structured Streaming Job to detect fraud transaction").getOrCreate()


def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Start Structured Streaming Job to detect fraud transaction") \
        .config("spark.cassandra.connection.host", config.cassandra_config.host) \
        .getOrCreate()

    # Read customer data from Cassandra
    driver = CassandraDriver(config.cassandra_config)
    customer_df = read_from_cassandra(config.cassandra_config.keyspace, config.cassandra_config.customer, spark)
    # Calculate age from date of birth
    customer_age_df = customer_df.withColumn("age", (datediff(current_date(), to_timestamp(col("dob"), "yyyy-MM-dd")) / 365).cast(IntegerType()))
    customer_age_df.cache()
    # Read from kafka topic
    raw_stream = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_config.kafka_params["kafka.bootstrap.servers"])
        .option("subscribe", config.kafka_config.kafka_params["subscribe"])
        .option("enable.auto.commit", config.kafka_config.kafka_params["enable.auto.commit"].lower() == "true")
        .option("group.id", config.kafka_config.kafka_params["groupId"])
        .load()
    )
    
    # Process transaction stream and cast relevant columns to DoubleType
    transaction_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING)", "partition", "offset")
        .withColumn("data", from_json(col("value"), Schema.kafka_transaction_schema))
        .select("data.*", "partition", "offset") 
        .withColumn("amt", col("amt").cast(DoubleType()))
        .withColumn("merch_lat", col("merch_lat").cast(DoubleType()))
        .withColumn("merch_long", col("merch_long").cast(DoubleType()))
        .drop("first", "last")  
    )

    # Preview processed results in the console
    transaction_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    
    # UDF for calculating distance
    distance_udf = udf(get_distance)

    # Join transactions with customer data and calculate distance
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")
    processedTransactionDF = transaction_stream.join(broadcast(customer_age_df), "cc_num") \
        .withColumn("distance", round(distance_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long")), 2)) \
        .select("cc_num", "trans_num", to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss").alias("trans_time"),
                "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age")
    
    processedTransactionDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    
    # Feature preprocessing
    preprocessingModel = PipelineModel.load(config.spark_config.preprocessing_model_path)
    featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

    # Load RandomForest model and make predictions
    randomForestModel = RandomForestClassificationModel.load(config.spark_config.model_path)
    predictionDF = randomForestModel.transform(featureTransactionDF) \
                                    .withColumnRenamed("prediction", "is_fraud") \
                                    .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")

    # Preview prediction results in the console
    predictionDF.writeStream \
       .format("console") \
       .outputMode("append") \
       .start()
    
    # Save fraud transactions to Cassandra
    driver.save_foreach(predictionDF, config.cassandra_config.keyspace,
                                             config.cassandra_config.transaction_table, "append")

    spark.streams.awaitAnyTermination()  # Wait for any streaming query to terminate
    # Close the Cassandra connection
    driver.close()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])   
