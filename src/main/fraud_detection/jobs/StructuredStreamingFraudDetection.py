from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType
from src.main.fraud_detection.creditcard.Schema import Schema  
from src.main.fraud_detection.cassandra.driver import CassandraDriver
from src.main.fraud_detection.config import Config
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json
from pyspark.ml.classification import RandomForestClassificationModel


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

    # Definne Cassandra driver
    driver = CassandraDriver(config.cassandra_config)
  
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
        .withColumn("distance", col("distance").cast(DoubleType()))
        .withColumn("age", col("age").cast(IntegerType()))
        .drop("is_fraud")  
    )
    
    # Feature preprocessing
    preprocessingModel = PipelineModel.load(config.spark_config.preprocessing_model_path)
    featureTransactionDF = preprocessingModel.transform(transaction_stream)

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
    fraud_query = driver.save_foreach(predictionDF, config.cassandra_config.keyspace,
                                             config.cassandra_config.transaction_table, "append")

    fraud_query.awaitTermination()
    # Close the Cassandra connection
    driver.close()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])   
