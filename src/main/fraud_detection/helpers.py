import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.linalg import DenseVector
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.ml.clustering import KMeans
from pyspark.sql.streaming import StreamingQuery
from typing import List

logger = logging.getLogger(__name__)

def read_from_csv(transaction_datasource: str, schema: StructType, spark_session: SparkSession) -> DataFrame:
    """
    Reads data from a CSV file into a Spark DataFrame using the specified schema.
    """
    return spark_session.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(transaction_datasource)

def read_from_hudi(hudi_path: str, hudi_options: dict, spark_session: SparkSession) -> DataFrame:

    return spark_session.read.format("hudi") \
        .options(**hudi_options) \
        .load(hudi_path)

def read_from_cassandra(keyspace: str, table: str, spark_session: SparkSession) -> DataFrame:
    """
    Reads data from a Cassandra table into a Spark DataFrame.
    """
    logger.info(f"Reading data from Cassandra keyspace {keyspace}, table {table}.")
    return spark_session.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=keyspace, table=table, pushdown="true") \
        .load()

def get_offset(rdd: RDD, spark_session: SparkSession) -> DataFrame:
    """
    Extracts offset ranges from an RDD and returns them as a DataFrame.
    """
    from pyspark.streaming.kafka import HasOffsetRanges

    logger.info("Extracting offset ranges from RDD.")
    offset_ranges = rdd.offsetRanges() if isinstance(rdd, HasOffsetRanges) else []
    offset_data = [(offset.partition, offset.untilOffset) for offset in offset_ranges]

    return spark_session.createDataFrame(offset_data, ["partition", "offset"])


def create_balanced_dataframe(df: DataFrame, reduction_count: int, spark_session: SparkSession) -> DataFrame:
    """
    In fraud detection datasets, there are usually more non-fraud transactions than fraud ones.
    To balance the non-fraud transactions, KMeans algorithm is applied to reduce the number
    of non-fraud transactions to match the number of fraud transactions.
    """
    # Apply KMeans to create 'reduction_count' clusters
    kmeans = KMeans(k=reduction_count, maxIter=30)
    kmeans_model = kmeans.fit(df)

    # Convert cluster centers to DenseVector format
    centers = [DenseVector(center) for center in kmeans_model.clusterCenters()]

    # Define schema for the balanced DataFrame
    schema = StructType([
        StructField("features", df.schema["features"].dataType, False), 
        StructField("label", IntegerType(), False)
    ])

    # Create balanced DataFrame from cluster centers
    balanced_df = spark_session.createDataFrame(
        [(center, 0) for center in centers],
        schema=schema
    )
    
    return balanced_df

def send_to_kafka(producer, topic, df):
    """
    Send each row of DataFrame to the Kafka topic.
    """
    records = df.toJSON().collect()  # Convert DataFrame rows to JSON format
    for record in records:
        producer.send(topic, value=record)


def check_shutdown_marker(shutdown_marker, stop_flag):
    """Check if the shutdown marker file exists."""
    if not stop_flag:
        stop_flag = os.path.exists(shutdown_marker)

def handle_graceful_shutdown(check_interval_millis: int, streaming_queries: List[StreamingQuery], spark_session: SparkSession, shutdown_marker: str, stop_flag = False):
    """Handle graceful shutdown for structured streaming."""
    is_stopped = False

    while not is_stopped:
        logger.info("Calling awaitTerminationOrTimeout")
        is_stopped = spark_session.streams.awaitAnyTermination(check_interval_millis)
        
        if is_stopped:
            logger.info("Confirmed! The streaming context is stopped. Exiting application...")
        else:
            logger.info("Streaming App is still running. Timeout...")

        # Check for shutdown marker
        check_shutdown_marker(shutdown_marker, stop_flag)
        
        if not is_stopped and stop_flag:
            logger.info("Stopping streaming queries right now")
            
            # Stop all streaming queries
            for query in streaming_queries:
                query.stop()
            
            # Stop the Spark session
            spark_session.stop()
            logger.info("Spark session is stopped!!!!!!!")