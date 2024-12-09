import logging
import os
import random
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.linalg import Vectors
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


def create_balanced_dataframe(df, label_col, features_col, minority_class, num_samples):
    """
    Creates synthetic samples for the minority class by interpolating existing samples.
    :param df: Input DataFrame
    :param label_col: Column name for the label
    :param features_col: Column name for the features
    :param minority_class: The value of the minority class label
    :param num_samples: Number of synthetic samples to create
    :return: DataFrame with synthetic samples added
    """
    # Filter minority class
    minority_df = df.filter(F.col(label_col) == minority_class)
    minority_rows = minority_df.collect()

    synthetic_samples = []
    for _ in range(num_samples):
        # Randomly choose two minority samples
        row1, row2 = random.sample(minority_rows, 2)
        features1 = row1[features_col].toArray()
        features2 = row2[features_col].toArray()

        # Interpolate features
        synthetic_features = Vectors.dense(
            features1 + random.random() * (features2 - features1)
        )

        # Create a synthetic row
        synthetic_samples.append(Row(features=synthetic_features, label=minority_class))

    # Convert synthetic samples to a DataFrame
    synthetic_df = df.sql_ctx.createDataFrame(synthetic_samples)

    # Combine with the original DataFrame
    balanced_df = df.union(synthetic_df)
    return balanced_df

def kmeans_oversample(df, label_col, features_col, minority_class, num_samples, k):
    """
    Uses KMeans clustering to generate synthetic samples for the minority class.
    :param df: Input DataFrame
    :param label_col: Column name for the label
    :param features_col: Column name for the features
    :param minority_class: The value of the minority class label
    :param num_samples: Number of synthetic samples to generate
    :param k: Number of clusters for KMeans
    :return: DataFrame with synthetic samples added
    """
    # Filter the minority class
    minority_df = df.filter(F.col(label_col) == minority_class)
    
    # Train KMeans on the minority class
    kmeans = KMeans(k=k, featuresCol=features_col, seed=42, maxIter=50)
    kmeans_model = kmeans.fit(minority_df)
    
    # Get cluster centers
    cluster_centers = kmeans_model.clusterCenters()
    
    # Collect minority class data as a list
    minority_data = minority_df.select(features_col).collect()
    
    # Generate synthetic samples
    synthetic_samples = []
    for _ in range(num_samples):
        # Randomly choose a cluster center
        cluster_idx = random.randint(0, k - 1)
        cluster_center = Vectors.dense(cluster_centers[cluster_idx])
        
        # Randomly perturb the cluster center or interpolate between two points in the cluster
        if len(minority_data) > 1:
            point1 = random.choice(minority_data)[features_col].toArray()
            point2 = random.choice(minority_data)[features_col].toArray()
            synthetic_features = Vectors.dense(
                point1 + random.random() * (point2 - point1)
            )
        else:
            # If only one point exists, slightly perturb the center
            synthetic_features = Vectors.dense(
                cluster_center + 0.01 * (random.random() - 0.5)
            )
        
        synthetic_samples.append(Row(features=synthetic_features, label=minority_class))
    
    # Convert synthetic samples to DataFrame
    synthetic_df = df.sql_ctx.createDataFrame(synthetic_samples)
    
    # Combine synthetic samples with the original DataFrame
    balanced_df = df.union(synthetic_df)
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