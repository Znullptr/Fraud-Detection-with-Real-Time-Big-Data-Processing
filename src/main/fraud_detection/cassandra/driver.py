import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import max as spark_max
from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)

class CassandraDriver:
    def __init__(self, config):
        self.config = config
        self.cluster = Cluster([self.config.host])
        self.session = self.cluster.connect(self.config.keyspace)

    def debug_stream(self, df: DataFrame, mode: str = "append"):
        """
        Print data to the console for debugging purposes.
        """
        df.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "100") \
            .outputMode(mode) \
            .start()
        
    def save_foreach(self, df: DataFrame, keyspace: str, table: str, write_mode: str):
        """
        Save DataFrame to Cassandra using a foreachBatch approach.

        :param df: DataFrame to be saved.
        :param keyspace: Keyspace name.
        :param table: Table name.
        :param write_mode: Write mode ('append', 'overwrite').
        """
        def write_to_cassandra(batch_df, batch_id):
            # Convert DataFrame to RDD and write to Cassandra
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("spark.cassandra.connection.host", self.config.host) \
                .mode(write_mode) \
                .options(table=table, keyspace=keyspace) \
                .save()

        # Use foreachBatch to write each micro-batch to Cassandra
        df.writeStream \
            .foreachBatch(write_to_cassandra) \
            .outputMode("append") \
            .trigger(processingTime="15 seconds") \
            .start() \
            .awaitTermination()  # This will wait for the termination of the query

    def read_offset(self, keyspace: str, table: str, spark_session: SparkSession):
        """
        Read offsets from Cassandra for Structured Streaming.
        """
        df = spark_session \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table) \
            .option("pushdown", "true") \
            .load() \
            .select("partition", "offset")

        if df.rdd.isEmpty():
            return "startingOffsets", "earliest"
        else:
            # Collecting offsets and converting to JSON format
            offset_df = df.groupBy("partition").agg(spark_max("offset").alias("offset"))
            return "startingOffsets", self.transform_kafka_metadata_array_to_json(offset_df.collect())

    def transform_kafka_metadata_array_to_json(self, array):
        """
        Convert partition-offset metadata into a JSON format string.
        """
        partition_offset = "".join([f'"{row["partition"]}":{row["offset"]}, ' for row in array])
        logger.info("Offset: %s", partition_offset[:-2])

        partition_and_offset = f'{{"creditTransaction":{{{partition_offset[:-2]}}}}}'.replace("\n", "").replace(" ", "")
        logger.info(partition_and_offset)
        return partition_and_offset
    
    def close(self):
        """Close Cassandra session."""
        self.session.shutdown()
        self.cluster.shutdown()


