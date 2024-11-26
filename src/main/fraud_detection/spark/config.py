import logging
from pyspark import SparkConf
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.cassandra.config import CassandraConfig

class SparkConfig(Config):
    logger = logging.getLogger(__name__)

    spark_conf = SparkConf()
    
    transaction_datasource = None
    customer_datasource = None
    model_path = None
    preprocessing_model_path = None
    shutdown_marker = None
    batch_interval = None

    def load(self):
        """Load Spark settings from the configuration."""
        self.logger.info("Loading Spark settings")
        
        # Use graceful shutdown only if running DStreams; otherwise, Structured Streaming manages it with checkpointing.
        if super().application_conf.get("config.spark.useDStreams", "false").lower() == "true":
            self.spark_conf.set("spark.streaming.stopGracefullyOnShutdown", 
                super().application_conf.get("config.spark.gracefulShutdown", "true"))

        # Checkpoint location is crucial for Structured Streaming to maintain progress and state.
        self.spark_conf.set("spark.sql.streaming.checkpointLocation", 
            super().application_conf.get("config.spark.checkpoint", "/tmp/checkpoint"))

        # Cassandra connection setting
        self.spark_conf.set("spark.cassandra.connection.host", 
            super().application_conf.get("config.cassandra.host", CassandraConfig.host))

        # Custom shutdown marker, batch interval, and data paths
        self.shutdown_marker = super().application_conf.get("config.spark.shutdownPath", "/tmp/shutdownmarker")
        
        # Batch interval is used only for DStreams, not Structured Streaming
        self.batch_interval = int(super().application_conf.get("config.spark.batch.interval", "5000")) 
        
        # Data sources
        self.transaction_datasource = super().local_project_dir + \
            super().application_conf.get("config.spark.transaction.datasource", "resources/data/train/transactions.csv")
        
        self.customer_datasource = super().local_project_dir + \
            super().application_conf.get("config.spark.customer.datasource", "resources/data/train/customer.csv")
        
        # Model paths
        self.model_path = super().local_project_dir + \
            super().application_conf.get("config.spark.model.path", "resources/spark/training/RandomForestModel")
        
        self.preprocessing_model_path = super().local_project_dir + \
            super().application_conf.get("config.spark.preprocessing.model.path", "resources/spark/training/PreprocessingModel")
        
        return self

