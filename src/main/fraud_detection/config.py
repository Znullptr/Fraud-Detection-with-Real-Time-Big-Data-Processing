import logging
import os
import json

class Config:

    logger = logging.getLogger(__name__)
    application_conf = {}
    run_mode = "local"
    local_project_dir = ""
    spark_config = None
    cassandra_config = None
    kafka_config = None

    def parse_args(self, args):
        """Parse a config object from a configuration file."""
        if len(args) > 0:
            # Load configuration from the provided file
            with open(args[0], 'r') as file:
                self.application_conf = json.load(file)
        """Load all configuration modules."""
        self.run_mode = self.application_conf.get("config.mode", "hdfs")

        if self.run_mode == "local":
            self.local_project_dir = f"file:///{os.path.expanduser('~')}/Projects/realtime_fraud_detection/" 
        self.load_configs()

    def load_configs(self):
        from src.main.fraud_detection.spark.config import SparkConfig
        from src.main.fraud_detection.cassandra.config import CassandraConfig
        from src.main.fraud_detection.kafka.config import KafkaConfig
        self.spark_config = SparkConfig().load()
        self.cassandra_config = CassandraConfig().load()
        self.kafka_config = KafkaConfig().load()
        