import logging
from src.main.fraud_detection.config import Config

class CassandraConfig(Config):
    logger = logging.getLogger(__name__)

    # Initializing variables
    keyspace = None
    transaction_table = None
    kafka_offset_table = None
    customer = None
    host = None

    def load(self):
        """Load Cassandra settings from the configuration file."""
        self.logger.info("Loading Cassandra settings")
        self.keyspace = super().application_conf.get("config.cassandra.keyspace","creditcard")
        self.transaction_table = super().application_conf.get("config.cassandra.table.transaction","transaction")
        self.kafka_offset_table = super().application_conf.get("config.cassandra.table.kafka.offset","kafka_offset")
        self.customer = super().application_conf.get("config.cassandra.table.customer","customer")
        self.host = super().application_conf.get("config.cassandra.host","10.0.2.10")
        return self

