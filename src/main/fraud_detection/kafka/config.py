import logging
from src.main.fraud_detection.config import Config

class KafkaConfig(Config):
    logger = logging.getLogger(__name__)

    kafka_params = {}
    transaction_datasource = None

    def load(self):
        """Load Kafka settings from the configuration."""
        self.logger.info("Loading Kafka settings")
        self.transaction_datasource = super().local_project_dir + \
            super().application_conf.get("config.kafka.transaction.datasource", "resources/data/test/transactions.csv")
        self.customer_datasource = super().local_project_dir + \
            super().application_conf.get("config.kafka.customer.datasource", "resources/data/train/customer.csv")
        self.kafka_params["subscribe"] = super().application_conf.get("config.kafka.topic", "creditcardTransaction")
        self.kafka_params["enable.auto.commit"] = super().application_conf.get("config.kafka.enable.auto.commit", "false")
        self.kafka_params["groupId"] = super().application_conf.get("config.kafka.group.id", "creditcardConsumer")
        self.kafka_params["kafka.bootstrap.servers"] = super().application_conf.get("config.kafka.bootstrap.servers", "Master:9092,Worker1:9092,Worker2:9092")
        self.kafka_params["startingOffsets"] = super().application_conf.get("config.kafka.auto.offset.reset", "earliest")
        return self

