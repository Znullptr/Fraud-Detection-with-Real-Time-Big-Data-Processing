from cassandra.cluster import Cluster
from pyspark.sql import Row
from pyspark.sql.streaming import ForeachWriter
from src.main.fraud_detection.creditcard.CreditcardEnum import Enums
from src.main.fraud_detection.cassandra.config import CassandraConfig
import logging

logger = logging.getLogger(__name__)

class CassandraSinkForeach(ForeachWriter):
    
    def __init__(self, db_name: str, table_name: str, config: CassandraConfig):
        self.db = db_name
        self.table = table_name
        self.cluster = None
        self.session = None
        self.cassandra_config = config

    def cql_transaction(self, record: Row) -> str:
        """
        Create CQL statement to insert transaction record into Cassandra.
        """
        return f"""
        INSERT INTO {self.db}.{self.table} (
            {Enums.TransactionCassandra.cc_num},
            {Enums.TransactionCassandra.trans_time},
            {Enums.TransactionCassandra.trans_num},
            {Enums.TransactionCassandra.category},
            {Enums.TransactionCassandra.merchant},
            {Enums.TransactionCassandra.amt},
            {Enums.TransactionCassandra.merch_lat},
            {Enums.TransactionCassandra.merch_long},
            {Enums.TransactionCassandra.distance},
            {Enums.TransactionCassandra.age},
            {Enums.TransactionCassandra.is_fraud}
        ) VALUES (
            '{record[Enums.TransactionCassandra.cc_num]}',
            '{record[Enums.TransactionCassandra.trans_time]}',
            '{record[Enums.TransactionCassandra.trans_num]}',
            '{record[Enums.TransactionCassandra.category]}',
            '{record[Enums.TransactionCassandra.merchant]}',
             {record[Enums.TransactionCassandra.amt]},
             {record[Enums.TransactionCassandra.merch_lat]},
             {record[Enums.TransactionCassandra.merch_long]},
             {record[Enums.TransactionCassandra.distance]},
             {record[Enums.TransactionCassandra.age]},
             {record[Enums.TransactionCassandra.is_fraud]}
        )
        """

    def cql_offset(self, record: Row) -> str:
        """
        Create CQL statement to insert Kafka offset into Cassandra.
        """
        return f"""
        INSERT INTO {self.db}.{self.table} (
            {Enums.TransactionCassandra.kafka_partition},
            {Enums.TransactionCassandra.kafka_offset}
        ) VALUES (
            {record[Enums.TransactionCassandra.kafka_partition]},
            {record[Enums.TransactionCassandra.kafka_offset]}
        )
        """

    def open(self, partition_id: int, epoch_id: int) -> bool:
        """
        Open connection to Cassandra.
        """
        try:
            self.cluster = Cluster([self.cassandra_config.host])
            self.session = self.cluster.connect()
            logger.info(f"Opened Cassandra connection for partition {partition_id}")
            return True
        except Exception as e:
            logger.error(f"Error opening connection: {e}")
            return False

    def process(self, record: Row):
        """
        Process each record and insert it into the appropriate Cassandra table.
        """
        try:
            if self.table == self.cassandra_config.transaction_table:
                cql = self.cql_transaction(record)
                self.session.execute(cql)
                logger.info(f"Inserted transaction record: {record}")
            elif self.table == self.cassandra_config.kafka_offset_table:
                cql = self.cql_offset(record)
                self.session.execute(cql)
                logger.info(f"Inserted Kafka offset: {record}")
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    def close(self, error: Exception):
        """
        Close the connection to Cassandra.
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        if error:
            logger.error(f"Error during close: {error}")
    
        logger.info("Closed Cassandra connection cleanly.")
