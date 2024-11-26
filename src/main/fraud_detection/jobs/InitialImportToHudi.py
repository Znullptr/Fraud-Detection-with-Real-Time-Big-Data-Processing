from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.main.fraud_detection.helpers import read_from_csv
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.creditcard.Schema import Schema  
import sys

def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Hudi CSV to Hudi Table") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .getOrCreate()

    # Define Hudi options
    customer_hudi_options = {
        "hoodie.table.name": "customers",
        "hoodie.datasource.write.recordkey.field": "cc_num",
        "hoodie.datasource.write.partitionpath.field": "partition_column",
        "hoodie.datasource.write.table.name": "customers",
        "hoodie.datasource.write.precombine.field": "update_timestamp",
        "hoodie.datasource.write.operation": "upsert",  # can be 'bulk_insert', 'insert', or 'upsert'
         "hoodie.metadata.enable": "false"
    }

    # Load CSV file into DataFrame
    customer_df = read_from_csv(config.spark_config.customer_datasource, Schema.customer_schema, spark)


    # Add partition and timestamp columns if necessary
    customer_df = customer_df.withColumn("partition_column", lit("default_partition")) \
        .withColumn("update_timestamp", lit("2024-01-01 00:00:00"))  # Example static columns

    # Write DataFrame to Hudi table
    customers_output_path = "hdfs://Master:9000/user/hudi/tables/customers"
    customer_df.write.format("hudi") \
        .options(**customer_hudi_options) \
        .mode("overwrite") \
        .save(customers_output_path)

    print("Customers Data written successfully to Hudi table.")
    transaction_hudi_options = {
        "hoodie.table.name": "transactions",
        "hoodie.datasource.write.recordkey.field": "cc_num", 
        "hoodie.datasource.write.partitionpath.field": "partition_column",  
        "hoodie.datasource.write.table.name": "transactions",
        "hoodie.datasource.write.precombine.field": "timestamp", 
        "hoodie.datasource.write.operation": "upsert",  
        "hoodie.metadata.enable": "false"
    }

    # Load transactions CSV into DataFrame
    transactions_df = read_from_csv(config.spark_config.transaction_datasource, Schema.transaction_schema, spark)

    # Add partition and timestamp columns for transactions
    transactions_df = transactions_df.withColumn("partition_column", lit("default_partition")) \
        .withColumn("timestamp", lit("2024-01-01 00:00:00")) 
    # Write transactions DataFrame to Hudi table
    transaction_output_path = "hdfs://Master:9000/user/hudi/tables/transactions"
    transactions_df.write.format("hudi") \
        .options(**transaction_hudi_options) \
        .mode("overwrite") \
        .save(transaction_output_path)

    print("Transaction data written successfully to Hudi table.")


if __name__ == "__main__":
    main(sys.argv[1:])