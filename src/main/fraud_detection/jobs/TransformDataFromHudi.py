import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, current_date, datediff, lit, round, udf, broadcast, to_date
from pyspark.sql.types import TimestampType, IntegerType
from src.main.fraud_detection.utils import get_distance
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.creditcard.Schema import Schema  
from src.main.fraud_detection.helpers import read_from_csv, read_from_hudi

def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Transform data from Hudi") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .getOrCreate()
    
    hudi_options = {
        "hoodie.datasource.read.streaming.enabled": "false",  
        "hoodie.datasource.read.recordkey.field": "cc_num", 
        "hoodie.datasource.read.precombine.field": "timestamp",
        "hoodie.metadata.enable": "false"
    }

    # Read transaction data
    transaction_hudi_path = "hdfs://Master:9000/user/hudi/tables/transactions"
    transaction_df = read_from_hudi(transaction_hudi_path,hudi_options,spark).withColumn("trans_time", to_timestamp("trans_time", "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
    transaction_df.show()

    # Read customer data
    customer_hudi_path = "hdfs://Master:9000/user/hudi/tables/customers"
    customer_df = read_from_hudi(customer_hudi_path, hudi_options, spark)
    customer_df.show()

    # Calculate customer age
    customer_age_df = customer_df.withColumn("age", (datediff(current_date(), to_date("dob")) / 365).cast(IntegerType()))
    # UDF to calculate distance
    distance_udf = udf(get_distance)

    # Join transaction and customer data
    processed_transaction_df = transaction_df.join(broadcast(customer_age_df), "cc_num") \
        .withColumn("distance", lit(round(distance_udf("lat", "long", "merch_lat", "merch_long"), 2))) \
        .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")
    processed_transaction_df.cache()

    processed_transaction_df = processed_transaction_df.withColumn("partition_column", lit("default_partition")) \
        .withColumn("timestamp", lit("2024-01-01 00:00:00")) 

    # Save transactions to Hudi
    transaction_hudi_options = {
        "hoodie.table.name": "processed_transactions",
        "hoodie.datasource.write.recordkey.field": "cc_num", 
        "hoodie.datasource.write.partitionpath.field": "partition_column",  
        "hoodie.datasource.write.table.name": "processed_transactions",
        "hoodie.datasource.write.precombine.field": "timestamp", 
        "hoodie.datasource.write.operation": "upsert",  
        "hoodie.metadata.enable": "false"
    }
    processed_transaction_output_path = "hdfs://Master:9000/user/hudi/tables/processed_transactions"
    processed_transaction_df.write.format("hudi") \
        .options(**transaction_hudi_options) \
        .mode("overwrite") \
        .save(processed_transaction_output_path)

if __name__ == "__main__":
    main(sys.argv[1:])
