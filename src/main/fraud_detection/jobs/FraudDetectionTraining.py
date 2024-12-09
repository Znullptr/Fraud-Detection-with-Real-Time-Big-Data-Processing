from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.helpers import read_from_hudi, kmeans_oversample
from src.main.fraud_detection.spark.algorithms import random_forest_classifier
from src.main.fraud_detection.spark.pipeline import create_feature_pipeline


def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Initialise Fraud Detection Model and fit it to dataset") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .getOrCreate()
    
    # Read transactions from Cassandra
    
    hudi_options = {
        "hoodie.datasource.read.streaming.enabled": "false",  
        "hoodie.datasource.read.recordkey.field": "trans_num", 
        "hoodie.datasource.read.precombine.field": "timestamp",
        "hoodie.metadata.enable": "false"
    }

    processed_transaction_hudi_path = "hdfs://Master:9000/user/hudi/tables/processed_transactions"

    transaction_df = read_from_hudi(processed_transaction_hudi_path,hudi_options,spark) \
        .select("category", "merchant", "distance", "amt", "age", "is_fraud")

    transaction_df.cache()

    # Define column names
    column_names = ["category", "merchant", "distance", "amt", "age"]

    # Build the feature pipeline
    pipeline_stages = create_feature_pipeline(transaction_df.schema, column_names)
    pipeline = Pipeline(stages=pipeline_stages)
    
    # Fit the pipeline model
    preprocessing_transformer_model = pipeline.fit(transaction_df)
    preprocessing_transformer_model.write().overwrite().save(config.spark_config.preprocessing_model_path)

    # Transform the transaction DataFrame using the pipeline
    feature_df = preprocessing_transformer_model.transform(transaction_df) \
                                                .withColumnRenamed("is_fraud", "label") \
                                                .select("features", "label")


    # Balance the dataset by applying K-means clustering on fraud data
    balanced_feature_df = kmeans_oversample(feature_df, label_col="label", features_col="features", minority_class=1, num_samples=10000, k=10)

    # Train the random forest classifier
    random_forest_model = random_forest_classifier(balanced_feature_df)
    
    # Save the trained model
    random_forest_model.write().overwrite().save(config.spark_config.model_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
