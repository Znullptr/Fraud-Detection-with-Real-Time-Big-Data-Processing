from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from src.main.fraud_detection.config import Config
from src.main.fraud_detection.helpers import read_from_cassandra, create_balanced_dataframe
from src.main.fraud_detection.spark.algorithms import random_forest_classifier
from src.main.fraud_detection.spark.pipeline import create_feature_pipeline


def main(args):
    # Parse command-line arguments
    config = Config()
    config.parse_args(args)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Initialise Fraud Detection Model and fit it to dataset") \
        .config("spark.cassandra.connection.host", config.cassandra_config.host) \
        .getOrCreate()
    
    # Read transactions from Cassandra

    transaction_df = read_from_cassandra(config.cassandra_config.keyspace, config.cassandra_config.transaction_table, spark) \
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
    feature_df = preprocessing_transformer_model.transform(transaction_df)

    # Separate fraud and non-fraud transactions
    fraud_df = feature_df.filter(col("is_fraud") == 1) \
        .withColumnRenamed("is_fraud", "label") \
        .select("features", "label")

    non_fraud_df = feature_df.filter(col("is_fraud") == 0)

    # Count fraud transactions
    fraud_count = fraud_df.count()

    # Balance the dataset by applying K-means clustering on non-fraud data
    balanced_non_fraud_df = create_balanced_dataframe(non_fraud_df, int(fraud_count), spark)
    final_feature_df = fraud_df.union(balanced_non_fraud_df)

    # Train the random forest classifier
    random_forest_model = random_forest_classifier(final_feature_df)
    
    # Save the trained model
    random_forest_model.write().overwrite().save(config.spark_config.model_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
