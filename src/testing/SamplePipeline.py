from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, round as ps_round, current_date, to_date, datediff, col
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import VectorUDT
import math

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sample App") \
    .getOrCreate()

def get_distance(lat1, lon1, lat2, lon2):
    r = 6371  # Earth radius in km
    lat_distance = math.radians(lat2 - lat1)
    lon_distance = math.radians(lon2 - lon1)
    a = (math.sin(lat_distance / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(lon_distance / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c

# Register UDF
distance_udf = udf(get_distance, IntegerType())

# Read options
read_options = {"inferSchema": "true", "header": "true"}

# Load transaction and customer data
raw_transaction_df = spark.read.options(**read_options).csv("path/to/transaction_datasource")
raw_customer_df = spark.read.options(**read_options).csv("path/to/customer_datasource")

# Process customer data
customer_age_df = raw_customer_df \
    .withColumn("age", (datediff(current_date(), to_date(col("dob"))) / 365).cast(IntegerType())) \
    .withColumnRenamed("cc_num", "cardNo")

# Process transaction data
processed_transaction_df = customer_age_df.join(
    raw_transaction_df, customer_age_df.cardNo == raw_transaction_df.cc_num
).withColumn(
    "distance", ps_round(
        distance_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long")), 2
    )
).selectExpr(
    "cast(cc_num as string) cc_num", "category", "merchant", "distance", "amt", "age", "is_fraud"
)

processed_transaction_df.ca
