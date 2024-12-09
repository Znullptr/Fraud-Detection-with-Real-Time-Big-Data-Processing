import logging
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def random_forest_classifier(df: DataFrame):
    """
    Train a RandomForestClassifier on the given DataFrame and return the model.
    """
    # Split the data into training and testing sets
    training, test = df.randomSplit([0.7, 0.3])

    # Create a RandomForestClassifier estimator
    random_forest_estimator = RandomForestClassifier(labelCol="label", featuresCol="features", maxBins=700)

    # Fit the model on the training data
    model = random_forest_estimator.fit(training)

    # Perform predictions on the test data
    transaction_with_prediction = model.transform(test)

    # Log the total data count and count of correct predictions
    total_data_count= transaction_with_prediction.count()
    correct_predection_count = transaction_with_prediction.filter(transaction_with_prediction['prediction'] == transaction_with_prediction['label']).count()
    accuracy = (correct_predection_count/total_data_count)*100
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")
    print(f"accuracy score: {accuracy}%")
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")
    print("*******************************************************")

    return model
