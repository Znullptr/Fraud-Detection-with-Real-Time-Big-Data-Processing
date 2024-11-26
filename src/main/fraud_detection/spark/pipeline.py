import logging
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer, StringIndexerModel
from pyspark.sql.types import StringType, NumericType, StructType
from pyspark.ml import PipelineModel

logger = logging.getLogger(__name__)

def create_string_indexer(columns):
    """
    Create a list of StringIndexer stages for the given columns.
    """
    return [StringIndexer(inputCol=column, outputCol=f"{column}_indexed") for column in columns]

def create_one_hot_encoder(columns):
    """
    Create a list of OneHotEncoder stages for the given columns.
    """
    return [OneHotEncoder(inputCol=f"{column}_indexed", outputCol=f"{column}_encoded") for column in columns]

def create_vector_assembler(feature_columns):
    """
    Create a VectorAssembler for the given feature columns.
    """
    return VectorAssembler(inputCols=feature_columns, outputCol="features")

def create_feature_pipeline(schema: StructType, columns):
    """
    Create a feature pipeline with StringIndexer, OneHotEncoder, and VectorAssembler.
    """
    feature_columns = []
    preprocessing_stages = []

    for field in schema.fields:
        if field.name in columns:
            if isinstance(field.dataType, StringType):
                string_indexer = StringIndexer(inputCol=field.name, outputCol=f"{field.name}_indexed")
                one_hot_encoder = OneHotEncoder(inputCol=f"{field.name}_indexed", outputCol=f"{field.name}_encoded")
                feature_columns.append(f"{field.name}_encoded")
                preprocessing_stages.extend([string_indexer, one_hot_encoder])

            elif isinstance(field.dataType, NumericType):
                feature_columns.append(field.name)

        vector_assembler = create_vector_assembler(feature_columns)
        return preprocessing_stages + [vector_assembler]


def create_string_indexer_pipeline(schema: StructType, columns):
    """
    Create a pipeline with only StringIndexer and VectorAssembler.
    """
    feature_columns = []
    preprocessing_stages = []

    for field in schema.fields:
        if field.name in columns:
            if isinstance(field.dataType, StringType):
                string_indexer = StringIndexer(inputCol=field.name, outputCol=f"{field.name}_indexed")
                feature_columns.append(f"{field.name}_indexed")
                preprocessing_stages.append(string_indexer)

            elif isinstance(field.dataType, NumericType):
                feature_columns.append(field.name)

    vector_assembler = create_vector_assembler(feature_columns)
    return preprocessing_stages + [vector_assembler]

def get_features(pipeline_model: PipelineModel):
    """
    Extract the feature names from the pipeline model stages, including handling of
    OneHotEncoding and StringIndexing.
    """
    # Find the VectorAssembler stage in the pipeline
    vector_assembler = next(
        (stage for stage in pipeline_model.stages if isinstance(stage, VectorAssembler)),
        None
    )
    if not vector_assembler:
        raise ValueError("Invalid model: No VectorAssembler found")

    # Get the input columns from the VectorAssembler
    feature_names = vector_assembler.getInputCols()

    # Map each feature name to the final column name after OneHotEncoding or StringIndexing
    def process_feature_name(feature_name):
        # Find the OneHotEncoder related to this feature (if any)
        one_hot_encoder = next(
            (stage for stage in pipeline_model.stages if isinstance(stage, OneHotEncoder) and stage.getOutputCol() == feature_name),
            None
        )

        # If there's a OneHotEncoder, get its input column
        one_hot_encoder_input_col = one_hot_encoder.getInputCol() if one_hot_encoder else feature_name

        # Find the StringIndexer related to this feature (if any)
        string_indexer = next(
            (stage for stage in pipeline_model.stages if isinstance(stage, StringIndexerModel) and stage.getOutputCol() == one_hot_encoder_input_col),
            None
        )

        string_indexer_input = string_indexer.getInputCol() if string_indexer else feature_name

        # If there was a OneHotEncoder, return the indexed column names (i.e., label names)
        if one_hot_encoder and string_indexer:
            label_values = string_indexer.labels
            return [f"{string_indexer_input}-{label}" for label in label_values]

        # Otherwise, return the original feature name
        return [string_indexer_input]

    # Apply the processing for each feature name
    final_feature_names = []
    for feature_name in feature_names:
        final_feature_names.extend(process_feature_name(feature_name))

    return final_feature_names