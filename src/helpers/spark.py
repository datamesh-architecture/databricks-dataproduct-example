from pyspark.sql import SparkSession, DataFrame


def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()