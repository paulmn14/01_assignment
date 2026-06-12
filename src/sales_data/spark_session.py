"""
SparkSession factory for the sales-data pipeline.

:description: Centralises SparkSession creation so that the same
    configuration is used in both the production pipeline and the test
    suite.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "SalesData", master: str = "local[*]") -> SparkSession:

    # Build and return a SparkSession`.

    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
