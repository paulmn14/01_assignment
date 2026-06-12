"""
Pytest configuration and shared fixtures.

:description: Provides a module-scoped SparkSession so that Spark is
    initialised once per test session rather than once per test, keeping
    the test suite fast.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Return a SparkSession configured for unit testing.

    :return: A local SparkSession.
    :rtype: pyspark.sql.SparkSession
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("SalesDataTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
