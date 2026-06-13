"""
Tests for :mod:`sales_data.data_quality`.

:description: Uses *chispa* for DataFrame equality assertions and plain
    pytest for exception and warning-message behaviour.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sales_data.data_quality import (
    check_address_format,
    check_calls_successful_le_calls_made,
    check_non_negative_numerics,
    check_non_null_unique_ids,
    check_referential_integrity,
    check_row_count,
    DataQualityError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DS1_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("area", StringType()),
        StructField("calls_made", IntegerType()),
        StructField("calls_successful", IntegerType()),
    ]
)

DS2_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("address", StringType()),
        StructField("sales_amount", DoubleType()),
    ]
)

DS3_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("caller_id", IntegerType()),
        StructField("company", StringType()),
        StructField("recipient", StringType()),
        StructField("age", IntegerType()),
        StructField("country", StringType()),
        StructField("product_sold", StringType()),
        StructField("quantity", IntegerType()),
    ]
)


# ---------------------------------------------------------------------------
# check_non_null_unique_ids
# ---------------------------------------------------------------------------


def test_non_null_unique_ids_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "IT"), (2, "Marketing")], ["id", "area"])
    # No exception → test passes
    check_non_null_unique_ids(df, "id", "test_ds", halt_on_failure=True)


def test_non_null_unique_ids_duplicate_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "IT"), (1, "Marketing")], ["id", "area"])
    with pytest.raises(DataQualityError):
        check_non_null_unique_ids(df, "id", "test_ds", halt_on_failure=True)


def test_non_null_unique_ids_null_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, "IT"), (2, "Marketing")], ["id", "area"])
    with pytest.raises(DataQualityError):
        check_non_null_unique_ids(df, "id", "test_ds", halt_on_failure=True)


def test_non_null_unique_ids_warn_only(spark: SparkSession) -> None:
    """Failed check with halt_on_failure=False should NOT raise."""
    df = spark.createDataFrame([(1, "IT"), (1, "Marketing")], ["id", "area"])
    check_non_null_unique_ids(df, "id", "test_ds", halt_on_failure=False)


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------


def test_row_count_correct(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    check_row_count(df, 2, "test_ds", halt_on_failure=True)


def test_row_count_wrong_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    with pytest.raises(DataQualityError):
        check_row_count(df, 5, "test_ds", halt_on_failure=True)


# ---------------------------------------------------------------------------
# check_non_negative_numerics
# ---------------------------------------------------------------------------


def test_non_negative_numerics_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 10, 5)], ["id", "calls_made", "calls_successful"])
    check_non_negative_numerics(df, ["calls_made", "calls_successful"], "ds1", halt_on_failure=True)


def test_non_negative_numerics_negative_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, -1, 5)], ["id", "calls_made", "calls_successful"])
    with pytest.raises(DataQualityError):
        check_non_negative_numerics(df, ["calls_made"], "ds1", halt_on_failure=True)


# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------


def test_referential_integrity_passes(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "IT"), (2, "Games")], ["id", "area"])
    df3 = spark.createDataFrame(
        [(1, 1, "Corp", "Bob", 30, "Netherlands", "Laptop", 5)],
        ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"],
    )
    check_referential_integrity(df3, df1, halt_on_failure=True)


def test_referential_integrity_orphan_raises(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "IT")], ["id", "area"])
    df3 = spark.createDataFrame(
        [(1, 99, "Corp", "Bob", 30, "Netherlands", "Laptop", 5)],
        ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"],
    )
    with pytest.raises(DataQualityError):
        check_referential_integrity(df3, df1, halt_on_failure=True)


# ---------------------------------------------------------------------------
# check_calls_successful_le_calls_made
# ---------------------------------------------------------------------------


def test_calls_le_made_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "IT", 10, 8)], ["id", "area", "calls_made", "calls_successful"])
    check_calls_successful_le_calls_made(df, halt_on_failure=True)


def test_calls_le_made_violation_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "IT", 5, 10)], ["id", "area", "calls_made", "calls_successful"])
    with pytest.raises(DataQualityError):
        check_calls_successful_le_calls_made(df, halt_on_failure=True)


# ---------------------------------------------------------------------------
# check_address_format
# ---------------------------------------------------------------------------


def test_address_format_valid(spark: SparkSession) -> None:
    # Format: "[street name], [house number], [4digits SPACE 2CAPS]"
    df = spark.createDataFrame(
        [(1, "Alice", "Lindehof, 5, 4133 HB", 50000.0)], DS2_SCHEMA
    )
    check_address_format(df, halt_on_failure=True)


def test_address_format_invalid_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "Bob", "No Zipcode Street", 40000.0)], DS2_SCHEMA
    )
    with pytest.raises(DataQualityError):
        check_address_format(df, halt_on_failure=True)
