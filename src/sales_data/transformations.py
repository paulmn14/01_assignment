"""
Transformation logic for all six analytical outputs.

:description: Each function accepts one or more PySpark
    DataFrames and returns a transformed DataFrame ready for writing.
    No I/O happens here, keeping the functions easily testable.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from sales_data.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _join_ds1_ds2(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Inner-join dataset_one and dataset_two on their ``id`` columns.

    """

    return df1.join(df2, on="id", how="inner")


def _extract_zip_code(df: DataFrame) -> DataFrame:
    """
    Parse the ``address`` column and split it into ``street_address``
    and ``zip_code`` columns.

    """
    # Regex: 4 digits, a space, 2 uppercase letters – standard Dutch postcode.
    zip_pattern = r"(\d{4}\s[A-Z]{2})"

    return df.withColumn(
        "zip_code", F.regexp_extract(F.col("address"), zip_pattern, 1)
    ).withColumn(
        "street_address",
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("address"), zip_pattern, ""), r",\s*,", ","
                ),
                r"(^,\s*|\s*,$)",
                "",
            )
        ),
    )


# ---------------------------------------------------------------------------
# Output #1 – IT Data
# ---------------------------------------------------------------------------


def build_it_data(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Return the top-100 IT-department employees ordered by sales amount desc.

    """
    logger.info("Building Output #1 – IT Data …")
    joined = _join_ds1_ds2(df1, df2)
    result = (
        joined.filter(F.col("area") == "IT")
        .orderBy(F.col("sales_amount").desc())
        .limit(100)
    )
    logger.info("Output #1 ready.")
    return result


# ---------------------------------------------------------------------------
# Output #2 – Marketing Address Information
# ---------------------------------------------------------------------------


def build_marketing_address_info(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Return the address and zip code for Marketing-department employees.

    """
    logger.info("Building Output #2 – Marketing Address Info …")
    joined = _join_ds1_ds2(df1, df2)
    filtered = joined.filter(F.col("area") == "Marketing")
    with_zip = _extract_zip_code(filtered)
    result = with_zip.select("street_address", "zip_code")
    logger.info("Output #2 ready.")
    return result


# ---------------------------------------------------------------------------
# Output #3 – Department Breakdown
# ---------------------------------------------------------------------------


def build_department_breakdown(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Return total sales amount and call-success rate per department.

    """
    logger.info("Building Output #3 – Department Breakdown …")
    joined = _join_ds1_ds2(df1, df2)
    result = (
        joined.groupBy("area")
        .agg(
            F.round(F.sum("sales_amount"), 2).alias("total_sales_amount"),
            F.sum("calls_made").alias("total_calls_made"),
            F.sum("calls_successful").alias("total_calls_successful"),
        )
        .withColumn(
            "call_success_rate",
            F.concat(
                F.format_number(
                    F.col("total_calls_successful") / F.col("total_calls_made") * 100,
                    2,
                ),
                F.lit("%"),
            ),
        )
        .drop("total_calls_made", "total_calls_successful")
        .orderBy("area")
    )
    logger.info("Output #3 ready.")
    return result


# ---------------------------------------------------------------------------
# Output #4 – Top 3 best performers per department
# ---------------------------------------------------------------------------


def build_top_3_performers(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Return the top-3 performers per department with success rate (calls_successful / calls_made) > 75 %.

    """

    logger.info("Building Output #4 – Top 3 Performers per Department …")
    joined = _join_ds1_ds2(df1, df2)

    window = Window.partitionBy("area").orderBy(
        (F.col("calls_successful") / F.col("calls_made")).desc(),
        F.col("sales_amount").desc(),
    )

    result = (
        joined.withColumn(
            "success_rate",
            F.col("calls_successful") / F.col("calls_made"),
        )
        .filter(F.col("success_rate") > 0.75)
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 3)
        .withColumn(
            "success_rate_pct",
            F.concat(F.format_number(F.col("success_rate") * 100, 2), F.lit("%")),
        )
        .select("area", "name", "success_rate_pct", "sales_amount", "rank")
        .orderBy("area", "rank")
    )
    logger.info("Output #4 ready.")
    return result


# ---------------------------------------------------------------------------
# Output #5 – Top 3 most sold products per department in the Netherlands
# ---------------------------------------------------------------------------


def build_top_3_products_netherlands(df1: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Return the top-3 most-sold products per department for Netherlands sales.

    """

    logger.info("Building Output #5 – Top 3 Products per Dept (NL) …")

    nl_sales = df3.filter(F.col("country") == "Netherlands")
    joined = nl_sales.join(
        df1.select("id", "area"), nl_sales["caller_id"] == df1["id"], how="inner"
    )

    agg = joined.groupBy("area", "product_sold").agg(
        F.sum("quantity").alias("total_quantity")
    )

    window = Window.partitionBy("area").orderBy(F.col("total_quantity").desc())

    result = (
        agg.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 3)
        .orderBy("area", "rank")
    )
    logger.info("Output #5 ready.")
    return result


# ---------------------------------------------------------------------------
# Output #6 – Best overall salesperson per country
# ---------------------------------------------------------------------------


def build_best_salesperson_per_country(df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Return the best overall salesperson per country.

    "Best" is defined as the employee who sold highest
    total ``quantity`` in that country.

    """
    logger.info("Building Output #6 – Best Salesperson per Country …")

    joined = df3.join(
        df2.select(F.col("id").alias("caller_id"), "name"),
        on="caller_id",
        how="inner",
    )

    agg = joined.groupBy("country", "name").agg(
        F.sum("quantity").alias("total_quantity")
    )

    window = Window.partitionBy("country").orderBy(F.col("total_quantity").desc())

    result = (
        agg.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .orderBy("country")
    )
    logger.info("Output #6 ready.")
    return result
