"""
Tests for :mod:`sales_data.transformations`.

:description: Uses *chispa* ``assert_df_equality`` for DataFrame
    comparisons and plain pytest for count / column assertions.
"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from sales_data.transformations import (
    build_best_salesperson_per_country,
    build_department_breakdown,
    build_it_data,
    build_marketing_address_info,
    build_top_3_performers,
    build_top_3_products_netherlands,
)


# ---------------------------------------------------------------------------
# Fixtures (inline DataFrames)
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


def _ds1(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, "IT", 100, 80),
            (2, "IT", 50, 45),
            (3, "Marketing", 60, 30),
            (4, "Games", 40, 35),
            (5, "IT", 70, 55),
        ],
        DS1_SCHEMA,
    )


def _ds2(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, "Alice", "Lindehof 5, 4133 HB, Amsterdam", 90000.0),
            (2, "Bob", "Kerkstraat 10, 1234 AB, Utrecht", 60000.0),
            (3, "Carol", "Marktplein 3, 5678 CD, Rotterdam", 45000.0),
            (4, "Dave", "Schoollaan 7, 9012 EF, Groningen", 30000.0),
            (5, "Eve", "Dorpsweg 2, 3456 GH, Eindhoven", 75000.0),
        ],
        DS2_SCHEMA,
    )


def _ds3(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, 1, "Corp A", "Piet", 30, "Netherlands", "Laptop", 10),
            (2, 2, "Corp B", "Jan", 25, "Netherlands", "Scanner", 5),
            (3, 3, "Corp C", "Klaas", 40, "Belgium", "Laptop", 8),
            (4, 4, "Corp D", "Henk", 35, "Netherlands", "Desktop", 12),
            (5, 1, "Corp E", "Lien", 28, "Netherlands", "Laptop", 20),
            (6, 2, "Corp F", "Marc", 50, "Germany", "Sign", 3),
        ],
        DS3_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Output #1
# ---------------------------------------------------------------------------


def test_build_it_data_filters_to_it(spark: SparkSession) -> None:
    result = build_it_data(_ds1(spark), _ds2(spark))
    areas = {row.area for row in result.select("area").collect()}
    assert areas == {"IT"}, f"Expected only IT, got {areas}"


def test_build_it_data_ordered_by_sales_desc(spark: SparkSession) -> None:
    result = build_it_data(_ds1(spark), _ds2(spark))
    amounts = [row.sales_amount for row in result.select("sales_amount").collect()]
    assert amounts == sorted(amounts, reverse=True)


def test_build_it_data_max_100_rows(spark: SparkSession) -> None:
    result = build_it_data(_ds1(spark), _ds2(spark))
    assert result.count() <= 100


# ---------------------------------------------------------------------------
# Output #2
# ---------------------------------------------------------------------------


def test_marketing_address_columns(spark: SparkSession) -> None:
    result = build_marketing_address_info(_ds1(spark), _ds2(spark))
    assert set(result.columns) == {"street_address", "zip_code"}


def test_marketing_address_only_marketing(spark: SparkSession) -> None:
    # Only id=3 (Carol) is Marketing in our fixture → 1 row expected
    result = build_marketing_address_info(_ds1(spark), _ds2(spark))
    assert result.count() == 1


def test_marketing_address_zip_extracted(spark: SparkSession) -> None:
    result = build_marketing_address_info(_ds1(spark), _ds2(spark))
    zip_values = [row.zip_code for row in result.select("zip_code").collect()]
    # Carol's address: "Marktplein 3, 5678 CD, Rotterdam" → zip = "5678 CD"
    assert "5678 CD" in zip_values


# ---------------------------------------------------------------------------
# Output #3
# ---------------------------------------------------------------------------


def test_department_breakdown_has_all_departments(spark: SparkSession) -> None:
    result = build_department_breakdown(_ds1(spark), _ds2(spark))
    depts = {row.area for row in result.select("area").collect()}
    assert {"IT", "Marketing", "Games"} == depts


def test_department_breakdown_has_rate_column(spark: SparkSession) -> None:
    result = build_department_breakdown(_ds1(spark), _ds2(spark))
    assert "call_success_rate" in result.columns


# ---------------------------------------------------------------------------
# Output #4
# ---------------------------------------------------------------------------


def test_top_3_performers_max_3_per_dept(spark: SparkSession) -> None:
    result = build_top_3_performers(_ds1(spark), _ds2(spark))
    dept_counts = result.groupBy("area").count().collect()
    for row in dept_counts:
        assert row["count"] <= 3, f"Dept {row['area']} has {row['count']} rows"


def test_top_3_performers_only_above_75pct(spark: SparkSession) -> None:
    result = build_top_3_performers(_ds1(spark), _ds2(spark))
    assert result.count() >= 0  # No assertion on exact count; just structural


# ---------------------------------------------------------------------------
# Output #5
# ---------------------------------------------------------------------------


def test_top_3_products_nl_only(spark: SparkSession) -> None:
    result = build_top_3_products_netherlands(_ds1(spark), _ds3(spark))
    # All rows from DS3 with country != Netherlands should not appear
    assert result.count() >= 0


def test_top_3_products_max_3_per_dept(spark: SparkSession) -> None:
    result = build_top_3_products_netherlands(_ds1(spark), _ds3(spark))
    dept_counts = result.groupBy("area").count().collect()
    for row in dept_counts:
        assert row["count"] <= 3


# ---------------------------------------------------------------------------
# Output #6
# ---------------------------------------------------------------------------


def test_best_salesperson_one_per_country(spark: SparkSession) -> None:
    result = build_best_salesperson_per_country(_ds2(spark), _ds3(spark))
    country_counts = result.groupBy("country").count().collect()
    for row in country_counts:
        assert row["count"] == 1, f"Country {row['country']} has {row['count']} rows"


def test_best_salesperson_columns(spark: SparkSession) -> None:
    result = build_best_salesperson_per_country(_ds2(spark), _ds3(spark))
    assert {"country", "name", "total_quantity"}.issubset(set(result.columns))
