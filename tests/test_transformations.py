"""
Tests for :mod:`sales_data.transformations`.

:description: Uses *chispa* ``assert_df_equality`` to compare actual
    DataFrame content row-by-row for deterministic outputs.  Plain
    pytest assertions are used only where row order is non-deterministic
    (window-ranked results with ties) or where a structural property
    is being checked rather than exact content.
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
# Input schemas
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
# Input fixture helpers
# ---------------------------------------------------------------------------

def _ds1(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, "IT",        100, 80),   # success rate 80% → qualifies
            (2, "IT",         50, 45),   # success rate 90% → qualifies
            (3, "Marketing",  60, 30),   # success rate 50% → below 75%
            (4, "Games",      40, 35),   # success rate 87.5% → qualifies
            (5, "IT",         70, 55),   # success rate 78.6% → qualifies
        ],
        DS1_SCHEMA,
    )


def _ds2(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, "Alice", "Lindehof, 5, 4133 HB", 90000.0),
            (2, "Bob",   "Kerkstraat, 10, 1234 AB", 60000.0),
            (3, "Carol", "Marktplein, 3, 5678 CD", 45000.0),
            (4, "Dave",  "Schoollaan, 7, 9012 EF", 30000.0),
            (5, "Eve",   "Dorpsweg, 2, 3456 GH", 75000.0),
        ],
        DS2_SCHEMA,
    )


def _ds3(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, 1, "Corp A", "Piet",  30, "Netherlands", "Laptop",  10),
            (2, 2, "Corp B", "Jan",   25, "Netherlands", "Scanner",  5),
            (3, 3, "Corp C", "Klaas", 40, "Belgium",     "Laptop",   8),
            (4, 4, "Corp D", "Henk",  35, "Netherlands", "Desktop", 12),
            (5, 1, "Corp E", "Lien",  28, "Netherlands", "Laptop",  20),
            (6, 2, "Corp F", "Marc",  50, "Germany",     "Sign",     3),
        ],
        DS3_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Output #1 – IT data
# chispa: compare exact rows (IT employees sorted by sales_amount desc)
# ---------------------------------------------------------------------------

def test_build_it_data_exact_content(spark: SparkSession) -> None:
    """
    IT data must contain exactly the three IT employees ordered by
    sales_amount descending.  chispa checks column names, types, and
    every row value.
    """
    result = build_it_data(_ds1(spark), _ds2(spark))

    expected_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("area", StringType()),
            StructField("calls_made", IntegerType()),
            StructField("calls_successful", IntegerType()),
            StructField("name", StringType()),
            StructField("address", StringType()),
            StructField("sales_amount", DoubleType()),
        ]
    )
    expected = spark.createDataFrame(
        [
            (1, "IT", 100, 80, "Alice", "Lindehof, 5, 4133 HB",   90000.0),
            (5, "IT",  70, 55, "Eve",   "Dorpsweg, 2, 3456 GH",   75000.0),
            (2, "IT",  50, 45, "Bob",   "Kerkstraat, 10, 1234 AB", 60000.0),
        ],
        expected_schema,
    )

    assert_df_equality(result, expected, ignore_nullable=True)


def test_build_it_data_max_100_rows(spark: SparkSession) -> None:
    """Row count must never exceed 100 (the limit clause)."""
    result = build_it_data(_ds1(spark), _ds2(spark))
    assert result.count() <= 100


# ---------------------------------------------------------------------------
# Output #2 – Marketing address info
# chispa: compare exact rows (street_address + zip_code)
# ---------------------------------------------------------------------------

def test_build_marketing_address_info_exact_content(spark: SparkSession) -> None:
    """
    Only Carol (id=3, Marketing) should appear.  chispa verifies both
    the extracted zip_code value and the street_address string.
    """
    result = build_marketing_address_info(_ds1(spark), _ds2(spark))

    expected_schema = StructType(
        [
            StructField("street_address", StringType()),
            StructField("zip_code", StringType()),
        ]
    )
    expected = spark.createDataFrame(
        [("Marktplein, 3,", "5678 CD")],
        expected_schema,
    )

    # ignore_row_order because groupBy inside _extract_zip_code may reorder
    assert_df_equality(
        result, expected, ignore_nullable=True, ignore_row_order=True
    )


def test_build_marketing_address_info_columns(spark: SparkSession) -> None:
    """Output must have exactly two columns: street_address and zip_code."""
    result = build_marketing_address_info(_ds1(spark), _ds2(spark))
    assert set(result.columns) == {"street_address", "zip_code"}


# ---------------------------------------------------------------------------
# Output #3 – Department breakdown
# chispa: compare exact aggregated rows
# ---------------------------------------------------------------------------

def test_build_department_breakdown_exact_content(spark: SparkSession) -> None:
    """
    Three departments, each with the correct total_sales_amount and
    call_success_rate string.  chispa checks every cell.

    Manual calculation from fixture:
      IT        → sales 90000+60000+75000=225000  calls 100+50+70=220  succ 80+45+55=180  → 81.82%
      Marketing → sales 45000                     calls 60              succ 30            → 50.00%
      Games     → sales 30000                     calls 40              succ 35            → 87.50%
    """
    result = build_department_breakdown(_ds1(spark), _ds2(spark))

    expected_schema = StructType(
        [
            StructField("area", StringType()),
            StructField("total_sales_amount", DoubleType()),
            StructField("call_success_rate", StringType()),
        ]
    )
    expected = spark.createDataFrame(
        [
            ("Games",     30000.0,  "87.50%"),
            ("IT",       225000.0,  "81.82%"),
            ("Marketing", 45000.0,  "50.00%"),
        ],
        expected_schema,
    )

    assert_df_equality(
        result, expected, ignore_nullable=True, ignore_row_order=True
    )


# ---------------------------------------------------------------------------
# Output #4 – Top 3 performers per department
# chispa: compare exact rows for deterministic fixture data
# ---------------------------------------------------------------------------

def test_build_top_3_performers_exact_content(spark: SparkSession) -> None:
    """
    From the fixture only employees with success_rate > 75% qualify:
      IT:        Bob (90%), Eve (78.6%), Alice (80%) → ranked 1,2,3 by rate desc
      Games:     Dave (87.5%) → rank 1
      Marketing: Carol (50%) → excluded (below 75%)

    chispa checks area, name, rank, and sales_amount for every row.
    """
    result = build_top_3_performers(_ds1(spark), _ds2(spark))

    expected_schema = StructType(
        [
            StructField("area", StringType()),
            StructField("name", StringType()),
            StructField("success_rate_pct", StringType()),
            StructField("sales_amount", DoubleType()),
            StructField("rank", IntegerType()),
        ]
    )
    expected = spark.createDataFrame(
        [
            ("Games", "Dave",  "87.50%", 30000.0, 1),
            ("IT",    "Bob",   "90.00%", 60000.0, 1),
            ("IT",    "Alice", "80.00%", 90000.0, 2),
            ("IT",    "Eve",   "78.57%", 75000.0, 3),
        ],
        expected_schema,
    )

    assert_df_equality(
        result, expected, ignore_nullable=True, ignore_row_order=True
    )


def test_build_top_3_performers_max_3_per_dept(spark: SparkSession) -> None:
    """No department may have more than 3 performers."""
    result = build_top_3_performers(_ds1(spark), _ds2(spark))
    for row in result.groupBy("area").count().collect():
        assert row["count"] <= 3, f"Dept {row['area']} has {row['count']} rows"


# ---------------------------------------------------------------------------
# Output #5 – Top 3 products per department (Netherlands only)
# chispa: compare exact aggregated rows
# ---------------------------------------------------------------------------

def test_build_top_3_products_netherlands_exact_content(spark: SparkSession) -> None:
    """
    NL transactions in fixture (Belgium + Germany excluded):
      id=1 caller_id=1 → IT   Laptop   10
      id=2 caller_id=2 → IT   Scanner   5
      id=4 caller_id=4 → Games Desktop 12
      id=5 caller_id=1 → IT   Laptop   20

    Aggregated NL quantities:
      IT    Laptop   30  → rank 1
      IT    Scanner   5  → rank 2
      Games Desktop  12  → rank 1

    chispa checks every row.
    """
    result = build_top_3_products_netherlands(_ds1(spark), _ds3(spark))

    expected_schema = StructType(
        [
            StructField("area", StringType()),
            StructField("product_sold", StringType()),
            StructField("total_quantity", LongType()),
            StructField("rank", IntegerType()),
        ]
    )
    expected = spark.createDataFrame(
        [
            ("Games", "Desktop", 12, 1),
            ("IT",    "Laptop",  30, 1),
            ("IT",    "Scanner",  5, 2),
        ],
        expected_schema,
    )

    assert_df_equality(
        result, expected, ignore_nullable=True, ignore_row_order=True
    )


def test_build_top_3_products_max_3_per_dept(spark: SparkSession) -> None:
    """No department may have more than 3 products."""
    result = build_top_3_products_netherlands(_ds1(spark), _ds3(spark))
    for row in result.groupBy("area").count().collect():
        assert row["count"] <= 3


# ---------------------------------------------------------------------------
# Output #6 – Best salesperson per country
# chispa: compare exact rows
# ---------------------------------------------------------------------------

def test_build_best_salesperson_per_country_exact_content(
    spark: SparkSession,
) -> None:
    """
    Quantities per country + name from fixture:
      Netherlands: Alice(id=1) Laptop 10+20=30, Bob(id=2) Scanner 5, Dave(id=4) Desktop 12
                   → Alice wins with 30
      Belgium:     Carol(id=3) Laptop 8
                   → Carol wins with 8
      Germany:     Bob(id=2) Sign 3
                   → Bob wins with 3

    chispa verifies country, name, and total_quantity for every row.
    """
    result = build_best_salesperson_per_country(_ds2(spark), _ds3(spark))

    expected_schema = StructType(
        [
            StructField("country", StringType()),
            StructField("name", StringType()),
            StructField("total_quantity", LongType()),
        ]
    )
    expected = spark.createDataFrame(
        [
            ("Belgium",     "Carol", 8),
            ("Germany",     "Bob",   3),
            ("Netherlands", "Alice", 30),
        ],
        expected_schema,
    )

    assert_df_equality(
        result, expected, ignore_nullable=True, ignore_row_order=True
    )


def test_build_best_salesperson_one_per_country(spark: SparkSession) -> None:
    """Each country must have exactly one winner."""
    result = build_best_salesperson_per_country(_ds2(spark), _ds3(spark))
    for row in result.groupBy("country").count().collect():
        assert row["count"] == 1, f"Country {row['country']} has {row['count']} rows"
