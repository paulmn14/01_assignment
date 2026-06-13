"""
Data Quality checks for the sales-data pipeline.

:description: Implements both **Basic** and **Intermediate** data
    quality checks as described in the exercise specification.  Every
    failed check emits a WARNING log message.  When ``halt_on_failure``
    is ``True`` a :class:`DataQualityError` is raised on the first
    failure.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_data.logger import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Raised when a data quality check fails and halt_on_failure is True."""


def _check_or_warn(condition: bool, message: str, halt_on_failure: bool) -> None:
    """
    Emit a warning (or raise) depending on *condition* and *halt_on_failure*.

    :param condition: ``True`` means the check **passed**.
    :type condition: bool
    :param message: Human-readable description of the problem.
    :type message: str
    :param halt_on_failure: When ``True`` raise :class:`DataQualityError`
        on a failed check.
    :type halt_on_failure: bool
    :raises DataQualityError: If *condition* is ``False`` and
        *halt_on_failure* is ``True``.
    """
    if not condition:
        logger.warning("DATA QUALITY ISSUE: %s", message)
        if halt_on_failure:
            raise DataQualityError(message)


# ---------------------------------------------------------------------------
# Basic checks
# ---------------------------------------------------------------------------


def check_non_null_unique_ids(
    df: DataFrame, id_col: str, dataset_name: str, halt_on_failure: bool = False
) -> None:
    """
    Verify that *id_col* contains no NULL values and no duplicates.

    :param df: Input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param id_col: Name of the ID column to check.
    :type id_col: str
    :param dataset_name: Human-readable dataset label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    null_count: int = df.filter(F.col(id_col).isNull()).count()
    _check_or_warn(
        null_count == 0,
        f"[{dataset_name}] Column '{id_col}' has {null_count} NULL value(s).",
        halt_on_failure,
    )

    total: int = df.count()
    distinct: int = df.select(id_col).distinct().count()
    _check_or_warn(
        total == distinct,
        f"[{dataset_name}] Column '{id_col}' has {total - distinct} duplicate value(s).",
        halt_on_failure,
    )


def check_row_count(
    df: DataFrame,
    expected: int,
    dataset_name: str,
    halt_on_failure: bool = False,
) -> None:
    """
    Verify that *df* contains exactly *expected* rows.

    :param df: Input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param expected: Expected row count.
    :type expected: int
    :param dataset_name: Human-readable dataset label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    actual: int = df.count()
    _check_or_warn(
        actual == expected,
        f"[{dataset_name}] Expected {expected} rows, found {actual}.",
        halt_on_failure,
    )


def check_non_negative_numerics(
    df: DataFrame,
    numeric_cols: List[str],
    dataset_name: str,
    halt_on_failure: bool = False,
) -> None:
    """
    Verify that all values in *numeric_cols* are >= 0.

    :param df: Input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param numeric_cols: List of column names to check.
    :type numeric_cols: list[str]
    :param dataset_name: Human-readable dataset label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    for col_name in numeric_cols:
        neg_count: int = df.filter(F.col(col_name) < 0).count()
        _check_or_warn(
            neg_count == 0,
            f"[{dataset_name}] Column '{col_name}' has {neg_count} negative value(s).",
            halt_on_failure,
        )


# ---------------------------------------------------------------------------
# Intermediate checks
# ---------------------------------------------------------------------------


def check_referential_integrity(
    df_three: DataFrame,
    df_one: DataFrame,
    halt_on_failure: bool = False,
) -> None:
    """
    Verify that every ``caller_id`` in *df_three* matches exactly one
    ``id`` in *df_one*.

    :param df_three: Sales dataset (dataset_three).
    :type df_three: pyspark.sql.DataFrame
    :param df_one: Employee expertise dataset (dataset_one).
    :type df_one: pyspark.sql.DataFrame
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    valid_ids = df_one.select(F.col("id").alias("caller_id"))
    orphan_count: int = df_three.join(
        valid_ids, on="caller_id", how="left_anti"
    ).count()
    _check_or_warn(
        orphan_count == 0,
        f"[dataset_three] {orphan_count} caller_id value(s) have no matching id in dataset_one.",
        halt_on_failure,
    )


def check_calls_successful_le_calls_made(
    df: DataFrame, halt_on_failure: bool = False
) -> None:
    """
    Verify that ``calls_successful`` is never greater than ``calls_made``.

    :param df: Employee expertise dataset (dataset_one).
    :type df: pyspark.sql.DataFrame
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    violation_count: int = df.filter(
        F.col("calls_successful") > F.col("calls_made")
    ).count()
    _check_or_warn(
        violation_count == 0,
        f"[dataset_one] {violation_count} row(s) where calls_successful > calls_made.",
        halt_on_failure,
    )


def check_address_format(df: DataFrame, halt_on_failure: bool = False) -> None:
    """
    Verify that the ``address`` column matches the expected format:
    ``[street name], [house number], [zip code]``

    where zip code is four digits, a space, then two uppercase letters
    (e.g. ``"Lindehof 5, 4133 HB, Nederhemert"`` — note that some
    addresses omit the street/number prefix; the check flags those).

    :param df: Personal information dataset (dataset_two).
    :type df: pyspark.sql.DataFrame
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    # Pattern: <street name (alphanum+spaces)>, <house number>, <4digits SPACE 2CAPS>
    address_pattern = r"^[A-Za-z0-9 ]+,\s*\d+,\s*\d{4}\s[A-Z]{2}"
    invalid_count: int = df.filter(~F.col("address").rlike(address_pattern)).count()
    _check_or_warn(
        invalid_count == 0,
        f"[dataset_two] {invalid_count} address(es) do not match the expected format.",
        halt_on_failure,
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_basic_checks(
    df1: DataFrame,
    df2: DataFrame,
    df3: DataFrame,
    halt_on_failure: bool = False,
) -> None:
    """
    Execute all **Basic** data quality checks.

    :param df1: dataset_one DataFrame.
    :type df1: pyspark.sql.DataFrame
    :param df2: dataset_two DataFrame.
    :type df2: pyspark.sql.DataFrame
    :param df3: dataset_three DataFrame.
    :type df3: pyspark.sql.DataFrame
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    logger.info("Running Basic data quality checks …")

    check_non_null_unique_ids(df1, "id", "dataset_one", halt_on_failure)
    check_non_null_unique_ids(df2, "id", "dataset_two", halt_on_failure)
    check_non_null_unique_ids(df3, "id", "dataset_three", halt_on_failure)

    check_row_count(df1, 1000, "dataset_one", halt_on_failure)
    check_row_count(df2, 1000, "dataset_two", halt_on_failure)
    check_row_count(df3, 10000, "dataset_three", halt_on_failure)

    check_non_negative_numerics(
        df1, ["calls_made", "calls_successful"], "dataset_one", halt_on_failure
    )
    check_non_negative_numerics(df2, ["sales_amount"], "dataset_two", halt_on_failure)
    check_non_negative_numerics(df3, ["quantity"], "dataset_three", halt_on_failure)

    logger.info("Basic data quality checks complete.")


def run_intermediate_checks(
    df1: DataFrame,
    df2: DataFrame,
    df3: DataFrame,
    halt_on_failure: bool = False,
) -> None:
    """
    Execute all **Intermediate** data quality checks.

    :param df1: dataset_one DataFrame.
    :type df1: pyspark.sql.DataFrame
    :param df2: dataset_two DataFrame.
    :type df2: pyspark.sql.DataFrame
    :param df3: dataset_three DataFrame.
    :type df3: pyspark.sql.DataFrame
    :param halt_on_failure: Stop execution on failure when ``True``.
    :type halt_on_failure: bool
    """
    logger.info("Running Intermediate data quality checks …")

    check_referential_integrity(df3, df1, halt_on_failure)
    check_calls_successful_le_calls_made(df1, halt_on_failure)
    check_address_format(df2, halt_on_failure)

    logger.info("Intermediate data quality checks complete.")
