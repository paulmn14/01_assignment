"""
Entry point for the sales-data pipeline.

:description: Parses command-line arguments, orchestrates data quality
    checks, executes all six transformations, and writes the outputs to
    the specified directory.

"""

import argparse
import sys

from sales_data.data_quality import run_basic_checks, run_intermediate_checks
from sales_data.io_utils import read_csv, write_single_csv
from sales_data.logger import get_logger
from sales_data.spark_session import get_spark_session
from sales_data.transformations import (
    build_best_salesperson_per_country,
    build_department_breakdown,
    build_it_data,
    build_marketing_address_info,
    build_top_3_performers,
    build_top_3_products_netherlands,
)

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
   
    # Parse command-line arguments.

    parser = argparse.ArgumentParser(
        prog="sales-data",
        description="EternalTeleSales Fran van Seb Group – PySpark Analytics Pipeline",
    )
    parser.add_argument(
        "--ds1",
        required=True,
        metavar="PATH",
        help="Path to dataset_one.csv (employee expertise & call stats).",
    )
    parser.add_argument(
        "--ds2",
        required=True,
        metavar="PATH",
        help="Path to dataset_two.csv (personal & sales info).",
    )
    parser.add_argument(
        "--ds3",
        required=True,
        metavar="PATH",
        help="Path to dataset_three.csv (transaction detail).",
    )
    parser.add_argument(
        "--output",
        default="output",
        metavar="DIR",
        help="Root output directory.  Default: %(default)s",
    )
    parser.add_argument(
        "--halt-on-failure",
        action="store_true",
        default=False,
        help="Halt the pipeline if any data quality check fails.",
    )
    parser.add_argument(
        "--skip-intermediate",
        action="store_true",
        default=False,
        help="Skip intermediate data quality checks.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:

    # Orchestrate the full sales-data pipeline.

    args = parse_args(argv)
    logger.info("Pipeline started.")
    logger.info(
        "Inputs  – ds1: %s | ds2: %s | ds3: %s", args.ds1, args.ds2, args.ds3
    )
    logger.info(
        "Options – output: %s | halt_on_failure: %s | skip_intermediate: %s",
        args.output,
        args.halt_on_failure,
        args.skip_intermediate,
    )

    spark = get_spark_session()

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------
    df1 = read_csv(spark, args.ds1)
    df2 = read_csv(spark, args.ds2)
    df3 = read_csv(spark, args.ds3)

    # ------------------------------------------------------------------
    # Data quality
    # ------------------------------------------------------------------
    run_basic_checks(df1, df2, df3, halt_on_failure=args.halt_on_failure)

    if not args.skip_intermediate:
        run_intermediate_checks(df1, df2, df3, halt_on_failure=args.halt_on_failure)

    # ------------------------------------------------------------------
    # Transformations & writes
    # ------------------------------------------------------------------
    out = args.output

    write_single_csv(build_it_data(df1, df2), f"{out}/it_data")
    write_single_csv(build_marketing_address_info(df1, df2), f"{out}/marketing_address_info")
    write_single_csv(build_department_breakdown(df1, df2), f"{out}/department_breakdown")
    write_single_csv(build_top_3_performers(df1, df2), f"{out}/top_3")
    write_single_csv(build_top_3_products_netherlands(df1, df3), f"{out}/top_3_most_sold_per_department_netherlands")
    write_single_csv(build_best_salesperson_per_country(df2, df3), f"{out}/best_salesperson")

    logger.info("Pipeline complete.  All outputs written to: %s", out)
    spark.stop()


if __name__ == "__main__":
    main()
