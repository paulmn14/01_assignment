"""
I/O utility functions for the sales-data pipeline.

:description: Provides wrappers around PySpark CSV reading and
    writing so that all I/O configuration is in one place.

"""

import glob
import os
import shutil

from pyspark.sql import DataFrame, SparkSession

from sales_data.logger import get_logger

logger = get_logger(__name__)


def read_csv(spark: SparkSession, path: str) -> DataFrame:
  
    # Read a CSV file into a PySpark DataFrame with header and schema inference.
    
    logger.info("Reading CSV from: %s", path)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def write_single_csv(df: DataFrame, output_dir: str) -> None:
  
    # Write *df* to a single, cleanly named CSV file inside *output_dir*.

    logger.info("Writing CSV to: %s", output_dir)

    tmp_dir = os.path.join(output_dir, "_spark_tmp")

    # Write to a temporary sub-directory first.
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(tmp_dir)
    )

    # Locate the single part file Spark produced.
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        logger.warning("No part file found in %s - output may be empty.", tmp_dir)
        return

    # Derive a clean filename from the output directory name, e.g.
    # "output/it_data"  ->  "output/it_data/it_data.csv"
    clean_name = os.path.basename(output_dir.rstrip("/\\")) + ".csv"
    final_path = os.path.join(output_dir, clean_name)

    # Remove a previous run's file if present.
    if os.path.exists(final_path):
        os.remove(final_path)

    shutil.move(part_files[0], final_path)

    # Clean up the temporary Spark directory.
    shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info("Output written to: %s", final_path)