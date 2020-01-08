#!/usr/bin/env python3
"""
Main script of the workflow.

Note: it is important that its extension is .py for Spark to know that it is a
pyspark script.
"""
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
import sys


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("yarn").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name", help="Name of the Spark application")
    parser.add_argument(
        "--crunch_date", "-c", help="Crunch date for current run. Should be in YYYYMMDD format"
    )
    parser.add_argument(
        "--src_file", "-s", help="Path to the source code of the wf packaged as a zip file"
    )
    parser.add_argument("--database", "-d", help="Database to use")
    params = parser.parse_args(sys.argv[1:])

    # create spark context
    spark = SparkSession.builder.appName(params.app_name).enableHiveSupport().getOrCreate()
    quiet_logs(spark.sparkContext)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # add project code
    spark.sparkContext.addPyFile(params.src_file)

    # We need to import the project's code after the addPyFile above
    from app.component.run_wf import run_wf

    # run wf
    run_wf(spark, database=params.database, ymd=int(params.crunch_date))
