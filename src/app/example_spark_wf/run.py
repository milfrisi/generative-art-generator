#!/usr/bin/env python3
"""
Main script of the workflow.

Note: it is important that its extension is .py
Spark needs to know that it is a pyspark script.
"""
import click
from pyspark.sql import SparkSession

from lib.logs import quiet_logs
from app.example_spark_wf.workflow import workflow

@click.command()
@click.option('--app-name', help="Name of the Spark application")
@click.option('--database', help="Database to use")
@click.option('--crunch-date', help="Crunch date for current run. Should be in YYYYMMDD format")
def run_workflow(app_name, database, crunch_date):
    """Pass parameters and spark object to actual workflow."""

    # create spark context
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    quiet_logs(spark.sparkContext)

    # run workflow
    workflow(spark, database=database, ymd=int(crunch_date))

if __name__ == "__main__":
    run_workflow()

