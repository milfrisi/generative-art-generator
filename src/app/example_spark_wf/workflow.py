from pyspark.sql import Row

from lib.timestamp_tools import get_timestamp


def workflow(spark, database, ymd):
    """Enter a new entry into the ticker table."""

    # write execution timestamp
    timestamp = get_timestamp()
    df = spark.createDataFrame([Row(timestamp=timestamp)])
    df.write.format("hive").mode("append").saveAsTable(f"{database}.ticker")

