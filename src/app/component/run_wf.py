
from datetime import datetime
from pyspark.sql import Row

def run_wf(spark, database, ymd):
    """Enter a new entry into the ticker table."""

    # write execution timestamp
    timestamp = datetime.now().isoformat()
    df = spark.createDataFrame([Row(timestamp=timestamp)])
    df.write.format('hive').mode('append').saveAsTable(f'{database}.ticker')
