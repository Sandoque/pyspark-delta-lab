"""Sobe uma sessão Spark com Delta uma vez, no build, para baixar os jars."""

import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.master("local[1]")
    .appName("aquecer-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
with tempfile.TemporaryDirectory() as pasta:
    spark.range(3).write.format("delta").save(pasta + "/t")
    assert spark.read.format("delta").load(pasta + "/t").count() == 3
spark.stop()
print("Delta ok")
