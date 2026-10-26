"""De/para vendedor -> base, com vigência (FR-06).

A base de um vendedor depende do dia: em 21/10/2026 a 131 saiu da Base Cobertura
e a 134 entrou. `valido_de` é inclusivo e `valido_ate` exclusivo; vazio é aberto.
É o mesmo desenho de uma dimensão de histórico (SCD tipo 2), só que a regra vem
de decisão comercial, num seed versionado, e não de mudança no cadastro.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ler_seed(spark: SparkSession, caminho: str) -> DataFrame:
    df = spark.read.option("header", True).csv(caminho)
    return df.select(
        F.trim("vendedor").alias("vendedor"),
        F.trim("base").alias("base"),
        F.expr("try_cast(nullif(trim(valido_de), '') AS DATE)").alias("valido_de"),
        F.expr("try_cast(nullif(trim(valido_ate), '') AS DATE)").alias("valido_ate"),
    )


def bases_no_dia(seed: DataFrame, dia: date) -> DataFrame:
    """(vendedor, base) que valem no dia."""

    d = F.lit(dia).cast("date")
    return seed.where(
        (F.col("valido_de").isNull() | (F.col("valido_de") <= d))
        & (F.col("valido_ate").isNull() | (d < F.col("valido_ate")))
    ).select("vendedor", "base")
