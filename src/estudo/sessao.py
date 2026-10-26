"""Sessão Spark com Delta Lake, igual no job, nos testes e no notebook."""

from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

JARS_CACHE_CONTAINER = Path("/root/.ivy2.5.2/jars")


def criar_sessao(
    nome: str = "estudo",
    master: str = "local[*]",
    particoes: int = 8,
    memoria_driver: str = "4g",
) -> SparkSession:
    builder = (
        SparkSession.builder.master(master)
        .appName(nome)
        .config("spark.driver.memory", memoria_driver)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Dezenas de milhares de linhas não pedem as 200 partições padrão do shuffle.
        .config("spark.sql.shuffle.partitions", str(particoes))
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Se os JARs do Delta já foram baixados no build da imagem (/root/.ivy2.5.2/jars),
    # carrega direto via spark.jars sem rodar a resolução verbosa do Ivy a cada sessão.
    jars_locais = sorted(str(p) for p in JARS_CACHE_CONTAINER.glob("*.jar")) if JARS_CACHE_CONTAINER.exists() else []
    if jars_locais:
        spark = builder.config("spark.jars", ",".join(jars_locais)).getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
