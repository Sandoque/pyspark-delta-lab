"""Sessão Spark com Delta Lake, igual no job, nos testes e no notebook."""

from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Onde o Ivy do Spark 4 deixa os jars do Delta: no HOME de quem roda (/root no
# contêiner, /home/runner no CI). Caminho fixo em /root quebrava no CI, que não
# roda como root e não pode nem olhar dentro de /root.
JARS_CACHE = Path.home() / ".ivy2.5.2" / "jars"


def _jars_locais() -> list[str]:
    try:
        return sorted(str(p) for p in JARS_CACHE.glob("*.jar")) if JARS_CACHE.is_dir() else []
    except OSError:  # sem permissão para listar: resolve pelo Ivy, como na primeira vez
        return []


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
    # Se os JARs do Delta já estão em ~/.ivy2.5.2/jars (build da imagem ou cache do CI),
    # carrega direto via spark.jars sem rodar a resolução verbosa do Ivy a cada sessão.
    jars_locais = _jars_locais()
    if jars_locais:
        spark = builder.config("spark.jars", ",".join(jars_locais)).getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
