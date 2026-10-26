"""Onde uma tabela Delta mora: num caminho (contêiner local) ou no catálogo (Databricks).

O mesmo pipeline roda nos dois lugares. No contêiner, o destino é uma pasta
(`/work/data/delta/silver_clientes`); no Databricks, uma tabela do Unity Catalog
(`workspace.lab.silver_clientes`). Estas funções escolhem a API certa pela forma do
destino, e o resto do código não precisa saber onde está rodando.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

_PREFIXOS_DE_CAMINHO = ("/", "file:", "dbfs:", "s3:", "s3a:", "abfss:", "gs:")


def eh_caminho(destino: str) -> bool:
    return destino.startswith(_PREFIXOS_DE_CAMINHO) or (len(destino) > 1 and destino[1] == ":")


def existe(spark: SparkSession, destino: str) -> bool:
    if eh_caminho(destino):
        return DeltaTable.isDeltaTable(spark, destino)
    return spark.catalog.tableExists(destino)


def ler(spark: SparkSession, destino: str) -> DataFrame:
    if eh_caminho(destino):
        return spark.read.format("delta").load(destino)
    return spark.read.table(destino)


def tabela(spark: SparkSession, destino: str) -> DeltaTable:
    if eh_caminho(destino):
        return DeltaTable.forPath(spark, destino)
    return DeltaTable.forName(spark, destino)


def nome_sql(destino: str) -> str:
    """Como o destino aparece num comando SQL (ALTER TABLE, DESCRIBE HISTORY)."""

    return f"delta.`{destino}`" if eh_caminho(destino) else destino


def salvar(escritor: DataFrameWriter, destino: str) -> None:
    if eh_caminho(destino):
        escritor.save(destino)
    else:
        escritor.saveAsTable(destino)
