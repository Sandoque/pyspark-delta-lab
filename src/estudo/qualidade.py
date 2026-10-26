"""Checagens de qualidade da silver e reconciliação com a produção (FR-05, FR-08)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INDICADORES = (
    "clientes_ativos",
    "reconhecidos_so_telefone",
    "reconhecidos_telefone_ou_celular",
    "com_numero_repetido",
)


def checar_silver(silver: DataFrame) -> dict:
    linha = silver.agg(
        F.count("*").alias("linhas"),
        F.countDistinct("codigo_cliente").alias("codigos"),
        F.sum(F.when(F.col("documento_tipo") == "cpf", 1).otherwise(0)).alias("cpf"),
        F.sum(F.when(F.col("documento_tipo") == "cnpj", 1).otherwise(0)).alias("cnpj"),
        F.sum(F.when(F.col("documento_tipo") == "vazio", 1).otherwise(0)).alias("documento_vazio"),
        F.sum(F.when(F.col("documento_tipo") == "desconhecido", 1).otherwise(0)).alias("documento_desconhecido"),
        F.sum(F.when(F.col("documento_valido") == False, 1).otherwise(0)).alias("documento_dv_invalido"),  # noqa: E712
        F.sum(F.when(F.col("telefone_fim8").isNull() & F.col("celular_fim8").isNull(), 1).otherwise(0))
        .alias("sem_telefone_nem_celular"),
        F.sum(F.when(F.col("cancelado"), 1).otherwise(0)).alias("cancelados"),
    ).first().asDict()
    linha["codigo_unico"] = linha["linhas"] == linha["codigos"]
    documentos_repetidos = (
        silver.where(F.col("documento").isNotNull()).groupBy("documento").count().where("count > 1").count()
    )
    linha["documentos_em_mais_de_um_cliente"] = documentos_repetidos
    return linha


def reconciliar(gold: DataFrame, controles: dict) -> list[dict]:
    """Cada indicador da gold contra o número do SQL de produção, no mesmo retrato."""

    spark_por_base = {r["base"]: r.asDict() for r in gold.collect()}
    resultado = []
    for base, sql in controles.items():
        spark = spark_por_base.get(base, {})
        for indicador in INDICADORES:
            resultado.append({
                "base": base,
                "indicador": indicador,
                "spark": spark.get(indicador),
                "sql": sql.get(indicador),
                "bate": spark.get(indicador) == sql.get(indicador),
            })
    return resultado
