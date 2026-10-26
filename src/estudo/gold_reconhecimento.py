"""Gold (FR-07): quantos clientes o WhatsApp reconhece pelo número, por base e dia.

A regra é a do sistema de produção: um número -- os 8 últimos dígitos --
reconhece o cliente quando pertence a UM só cliente da XPTO nos campos
considerados. A unicidade olha o cadastro INTEIRO (cancelados inclusive), como
a produção; a contagem é só dos clientes ativos da base no dia.

Indicadores por base:
* clientes_ativos
* reconhecidos_so_telefone          -- regra até 20/10 (só o campo telefone)
* reconhecidos_telefone_ou_celular  -- regra nova
* com_numero_repetido               -- têm algum número de mais de um cliente
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _pares(silver: DataFrame, campos: tuple[str, ...]) -> DataFrame:
    """(codigo_cliente, fim) para cada número presente nos campos, sem repetir."""

    partes = [
        silver.select("codigo_cliente", F.col(c).alias("fim")).where(F.col("fim").isNotNull())
        for c in campos
    ]
    pares = partes[0]
    for parte in partes[1:]:
        pares = pares.unionByName(parte)
    return pares.distinct()


def _donos(pares: DataFrame) -> DataFrame:
    return pares.groupBy("fim").agg(F.countDistinct("codigo_cliente").alias("donos"))


def _clientes_com(pares: DataFrame, donos: DataFrame, condicao) -> DataFrame:
    return pares.join(donos, "fim").where(condicao).select("codigo_cliente").distinct()


def calcular(silver: DataFrame, bases: DataFrame, dia: date) -> DataFrame:
    so_tel = _pares(silver, ("telefone_fim8",))
    tel_cel = _pares(silver, ("telefone_fim8", "celular_fim8"))
    donos_tel, donos_tel_cel = _donos(so_tel), _donos(tel_cel)

    reconhecidos_tel = _clientes_com(so_tel, donos_tel, F.col("donos") == 1).withColumn("rec_tel", F.lit(1))
    reconhecidos_tc = _clientes_com(tel_cel, donos_tel_cel, F.col("donos") == 1).withColumn("rec_tc", F.lit(1))
    repetidos = _clientes_com(tel_cel, donos_tel_cel, F.col("donos") > 1).withColumn("rep", F.lit(1))

    ativos = silver.where(~F.col("cancelado")).join(bases, "vendedor").select("codigo_cliente", "base")
    return (
        ativos.join(reconhecidos_tel, "codigo_cliente", "left")
        .join(reconhecidos_tc, "codigo_cliente", "left")
        .join(repetidos, "codigo_cliente", "left")
        .groupBy("base")
        .agg(
            F.count("*").alias("clientes_ativos"),
            F.count("rec_tel").alias("reconhecidos_so_telefone"),
            F.count("rec_tc").alias("reconhecidos_telefone_ou_celular"),
            F.count("rep").alias("com_numero_repetido"),
        )
        .withColumn("dia_referencia", F.lit(dia).cast("date"))
        .orderBy("base")
    )


def gravar(gold: DataFrame, destino: str, dia: date) -> None:
    """Substitui só o dia: recalcular um dia não mexe nos outros."""

    (
        gold.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"dia_referencia = DATE'{dia.isoformat()}'")
        .partitionBy("dia_referencia")
        .save(destino)
    )
