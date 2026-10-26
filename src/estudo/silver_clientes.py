"""Silver do cadastro (FR-04, FR-05): tipagem, normalização, quarentena e MERGE.

As regras de normalização estão no plan.md. As expressões são colunas do Spark
(sem UDF) sempre que dá: rodam na JVM, sem serializar linha para o Python. A
única UDF é a conferência dos dígitos verificadores, de propósito -- é o exemplo
de quando vale e quanto custa.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from estudo import destino as d

REVENDA = "XPTO"


# ---------------------------------------------------------------------------
# Colunas
# ---------------------------------------------------------------------------


def so_digitos(coluna: Column) -> Column:
    return F.regexp_replace(F.coalesce(coluna, F.lit("")), r"[^0-9]", "")


def fim8(coluna: Column) -> Column:
    """Os 8 últimos dígitos, como o reconhecimento compara; nulo com menos de 8."""

    d = so_digitos(coluna)
    return F.when(F.length(d) >= 8, F.substring(d, -8, 8))


def documento(coluna: Column) -> Column:
    """struct(tipo, numero): CPF de 11 ou CNPJ de 14, a partir do formato do ERP.

    O ERP guarda CPF com 15 dígitos -- 9 primeiros + "0000" + 2 verificadores
    -- e engole o zero da frente. Completado com zeros até 15, a "filial"
    (posições 10 a 13) "0000" é CPF; todo CNPJ real tem filial a partir de 0001.
    """

    d = so_digitos(coluna)
    p = F.lpad(d, 15, "0")
    cpf_do_erp = F.concat(F.substring(p, 1, 9), F.substring(p, 14, 2))
    cnpj = F.substring(p, 2, 14)
    tamanho = F.length(d)
    em_15 = (tamanho >= 12) & (tamanho <= 15)
    tipo = (
        F.when(tamanho == 0, F.lit("vazio"))
        .when(em_15 & (F.substring(p, 10, 4) == "0000"), F.lit("cpf"))
        .when(em_15 & (F.substring(p, 1, 1) == "0"), F.lit("cnpj"))
        .when((tamanho >= 9) & (tamanho <= 11), F.lit("cpf"))
        .otherwise(F.lit("desconhecido"))
    )
    numero = (
        F.when(tipo == "cpf", F.when(em_15, cpf_do_erp).otherwise(F.lpad(d, 11, "0")))
        .when(tipo == "cnpj", cnpj)
    )
    return F.struct(tipo.alias("tipo"), numero.alias("numero"))


def _dv_ok(numero: str | None, tipo: str | None) -> bool | None:
    if not numero or tipo not in ("cpf", "cnpj"):
        return None
    digitos = [int(c) for c in numero]
    if len(set(digitos)) == 1:
        return False
    if tipo == "cpf":
        for n in (9, 10):
            soma = sum(digitos[i] * (n + 1 - i) for i in range(n))
            dv = (soma * 10) % 11 % 10
            if dv != digitos[n]:
                return False
        return True
    pesos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for n in (12, 13):
        p = pesos if n == 12 else [6] + pesos
        soma = sum(digitos[i] * p[i] for i in range(n))
        resto = soma % 11
        dv = 0 if resto < 2 else 11 - resto
        if dv != digitos[n]:
            return False
    return True


documento_valido = F.udf(_dv_ok, BooleanType())


# ---------------------------------------------------------------------------
# Transformação
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Contagens:
    bronze: int
    silver: int
    quarentena: int
    duplicatas_exatas: int

    @property
    def fecha(self) -> bool:
        return self.bronze == self.silver + self.quarentena + self.duplicatas_exatas


def transformar(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze de UM lote -> (silver, quarentena).

    Quarentena, com o motivo: sem código; revenda diferente da XPTO; o mesmo
    código com conteúdo diferente (não dá para escolher qual vale). Duplicata
    EXATA (mesmo conteúdo) não é erro: fica uma e as outras são contadas.
    """

    base = (
        bronze.withColumn("codigo_cliente", F.trim("codigo_cliente"))
        .withColumn("vendedor", F.nullif(F.trim("vendedor"), F.lit("")))
        .withColumn("revenda", F.trim("revenda"))
    )
    motivo = (
        F.when(F.coalesce(F.col("codigo_cliente"), F.lit("")) == "", F.lit("sem_codigo"))
        .when(F.col("revenda") != REVENDA, F.lit("revenda_diferente"))
    )
    marcado = base.withColumn("_motivo", motivo)

    por_codigo = Window.partitionBy("codigo_cliente")
    validos = (
        marcado.where(F.col("_motivo").isNull())
        .withColumn("_conteudos", F.size(F.collect_set("_hash_linha").over(por_codigo)))
        .withColumn("_ordem", F.row_number().over(por_codigo.orderBy("_hash_linha")))
    )
    conflito = validos.where(F.col("_conteudos") > 1).withColumn("_motivo", F.lit("codigo_duplicado_conflitante"))
    unicos = validos.where((F.col("_conteudos") == 1) & (F.col("_ordem") == 1))

    quarentena = (
        marcado.where(F.col("_motivo").isNotNull())
        .unionByName(conflito.drop("_conteudos", "_ordem"))
        .withColumnRenamed("_motivo", "motivo")
    )

    doc = documento(F.col("cnpj_cpf"))
    silver = (
        unicos.withColumn("_doc", doc)
        .select(
            "codigo_cliente",
            "revenda",
            F.trim("nome").alias("nome"),
            F.trim("nome_fantasia").alias("nome_fantasia"),
            F.trim("cidade").alias("cidade"),
            "vendedor",
            F.trim("categoria").alias("categoria"),
            F.expr("try_cast(trim(tabela_preco) AS INT)").alias("tabela_preco"),
            F.col("cnpj_cpf").alias("documento_original"),
            F.col("_doc.tipo").alias("documento_tipo"),
            F.col("_doc.numero").alias("documento"),
            documento_valido(F.col("_doc.numero"), F.col("_doc.tipo")).alias("documento_valido"),
            F.col("telefone").alias("telefone_original"),
            F.col("celular").alias("celular_original"),
            F.col("telefone_b2b").alias("telefone_b2b_original"),
            fim8(F.col("telefone")).alias("telefone_fim8"),
            fim8(F.col("celular")).alias("celular_fim8"),
            fim8(F.col("telefone_b2b")).alias("telefone_b2b_fim8"),
            (F.coalesce(F.trim("cancelado"), F.lit("0")) == "1").alias("cancelado"),
            "_lote",
            "_hash_linha",
        )
    )
    return silver, quarentena


def contar(bronze: DataFrame, silver: DataFrame, quarentena: DataFrame) -> Contagens:
    n_bronze, n_silver, n_quarentena = bronze.count(), silver.count(), quarentena.count()
    return Contagens(n_bronze, n_silver, n_quarentena, n_bronze - n_silver - n_quarentena)


def gravar(spark: SparkSession, silver: DataFrame, destino: str) -> None:
    """MERGE do retrato completo do cadastro, idempotente.

    Atualiza quem mudou (pelo hash da linha), insere quem chegou e apaga quem
    sumiu do cadastro -- a exportação é sempre o cadastro inteiro.

    Com Deletion Vectors ligados (T12), repetir o mesmo retrato não reescreve
    nenhum arquivo, e mudanças pontuais gravam só as linhas tocadas (`numTargetRowsCopied = 0`).
    """

    if not d.existe(spark, destino):
        d.salvar(silver.write.format("delta").option("delta.enableDeletionVectors", "true"), destino)
        return
    tabela = d.tabela(spark, destino)
    props = tabela.detail().select("properties").first()["properties"] or {}
    if props.get("delta.enableDeletionVectors") != "true":
        spark.sql(f"ALTER TABLE {d.nome_sql(destino)} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        tabela = d.tabela(spark, destino)
    (
        tabela.alias("t")
        .merge(silver.alias("s"), "t.codigo_cliente = s.codigo_cliente")
        .whenMatchedUpdateAll(condition="t._hash_linha <> s._hash_linha")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def gravar_quarentena(quarentena: DataFrame, destino: str, lote: str) -> None:
    """A quarentena de um lote é substituída inteira se o lote for reprocessado."""

    d.salvar(
        quarentena.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"_lote = '{lote}'")
        .option("mergeSchema", "true"),
        destino,
    )
