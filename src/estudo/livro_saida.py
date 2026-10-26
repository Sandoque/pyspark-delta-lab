"""Fatia 2 — Livro de Saída da XPTO (FR-12 a FR-15): Bronze, Silver, Gold e Reconciliação.

Resolve e mede os três problemas reais da operação no Livro de Saída:
1. Precisão financeira: valores tipados como `DECIMAL(15,2)` na Silver (sem a
   perda de centavos do `float32` acima de R$ 2 milhões nem o ruído binário do `DOUBLE`);
2. Grão da Nota Fiscal: `chave_nota = revenda|nf|livro|chave_nf` (sem fundir NFs
   de mesma numeração);
3. Reprocessamento por janela: Silver e Gold particionadas por `(ano, mes)` e
   gravadas com `replaceWhere` restrito às competências do lote.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from estudo import bronze as b_base

REVENDA = "XPTO"
COLUNAS = (
    "REVENDA", "NF", "LIVRO", "CHAVE_NF", "DATA_E", "CFOP", "COD_NATUREZA",
    "CODCOMP", "CNPJ", "CODCLI", "COD_PRD_RED", "QTD", "QTD_INTEIRA",
    "QTD_AVULSA", "INCIDENCIA", "VLR_PRODUTO", "VLR_IPI", "VLR_ICMS",
    "VLR_PIS", "VLR_COFINS", "FRETE", "ADIC_FINANCEIRO", "VLR_TOTAL",
    "VLR_TOTAL_NOTA_CALC",
)
ESQUEMA_LANDING = StructType([StructField(c, StringType(), True) for c in COLUNAS])

INDICADORES_MENSAIS = (
    "itens",
    "nfs_distintas",
    "vlr_produto",
    "vlr_icms",
    "vlr_pis",
    "vlr_cofins",
    "vlr_total_nota_calc",
)


# ---------------------------------------------------------------------------
# Bronze (FR-12)
# ---------------------------------------------------------------------------


def com_metadados(df: DataFrame, lote: str, arquivo: str) -> DataFrame:
    conteudo = F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("\u0000")) for c in COLUNAS])
    return (
        df.withColumn("_lote", F.lit(lote))
        .withColumn("_arquivo", F.lit(arquivo))
        .withColumn("_hash_linha", F.sha2(conteudo, 256))
        .withColumn("_ingerido_em", F.current_timestamp())
    )


def ler_landing(spark: SparkSession, caminho_csv: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema(ESQUEMA_LANDING)
        .csv(str(caminho_csv))
    )


def ingerir_bronze(spark: SparkSession, caminho_csv: Path, destino: str) -> b_base.ResultadoBronze:
    caminho_csv = Path(caminho_csv)
    manifesto = b_base.ler_manifesto(caminho_csv)
    lote = b_base.sha256_do_arquivo(caminho_csv)
    if lote != manifesto["sha256"]:
        raise ValueError(f"sha256 do arquivo não confere com o manifesto: {caminho_csv.name}")

    if DeltaTable.isDeltaTable(spark, destino):
        ja = spark.read.format("delta").load(destino).where(F.col("_lote") == lote).limit(1).count() > 0
        if ja:
            return b_base.ResultadoBronze(lote, int(manifesto["linhas"]), 0, True)

    df = com_metadados(ler_landing(spark, caminho_csv), lote, caminho_csv.name)
    linhas = df.count()
    if linhas != manifesto["linhas"]:
        raise ValueError(f"o arquivo tem {linhas} linhas e o manifesto diz {manifesto['linhas']}")

    df.write.format("delta").mode("append").save(destino)
    return b_base.ResultadoBronze(lote, linhas, linhas, False)


# ---------------------------------------------------------------------------
# Expressões e Silver (FR-13)
# ---------------------------------------------------------------------------


def valor_decimal(coluna: Column) -> Column:
    """Converte texto numérico para DECIMAL(15,2) arredondado ao centavo."""
    limpo = F.nullif(F.trim(coluna), F.lit(""))
    return F.round(limpo.try_cast("double"), 2).try_cast("decimal(15,2)")


def chave_nota(revenda: Column, nf: Column, livro: Column, chave_nf: Column) -> Column:
    """Grão verdadeiro da nota fiscal (Correção 4): revenda|nf|livro|chave_nf."""
    return F.concat_ws(
        "|",
        F.coalesce(F.trim(revenda), F.lit("")),
        F.coalesce(F.trim(nf), F.lit("")),
        F.coalesce(F.trim(livro), F.lit("")),
        F.coalesce(F.trim(chave_nf), F.lit("")),
    )


@dataclass(frozen=True)
class ContagensLivro:
    bronze: int
    silver: int
    quarentena: int
    duplicatas_exatas: int

    @property
    def fecha(self) -> bool:
        return self.bronze == self.silver + self.quarentena + self.duplicatas_exatas


def transformar_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze de um lote -> (silver tipada em DECIMAL(15,2), quarentena com motivo)."""

    nf_int = F.expr("try_cast(trim(NF) AS INT)")
    data_e = F.expr("try_cast(trim(DATA_E) AS DATE)")
    vlr_calc_txt = F.nullif(F.trim(F.col("VLR_TOTAL_NOTA_CALC")), F.lit(""))
    vlr_calc_dec = valor_decimal(F.col("VLR_TOTAL_NOTA_CALC"))

    motivo = (
        F.when(F.trim(F.col("REVENDA")) != REVENDA, F.lit("revenda_diferente"))
        .when(nf_int.isNull(), F.lit("sem_nf"))
        .when(data_e.isNull(), F.lit("sem_data_emissao"))
        .when(vlr_calc_txt.isNotNull() & vlr_calc_dec.isNull(), F.lit("valor_invalido"))
    )
    marcado = bronze_df.withColumn("_motivo", motivo)
    quarentena = marcado.where(F.col("_motivo").isNotNull()).withColumnRenamed("_motivo", "motivo")

    janela_dup = Window.partitionBy("_hash_linha").orderBy(F.lit(1))
    validos = (
        marcado.where(F.col("_motivo").isNull())
        .withColumn("_ordem", F.row_number().over(janela_dup))
        .where(F.col("_ordem") == 1)
    )

    zero_dec = F.lit(0).cast("decimal(15,2)")
    silver = validos.select(
        F.trim("REVENDA").alias("revenda"),
        nf_int.alias("nf"),
        F.coalesce(F.trim("LIVRO"), F.lit("")).alias("livro"),
        F.coalesce(F.trim("CHAVE_NF"), F.lit("")).alias("chave_nf"),
        chave_nota(F.col("REVENDA"), nf_int.cast("string"), F.col("LIVRO"), F.col("CHAVE_NF")).alias("chave_nota"),
        data_e.alias("data_emissao"),
        F.year(data_e).alias("ano"),
        F.month(data_e).alias("mes"),
        F.expr("try_cast(trim(CFOP) AS INT)").alias("cfop"),
        F.expr("try_cast(trim(COD_NATUREZA) AS INT)").alias("cod_natureza"),
        F.expr("try_cast(trim(CODCOMP) AS INT)").alias("codcomp"),
        F.trim("CNPJ").alias("cnpj"),
        F.trim("CODCLI").alias("codcli"),
        F.expr("try_cast(trim(COD_PRD_RED) AS INT)").alias("cod_prd_red"),
        F.coalesce(F.expr("try_cast(trim(QTD_INTEIRA) AS INT)"), F.lit(0)).alias("qtd_inteira"),
        F.coalesce(valor_decimal(F.col("VLR_PRODUTO")), zero_dec).alias("vlr_produto"),
        F.coalesce(valor_decimal(F.col("VLR_IPI")), zero_dec).alias("vlr_ipi"),
        F.coalesce(valor_decimal(F.col("VLR_ICMS")), zero_dec).alias("vlr_icms"),
        F.coalesce(valor_decimal(F.col("VLR_PIS")), zero_dec).alias("vlr_pis"),
        F.coalesce(valor_decimal(F.col("VLR_COFINS")), zero_dec).alias("vlr_cofins"),
        F.coalesce(valor_decimal(F.col("FRETE")), zero_dec).alias("frete"),
        F.coalesce(valor_decimal(F.col("VLR_TOTAL")), zero_dec).alias("vlr_total"),
        F.coalesce(vlr_calc_dec, zero_dec).alias("vlr_total_nota_calc"),
        "_lote",
        "_hash_linha",
    )
    return silver, quarentena


def contar(bronze_df: DataFrame, silver_df: DataFrame, quarentena_df: DataFrame) -> ContagensLivro:
    n_b, n_s, n_q = bronze_df.count(), silver_df.count(), quarentena_df.count()
    return ContagensLivro(n_b, n_s, n_q, n_b - n_s - n_q)


def _predicado_competencias(df: DataFrame) -> tuple[str, list[str]]:
    pares = sorted(
        (int(r["ano"]), int(r["mes"]))
        for r in df.select("ano", "mes").distinct().collect()
    )
    if not pares:
        raise ValueError("Nenhuma competência (ano, mes) encontrada para gravar.")
    predicado = " OR ".join(f"(ano = {a} AND mes = {m})" for a, m in pares)
    nomes = [f"{a:04d}-{m:02d}" for a, m in pares]
    return predicado, nomes


def gravar_silver_janela(silver_df: DataFrame, destino: str) -> list[str]:
    """Substitui apenas as partições `(ano, mes)` presentes no lote (`replaceWhere`).

    Faz `repartition('ano', 'mes')` antes da escrita para que cada tarefa mantenha
    apenas um escritor Parquet aberto por vez (evitando N_cores × 30 buffers na
    heap da JVM e gerando 1 arquivo por competência em vez de N_cores × 30).
    """
    predicado, competencias = _predicado_competencias(silver_df)
    (
        silver_df.repartition("ano", "mes")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicado)
        .partitionBy("ano", "mes")
        .save(destino)
    )
    return competencias


def gravar_quarentena(quarentena_df: DataFrame, destino: str, lote: str) -> None:
    (
        quarentena_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_lote = '{lote}'")
        .option("mergeSchema", "true")
        .save(destino)
    )


# ---------------------------------------------------------------------------
# Gold mensal e Reconciliação (FR-14, FR-15)
# ---------------------------------------------------------------------------


def calcular_gold_mensal(silver_df: DataFrame) -> DataFrame:
    """Agrega por `(revenda, ano, mes)` em DECIMAL(15,2) e mede a perda do float32."""
    agrupado = (
        silver_df.groupBy("revenda", "ano", "mes")
        .agg(
            F.count("*").alias("itens"),
            F.countDistinct("chave_nota").alias("nfs_distintas"),
            F.countDistinct("nf").alias("nfs_so_numero"),
            F.sum("vlr_produto").cast("decimal(15,2)").alias("vlr_produto"),
            F.sum("vlr_icms").cast("decimal(15,2)").alias("vlr_icms"),
            F.sum("vlr_pis").cast("decimal(15,2)").alias("vlr_pis"),
            F.sum("vlr_cofins").cast("decimal(15,2)").alias("vlr_cofins"),
            F.sum("vlr_total_nota_calc").cast("decimal(15,2)").alias("vlr_total_nota_calc"),
            # Simula a soma em precisão simples (float32) que existia nos resumos antes da Correção 1
            F.round(
                F.sum(F.col("vlr_total_nota_calc").cast("float")).cast("float").cast("double"), 2
            )
            .cast("decimal(15,2)")
            .alias("vlr_total_float32"),
        )
        .withColumn(
            "diferenca_float32",
            (F.col("vlr_total_nota_calc") - F.col("vlr_total_float32")).cast("decimal(15,2)"),
        )
        .orderBy("ano", "mes")
    )
    return agrupado


def gravar_gold_janela(gold_df: DataFrame, destino: str) -> list[str]:
    predicado, competencias = _predicado_competencias(gold_df)
    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicado)
        .partitionBy("ano", "mes")
        .save(destino)
    )
    return competencias


def _fmt_dec(val) -> str:
    if val is None:
        return "0.00"
    return f"{Decimal(str(val)):.2f}"


def reconciliar_mensal(gold_df: DataFrame, controles_mensais: dict) -> list[dict]:
    """Compara cada competência e indicador da Gold com o SQL de produção ao centavo."""
    spark_por_comp = {
        f"{int(r['ano']):04d}-{int(r['mes']):02d}": r.asDict()
        for r in gold_df.collect()
    }
    resultados = []
    for comp, sql in sorted(controles_mensais.items()):
        sp = spark_por_comp.get(comp, {})
        dif_itens = int(sp.get("itens", -1)) == int(sql["itens"])
        dif_nfs = int(sp.get("nfs_distintas", -1)) == int(sql["nfs_distintas"])
        dec_ok = all(
            _fmt_dec(sp.get(c)) == _fmt_dec(sql[c])
            for c in ("vlr_produto", "vlr_icms", "vlr_pis", "vlr_cofins", "vlr_total_nota_calc")
        )
        resultados.append({
            "competencia": comp,
            "itens_spark": sp.get("itens"),
            "itens_sql": sql["itens"],
            "nfs_spark": sp.get("nfs_distintas"),
            "nfs_sql": sql["nfs_distintas"],
            "vlr_total_spark": _fmt_dec(sp.get("vlr_total_nota_calc")),
            "vlr_total_sql": _fmt_dec(sql["vlr_total_nota_calc"]),
            "vlr_total_float32": _fmt_dec(sp.get("vlr_total_float32")),
            "diferenca_float32": _fmt_dec(sp.get("diferenca_float32")),
            "bate": bool(dif_itens and dif_nfs and dec_ok),
        })
    return resultados

