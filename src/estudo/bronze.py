"""Bronze (FR-03): o arquivo bruto vira tabela Delta, tudo como texto.

Bronze é cópia fiel do que chegou. Tipar e limpar é trabalho da silver. O que a
bronze acrescenta são os metadados que permitem auditar e reprocessar:

* `_lote`        sha256 do arquivo -- a mesma exportação tem sempre o mesmo lote;
* `_arquivo`     nome do arquivo de origem;
* `_hash_linha`  sha256 do conteúdo da linha (para achar duplicata exata);
* `_ingerido_em` quando entrou.

IDEMPOTENTE PELO LOTE: ingerir de novo o mesmo arquivo não acrescenta nada.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

COLUNAS = (
    "revenda", "codigo_cliente", "nome", "nome_fantasia", "cidade", "cnpj_cpf",
    "vendedor", "categoria", "tabela_preco", "telefone", "celular", "telefone_b2b",
    "cancelado",
)
ESQUEMA_LANDING = StructType([StructField(c, StringType(), True) for c in COLUNAS])


@dataclass(frozen=True)
class ResultadoBronze:
    lote: str
    linhas_no_arquivo: int
    linhas_inseridas: int
    ja_ingerido: bool


def sha256_do_arquivo(caminho: Path) -> str:
    return hashlib.sha256(Path(caminho).read_bytes()).hexdigest()


def ler_manifesto(caminho_csv: Path) -> dict:
    return json.loads(Path(caminho_csv).with_suffix(".json").read_text(encoding="utf-8"))


def ler_landing(spark: SparkSession, caminho_csv: Path) -> DataFrame:
    # O csv do Python dobra a aspas dentro do campo ("") -- daí escape = '"'.
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema(ESQUEMA_LANDING)
        .csv(str(caminho_csv))
    )


def com_metadados(df: DataFrame, lote: str, arquivo: str) -> DataFrame:
    conteudo = F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("\u0000")) for c in COLUNAS])
    return (
        df.withColumn("_lote", F.lit(lote))
        .withColumn("_arquivo", F.lit(arquivo))
        .withColumn("_hash_linha", F.sha2(conteudo, 256))
        .withColumn("_ingerido_em", F.current_timestamp())
    )


def ingerir(spark: SparkSession, caminho_csv: Path, destino: str) -> ResultadoBronze:
    """Arquivo da landing -> Delta bronze. Confere sha256 e linhas contra o manifesto."""

    caminho_csv = Path(caminho_csv)
    manifesto = ler_manifesto(caminho_csv)
    lote = sha256_do_arquivo(caminho_csv)
    if lote != manifesto["sha256"]:
        raise ValueError(f"sha256 do arquivo não confere com o manifesto: {caminho_csv.name}")

    df = com_metadados(ler_landing(spark, caminho_csv), lote, caminho_csv.name)
    linhas = df.count()
    if linhas != manifesto["linhas"]:
        raise ValueError(f"o arquivo tem {linhas} linhas e o manifesto diz {manifesto['linhas']}")

    if DeltaTable.isDeltaTable(spark, destino):
        ja = spark.read.format("delta").load(destino).where(F.col("_lote") == lote).limit(1).count() > 0
        if ja:
            return ResultadoBronze(lote, linhas, 0, True)
    df.write.format("delta").mode("append").save(destino)
    return ResultadoBronze(lote, linhas, linhas, False)
