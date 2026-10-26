"""O mesmo pipeline gravando em TABELAS do catálogo, como no Databricks (fatia 3).

No contêiner o catálogo é o local do Spark; no Databricks, o Unity Catalog. O que
se prova aqui é que bronze, silver (MERGE com deletion vectors), quarentena e gold
funcionam por nome de tabela, e não só por caminho.
"""

from datetime import date
from pathlib import Path

import pytest

from estudo import bronze, de_para, destino, gold_reconhecimento, qualidade, silver_clientes

RAIZ = Path(__file__).resolve().parents[1]
AMOSTRA = RAIZ / "seeds" / "amostra_sintetica" / "clientes_xpto_sintetico.csv"
SEED = str(RAIZ / "seeds" / "de_para_vendedor_base.csv")


@pytest.mark.parametrize(
    "valor, caminho",
    [
        ("/work/data/delta/silver", True),
        ("/Volumes/workspace/lab/landing", True),
        ("dbfs:/tmp/x", True),
        ("C:/dados/x", True),
        ("workspace.lab.silver_clientes", False),
        ("lab.silver_clientes", False),
    ],
)
def test_caminho_ou_tabela(valor, caminho):
    assert destino.eh_caminho(valor) is caminho


@pytest.fixture
def esquema(spark):
    nome = "lab_teste_destino"
    spark.sql(f"DROP SCHEMA IF EXISTS {nome} CASCADE")
    spark.sql(f"CREATE SCHEMA {nome}")
    yield nome
    spark.sql(f"DROP SCHEMA IF EXISTS {nome} CASCADE")


def test_fatia1_por_nome_de_tabela(spark, esquema):
    manifesto = bronze.ler_manifesto(AMOSTRA)
    dia = date.fromisoformat(manifesto["dia_referencia"])
    t = {nome: f"{esquema}.{nome}" for nome in ("bronze", "silver", "quarentena", "gold")}

    primeira = bronze.ingerir(spark, AMOSTRA, t["bronze"])
    segunda = bronze.ingerir(spark, AMOSTRA, t["bronze"])
    assert (primeira.linhas_inseridas, segunda.ja_ingerido) == (185, True)

    lote = destino.ler(spark, t["bronze"]).where(f"_lote = '{primeira.lote}'")
    silver, quarentena = silver_clientes.transformar(lote)
    silver_clientes.gravar(spark, silver, t["silver"])
    silver_clientes.gravar(spark, silver, t["silver"])   # de novo: o MERGE não muda nada
    silver_clientes.gravar_quarentena(quarentena, t["quarentena"], primeira.lote)

    assert destino.ler(spark, t["silver"]).count() == 180
    assert destino.ler(spark, t["quarentena"]).count() == 4
    props = destino.tabela(spark, t["silver"]).detail().first()["properties"]
    assert props.get("delta.enableDeletionVectors") == "true"

    seed = de_para.ler_seed(spark, SEED)
    gold = gold_reconhecimento.calcular(destino.ler(spark, t["silver"]), de_para.bases_no_dia(seed, dia), dia)
    gold_reconhecimento.gravar(gold, t["gold"], dia)

    reconciliacao = qualidade.reconciliar(destino.ler(spark, t["gold"]), manifesto["controles"])
    assert len(reconciliacao) == 12 and all(r["bate"] for r in reconciliacao), reconciliacao
