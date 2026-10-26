"""Teste de ponta a ponta sobre a amostra sintética (T14)."""

from datetime import date
from pathlib import Path

from estudo import bronze, de_para, gold_reconhecimento, qualidade, silver_clientes

RAIZ = Path(__file__).resolve().parents[1]
AMOSTRA = RAIZ / "seeds" / "amostra_sintetica" / "clientes_xpto_sintetico.csv"
SEED = str(RAIZ / "seeds" / "de_para_vendedor_base.csv")


def test_fatia1_ponta_a_ponta_com_amostra_sintetica(spark, tmp_path):
    manifesto = bronze.ler_manifesto(AMOSTRA)
    dia = date.fromisoformat(manifesto["dia_referencia"])

    r_bronze = bronze.ingerir(spark, AMOSTRA, str(tmp_path / "bronze"))
    assert r_bronze.linhas_no_arquivo == 185
    assert not r_bronze.ja_ingerido

    lote_bronze = spark.read.format("delta").load(str(tmp_path / "bronze"))
    silver, quarentena = silver_clientes.transformar(lote_bronze)
    contagens = silver_clientes.contar(lote_bronze, silver, quarentena)

    assert (contagens.bronze, contagens.silver, contagens.quarentena, contagens.duplicatas_exatas) == (185, 180, 4, 1)
    assert contagens.fecha

    silver_clientes.gravar(spark, silver, str(tmp_path / "silver"))
    silver_clientes.gravar_quarentena(quarentena, str(tmp_path / "quarentena"), r_bronze.lote)

    seed = de_para.ler_seed(spark, SEED)
    gold = gold_reconhecimento.calcular(silver, de_para.bases_no_dia(seed, dia), dia)
    gold_reconhecimento.gravar(gold, str(tmp_path / "gold"), dia)

    reconciliacao = qualidade.reconciliar(gold, manifesto["controles"])
    assert len(reconciliacao) == 12
    assert all(r["bate"] for r in reconciliacao), reconciliacao

