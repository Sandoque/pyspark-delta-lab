"""Testes da Fatia 2: Livro de Saída (DECIMAL(15,2), chave da NF, quarentena e replaceWhere por janela)."""

from decimal import Decimal
from pathlib import Path

from estudo import bronze as b_base, livro_saida as ls

RAIZ = Path(__file__).resolve().parents[1]
AMOSTRA_LIVRO = RAIZ / "seeds" / "amostra_sintetica" / "livro_saida_xpto_sintetico.csv"


def _bronze_livro(spark, *linhas):
    base = {c: None for c in ls.COLUNAS}
    base_padrao = {
        "REVENDA": "XPTO",
        "NF": "100",
        "LIVRO": "S",
        "CHAVE_NF": "3526100001",
        "DATA_E": "2026-10-15",
        "CFOP": "5102",
        "QTD_INTEIRA": "1",
        "VLR_PRODUTO": "10.00",
        "VLR_ICMS": "1.80",
        "VLR_PIS": "0.17",
        "VLR_COFINS": "0.76",
        "VLR_TOTAL": "10.00",
        "VLR_TOTAL_NOTA_CALC": "10.00",
    }
    df = spark.createDataFrame(
        [{**base, **base_padrao, **linha} for linha in linhas],
        schema=ls.ESQUEMA_LANDING,
    )
    return ls.com_metadados(df, "lote-livro-teste", "teste_livro.csv")


def test_chave_nota_separa_nfs_de_mesmo_numero_e_soma_decimal_exata(spark):
    # Duas notas diferentes com o MESMO número (NF 8) em chaves distintas + valores que
    # perderiam precisão em float32 acima de R$ 2 milhões.
    lote = _bronze_livro(
        spark,
        {"NF": "8", "CHAVE_NF": "CHAVE_A", "DATA_E": "2026-09-10", "VLR_TOTAL_NOTA_CALC": "15958100.13"},
        {"NF": "8", "CHAVE_NF": "CHAVE_B", "DATA_E": "2026-09-20", "VLR_TOTAL_NOTA_CALC": "6.87"},
    )
    silver, quarentena = ls.transformar_silver(lote)
    assert quarentena.count() == 0

    (gold,) = ls.calcular_gold_mensal(silver).collect()
    assert gold["itens"] == 2
    assert gold["nfs_so_numero"] == 1
    assert gold["nfs_distintas"] == 2  # Correção 4: NF 8 com duas chaves = 2 notas
    assert gold["vlr_total_nota_calc"] == Decimal("15958107.00")


def test_quarentena_livro_saida_e_fechamento(spark):
    lote = _bronze_livro(
        spark,
        {"NF": "1", "CHAVE_NF": "K1"},
        {"NF": "1", "CHAVE_NF": "K1"},  # duplicata exata
        {"REVENDA": "Outra", "NF": "2", "CHAVE_NF": "K2"},
        {"NF": " ", "CHAVE_NF": "K3"},
        {"NF": "4", "DATA_E": "invalida", "CHAVE_NF": "K4"},
        {"NF": "5", "VLR_TOTAL_NOTA_CALC": "nao_numero", "CHAVE_NF": "K5"},
    )
    silver, quarentena = ls.transformar_silver(lote)
    contagens = ls.contar(lote, silver, quarentena)

    assert (contagens.bronze, contagens.silver, contagens.quarentena, contagens.duplicatas_exatas) == (6, 1, 4, 1)
    assert contagens.fecha
    motivos = sorted(r["motivo"] for r in quarentena.collect())
    assert motivos == ["revenda_diferente", "sem_data_emissao", "sem_nf", "valor_invalido"]


def test_replace_where_por_janela_preserva_meses_anteriores(spark, tmp_path):
    destino = str(tmp_path / "silver_livro")
    lote_inicial = _bronze_livro(
        spark,
        {"NF": "1", "DATA_E": "2026-07-10", "VLR_TOTAL_NOTA_CALC": "100.00"},
        {"NF": "2", "DATA_E": "2026-08-10", "VLR_TOTAL_NOTA_CALC": "200.00"},
    )
    s1, _ = ls.transformar_silver(lote_inicial)
    ls.gravar_silver_janela(s1, destino)

    # Janela reprocessa apenas 2026-08 (corrigindo valor para 250.00) e adiciona 2026-09:
    # 2026-07 não está no lote e tem de continuar intacto!
    lote_janela = _bronze_livro(
        spark,
        {"NF": "2", "DATA_E": "2026-08-10", "VLR_TOTAL_NOTA_CALC": "250.00"},
        {"NF": "3", "DATA_E": "2026-09-10", "VLR_TOTAL_NOTA_CALC": "300.00"},
    )
    s2, _ = ls.transformar_silver(lote_janela)
    comps = ls.gravar_silver_janela(s2, destino)
    assert comps == ["2026-08", "2026-09"]

    atual = spark.read.format("delta").load(destino)
    por_mes = {
        f"{r['ano']:04d}-{r['mes']:02d}": r["vlr_total_nota_calc"]
        for r in ls.calcular_gold_mensal(atual).collect()
    }
    assert por_mes == {
        "2026-07": Decimal("100.00"),
        "2026-08": Decimal("250.00"),
        "2026-09": Decimal("300.00"),
    }


def test_livro_saida_ponta_a_ponta_com_amostra_sintetica(spark, tmp_path):
    manifesto = b_base.ler_manifesto(AMOSTRA_LIVRO)
    r_bronze = ls.ingerir_bronze(spark, AMOSTRA_LIVRO, str(tmp_path / "bronze_ls"))
    assert r_bronze.linhas_no_arquivo == 95
    assert not r_bronze.ja_ingerido

    bronze_df = spark.read.format("delta").load(str(tmp_path / "bronze_ls"))
    silver, quarentena = ls.transformar_silver(bronze_df)
    contagens = ls.contar(bronze_df, silver, quarentena)
    assert (contagens.bronze, contagens.silver, contagens.quarentena, contagens.duplicatas_exatas) == (95, 90, 4, 1)
    assert contagens.fecha

    gold = ls.calcular_gold_mensal(silver)
    reconciliacao = ls.reconciliar_mensal(gold, manifesto["controles_mensais"])
    assert len(reconciliacao) == 3
    assert all(r["bate"] for r in reconciliacao), reconciliacao
