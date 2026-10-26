"""Silver: documento no formato do ERP, telefones, quarentena e MERGE."""

import pytest
from pyspark.sql import functions as F

from estudo import bronze, silver_clientes as s


def _bronze(spark, *linhas):
    """Linhas no formato da landing (tudo texto) + metadados da bronze."""

    base = {c: None for c in bronze.COLUNAS}
    df = spark.createDataFrame([{**base, **linha} for linha in linhas], schema=bronze.ESQUEMA_LANDING)
    return bronze.com_metadados(df, "lote-teste", "teste.csv")


# ---------------------------------------------------------------- documento


@pytest.mark.parametrize(
    "no_cadastro, tipo, numero",
    [
        # CPFs sintéticos no formato de 15 dígitos do ERP.
        ("529982247000025", "cpf", "52998224725"),
        ("123456789000009", "cpf", "12345678909"),
        # CPF que começa com zero: o ERP engole o zero e sobram 14 dígitos.
        ("12345678000090", "cpf", "01234567890"),
        # CNPJ com e sem o zero da frente.
        ("11222333000181", "cnpj", "11222333000181"),
        ("1222333000128", "cnpj", "01222333000128"),
        # CPF guardado sem o formato do ERP.
        ("529.982.247-25", "cpf", "52998224725"),
        ("", "vazio", None),
        (None, "vazio", None),
        ("123", "desconhecido", None),
    ],
)
def test_documento_no_formato_do_erp(spark, no_cadastro, tipo, numero):
    df = spark.createDataFrame([(no_cadastro,)], "doc string").select(s.documento(F.col("doc")).alias("d"))
    r = df.first()["d"]
    assert (r["tipo"], r["numero"]) == (tipo, numero)


@pytest.mark.parametrize(
    "numero, tipo, valido",
    [
        ("52998224725", "cpf", True),       # CPF de exemplo, verificadores certos
        ("52998224726", "cpf", False),
        ("11111111111", "cpf", False),      # todos iguais: inválido
        ("11222333000181", "cnpj", True),   # CNPJ de exemplo
        ("11222333000182", "cnpj", False),
        (None, "vazio", None),
    ],
)
def test_digitos_verificadores(numero, tipo, valido):
    assert s._dv_ok(numero, tipo) is valido


# ---------------------------------------------------------------- telefones


@pytest.mark.parametrize(
    "telefone, fim",
    [("82999990000", "99990000"), ("(82) 9 9999-0000", "99990000"), ("558299990000", "99990000"),
     ("1234567", None), ("", None), (None, None)],
)
def test_fim8(spark, telefone, fim):
    df = spark.createDataFrame([(telefone,)], "t string").select(s.fim8(F.col("t")).alias("f"))
    assert df.first()["f"] == fim


# ---------------------------------------------------------------- quarentena


def test_quarentena_com_motivo_e_a_conta_fecha(spark):
    lote = _bronze(
        spark,
        {"revenda": "XPTO", "codigo_cliente": "1", "vendedor": "132", "cancelado": "0"},
        {"revenda": "XPTO", "codigo_cliente": "1", "vendedor": "132", "cancelado": "0"},   # duplicata exata
        {"revenda": "XPTO", "codigo_cliente": "2", "vendedor": "132", "cancelado": "0"},
        {"revenda": "XPTO", "codigo_cliente": "2", "vendedor": "133", "cancelado": "0"},   # conflito
        {"revenda": "XPTO", "codigo_cliente": " ", "vendedor": "132"},                     # sem código
        {"revenda": "Outra", "codigo_cliente": "3", "vendedor": "132"},                      # outra revenda
        {"revenda": "XPTO", "codigo_cliente": "4", "vendedor": " 134 ", "cancelado": "1"},
    )
    silver, quarentena = s.transformar(lote)
    contagens = s.contar(lote, silver, quarentena)

    assert sorted(r["codigo_cliente"] for r in silver.collect()) == ["1", "4"]
    motivos = sorted((r["codigo_cliente"], r["motivo"]) for r in quarentena.collect())
    assert motivos == [("", "sem_codigo"), ("2", "codigo_duplicado_conflitante"),
                       ("2", "codigo_duplicado_conflitante"), ("3", "revenda_diferente")]
    assert (contagens.bronze, contagens.silver, contagens.quarentena, contagens.duplicatas_exatas) == (7, 2, 4, 1)
    assert contagens.fecha
    quatro = silver.where("codigo_cliente = '4'").first()
    assert quatro["vendedor"] == "134" and quatro["cancelado"] is True


# ---------------------------------------------------------------- MERGE


def test_merge_e_idempotente_e_segue_o_retrato(spark, tmp_path):
    from delta.tables import DeltaTable

    destino = str(tmp_path / "silver")
    v1 = s.transformar(_bronze(spark,
                               {"revenda": "XPTO", "codigo_cliente": "1", "telefone": "82999990001"},
                               {"revenda": "XPTO", "codigo_cliente": "2", "telefone": "82999990002"}))[0]
    s.gravar(spark, v1, destino)
    s.gravar(spark, v1, destino)  # de novo: nada muda e não reescreve arquivos (T12)
    assert spark.read.format("delta").load(destino).count() == 2
    hist_idemp = DeltaTable.forPath(spark, destino).history(1).first()
    assert hist_idemp["version"] == 0  # Deletion Vectors: MERGE sem mudança vira no-op

    # Retrato seguinte: o 1 mudou de telefone, o 2 sumiu, o 3 chegou.
    v2 = s.transformar(_bronze(spark,
                               {"revenda": "XPTO", "codigo_cliente": "1", "telefone": "82988880001"},
                               {"revenda": "XPTO", "codigo_cliente": "3", "telefone": "82999990003"}))[0]
    s.gravar(spark, v2, destino)
    agora = {r["codigo_cliente"]: r["telefone_fim8"] for r in spark.read.format("delta").load(destino).collect()}
    assert agora == {"1": "88880001", "3": "99990003"}
    metricas = DeltaTable.forPath(spark, destino).history(1).first()["operationMetrics"]
    assert metricas["numTargetRowsCopied"] == "0"
