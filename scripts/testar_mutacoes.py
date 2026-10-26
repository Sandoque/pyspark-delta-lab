"""Prova os testes por mutação (T13).

Aplica mutações cirúrgicas em memória nas regras de negócio (documento no
formato do ERP, dígitos verificadores, fim8, quarentena de conflito,
vigência do de/para e unicidade na gold) e executa as funções de teste sobre a
mesma sessão Spark para comprovar que nenhuma mutação sobrevive.

Uso no contêiner:
    docker compose run --rm spark python scripts/testar_mutacoes.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import Column
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ / "tests"))

from estudo import de_para, gold_reconhecimento, livro_saida as ls, silver_clientes as s  # noqa: E402
from estudo.sessao import criar_sessao  # noqa: E402
import test_de_para_e_gold  # noqa: E402
import test_livro_saida  # noqa: E402
import test_silver_clientes  # noqa: E402


def _rodar_suite(spark) -> list[str]:
    """Roda os testes de regras de negócio e devolve os nomes dos que falharam."""

    falhas: list[str] = []

    def checar(nome: str, fn):
        try:
            fn()
        except AssertionError:
            falhas.append(nome)

    for doc_in, tipo, numero in [
        ("529982247000025", "cpf", "52998224725"),
        ("12345678000090", "cpf", "01234567890"),
        ("11222333000181", "cnpj", "11222333000181"),
        ("1222333000128", "cnpj", "01222333000128"),
        ("529.982.247-25", "cpf", "52998224725"),
        ("", "vazio", None),
        ("123", "desconhecido", None),
    ]:
        checar(
            f"test_documento_no_formato_do_erp[{doc_in}]",
            lambda d=doc_in, t=tipo, n=numero: test_silver_clientes.test_documento_no_formato_do_erp(
                spark, d, t, n
            ),
        )

    for numero, tipo, valido in [
        ("52998224725", "cpf", True),
        ("52998224726", "cpf", False),
        ("11111111111", "cpf", False),
        ("11222333000181", "cnpj", True),
        ("11222333000182", "cnpj", False),
    ]:
        checar(
            f"test_digitos_verificadores[{numero}]",
            lambda n=numero, t=tipo, v=valido: test_silver_clientes.test_digitos_verificadores(n, t, v),
        )

    for tel, esperado in [
        ("82999990000", "99990000"),
        ("(82) 9 9999-0000", "99990000"),
        ("558299990000", "99990000"),
        ("1234567", None),
    ]:
        checar(
            f"test_fim8[{tel}]",
            lambda t=tel, e=esperado: test_silver_clientes.test_fim8(spark, t, e),
        )

    checar(
        "test_quarentena_com_motivo_e_a_conta_fecha",
        lambda: test_silver_clientes.test_quarentena_com_motivo_e_a_conta_fecha(spark),
    )
    checar(
        "test_a_131_sai_e_a_134_entra_em_21_10",
        lambda: test_de_para_e_gold.test_a_131_sai_e_a_134_entra_em_21_10(spark),
    )
    checar(
        "test_reconhecimento_pela_regra_da_producao",
        lambda: test_de_para_e_gold.test_reconhecimento_pela_regra_da_producao(spark),
    )
    checar(
        "test_chave_nota_separa_nfs_de_mesmo_numero_e_soma_decimal_exata",
        lambda: test_livro_saida.test_chave_nota_separa_nfs_de_mesmo_numero_e_soma_decimal_exata(spark),
    )
    return falhas


# ---------------------------------------------------------------------------
# Mutantes
# ---------------------------------------------------------------------------


def _mut_documento_sem_filial_0000(coluna: Column) -> Column:
    """Esquece que filial '0000' em 15 dígitos é CPF do ERP."""
    d = s.so_digitos(coluna)
    p = F.lpad(d, 15, "0")
    tamanho = F.length(d)
    em_15 = (tamanho >= 12) & (tamanho <= 15)
    tipo = (
        F.when(tamanho == 0, F.lit("vazio"))
        .when(em_15, F.lit("cnpj"))
        .when((tamanho >= 9) & (tamanho <= 11), F.lit("cpf"))
        .otherwise(F.lit("desconhecido"))
    )
    numero = F.when(tipo == "cpf", F.lpad(d, 11, "0")).when(tipo == "cnpj", F.substring(p, 2, 14))
    return F.struct(tipo.alias("tipo"), numero.alias("numero"))


def _mut_documento_sem_lpad_15(coluna: Column) -> Column:
    """Não recompõe o zero à esquerda engolido pelo ERP."""
    d = s.so_digitos(coluna)
    tamanho = F.length(d)
    tipo = (
        F.when(tamanho == 0, F.lit("vazio"))
        .when((tamanho == 15) & (F.substring(d, 10, 4) == "0000"), F.lit("cpf"))
        .when(tamanho == 14, F.lit("cnpj"))
        .when(tamanho == 11, F.lit("cpf"))
        .otherwise(F.lit("desconhecido"))
    )
    numero = (
        F.when((tamanho == 15) & (tipo == "cpf"), F.concat(F.substring(d, 1, 9), F.substring(d, 14, 2)))
        .when(tipo == "cpf", d)
        .when(tipo == "cnpj", d)
    )
    return F.struct(tipo.alias("tipo"), numero.alias("numero"))


def _mut_fim8_primeiros_8(coluna: Column) -> Column:
    """Pega os 8 primeiros dígitos (com DDD/DDI) em vez dos 8 últimos."""
    d = s.so_digitos(coluna)
    return F.when(F.length(d) >= 8, F.substring(d, 1, 8))


def _mut_fim8_aceita_curto(coluna: Column) -> Column:
    """Não exige mínimo de 8 dígitos."""
    d = s.so_digitos(coluna)
    return F.when(F.length(d) > 0, F.substring(d, -8, 8))


def _mut_de_para_ate_inclusivo(seed, dia):
    """Usa `d <= valido_ate` em vez de `d < valido_ate`, mantendo a 131 ativa em 21/10."""
    d = F.lit(dia).cast("date")
    return seed.where(
        (F.col("valido_de").isNull() | (F.col("valido_de") <= d))
        & (F.col("valido_ate").isNull() | (d <= F.col("valido_ate")))
    ).select("vendedor", "base")


def _mut_gold_ignora_cancelado_na_unicidade(silver, bases, dia):
    """Filtra cancelados antes de checar unicidade do número (erro clássico)."""
    return _calcular_original(silver.where(~F.col("cancelado")), bases, dia)


def _mut_gold_aceita_numero_repetido(pares, donos, condicao):
    """Não exige que o número pertença a 1 único cliente."""
    return pares.join(donos, "fim").where(F.col("donos") >= 1).select("codigo_cliente").distinct()


def _mut_livro_chave_nota_so_nf(revenda: Column, nf: Column, livro: Column, chave_nf: Column) -> Column:
    """Agrupa nota só por revenda + NF, fundindo notas de chaves distintas (erro da Correção 4)."""
    return F.concat_ws("|", F.trim(revenda), F.trim(nf))


def _mut_livro_valor_float32(coluna: Column) -> Column:
    """Converte valor fiscal para float32 antes de somar (erro da Correção 1: perde centavos > R$ 2M)."""
    limpo = F.nullif(F.trim(coluna), F.lit(""))
    return limpo.try_cast("float").try_cast("decimal(15,2)")


_calcular_original = gold_reconhecimento.calcular


def main() -> int:
    spark = criar_sessao("mutacoes-t13")

    base = _rodar_suite(spark)
    if base:
        print(f"ERRO: suíte base falhou sem mutação: {base}")
        spark.stop()
        return 1

    mutacoes = [
        ("documento_sem_filial_0000", patch.object(s, "documento", _mut_documento_sem_filial_0000)),
        ("documento_sem_lpad_15", patch.object(s, "documento", _mut_documento_sem_lpad_15)),
        ("dv_sempre_true", patch.object(s, "_dv_ok", lambda num, tipo: True if num else None)),
        ("fim8_primeiros_8", patch.object(s, "fim8", _mut_fim8_primeiros_8)),
        ("fim8_aceita_curto", patch.object(s, "fim8", _mut_fim8_aceita_curto)),
        ("de_para_ate_inclusivo", patch.object(de_para, "bases_no_dia", _mut_de_para_ate_inclusivo)),
        ("unicidade_ignora_cancelado", patch.object(gold_reconhecimento, "calcular", _mut_gold_ignora_cancelado_na_unicidade)),
        ("unicidade_aceita_repetido", patch.object(gold_reconhecimento, "_clientes_com", _mut_gold_aceita_numero_repetido)),
        ("livro_chave_nota_so_nf", patch.object(ls, "chave_nota", _mut_livro_chave_nota_so_nf)),
        ("livro_valor_float32", patch.object(ls, "valor_decimal", _mut_livro_valor_float32)),
    ]

    sobreviventes = 0
    print(f"{'Mutação':30} | {'Status':6} | Testes que pegaram")
    print("-" * 95)
    for nome, ctx in mutacoes:
        with ctx:
            falhas = _rodar_suite(spark)
        status = "MORTA" if falhas else "VIVA!"
        if not falhas:
            sobreviventes += 1
        resumo = ", ".join(falhas[:2]) + (f" (+{len(falhas) - 2})" if len(falhas) > 2 else "")
        print(f"{nome:30} | {status:6} | {resumo}")

    spark.stop()
    print("-" * 95)
    print(f"Total: {len(mutacoes) - sobreviventes}/{len(mutacoes)} mutações mortas.")
    return 0 if sobreviventes == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

