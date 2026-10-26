"""De/para com vigência e o indicador de reconhecimento pelo número."""

from datetime import date
from pathlib import Path

from estudo import de_para, gold_reconhecimento

SEED = str(Path(__file__).resolve().parents[1] / "seeds" / "de_para_vendedor_base.csv")


def _cobertura(spark, dia):
    seed = de_para.ler_seed(spark, SEED)
    return sorted(r["vendedor"] for r in de_para.bases_no_dia(seed, dia).where("base = 'cobertura'").collect())


def test_a_131_sai_e_a_134_entra_em_21_10(spark):
    # Decisão do dono em 21/10/2026: a troca vale no próprio dia.
    assert _cobertura(spark, date(2026, 10, 20)) == ["131", "132", "133"]
    assert _cobertura(spark, date(2026, 10, 21)) == ["132", "133", "134"]
    assert _cobertura(spark, date(2026, 12, 1)) == ["132", "133", "134"]


def _silver(spark, linhas):
    return spark.createDataFrame(
        linhas, "codigo_cliente string, vendedor string, cancelado boolean, telefone_fim8 string, celular_fim8 string"
    )


def test_reconhecimento_pela_regra_da_producao(spark):
    silver = _silver(spark, [
        # 1: telefone só dele -> reconhecido nas duas regras.
        ("1", "132", False, "11110000", None),
        # 2: só celular, só dele -> só na regra nova.
        ("2", "132", False, None, "22220000"),
        # 3 e 4: o mesmo número no telefone dos dois -> repetido, nenhum reconhecido.
        ("3", "133", False, "33330000", None),
        ("4", "133", False, "33330000", None),
        # 5: telefone só dele, mas o celular é o do 1 -> reconhecido E com número repetido.
        ("5", "134", False, "55550000", "11110000"),
        # 6: cancelado, fora da contagem -- mas o número dele conta na unicidade.
        ("6", "134", True, "66660000", None),
        ("7", "134", False, None, "66660000"),
        # 8: de outra base.
        ("8", "111", False, "88880000", None),
    ])
    bases = spark.createDataFrame([("132", "cobertura"), ("133", "cobertura"), ("134", "cobertura")],
                                  "vendedor string, base string")

    (linha,) = gold_reconhecimento.calcular(silver, bases, date(2026, 10, 23)).collect()

    assert linha["clientes_ativos"] == 6              # 1, 2, 3, 4, 5, 7
    # Regra antiga (só telefone): 1 único; 5 único (o 1 tem o 11110000 só no telefone dele).
    assert linha["reconhecidos_so_telefone"] == 2     # 1, 5
    # Regra nova: o 11110000 agora é de dois (telefone do 1, celular do 5): o 1 perde,
    # o 5 segue pelo 55550000; o 2 entra pelo celular; o 7 divide com o cancelado 6.
    assert linha["reconhecidos_telefone_ou_celular"] == 2   # 2, 5
    assert linha["com_numero_repetido"] == 5          # 1, 3, 4, 5, 7
