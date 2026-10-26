"""Fatia 1 de ponta a ponta: landing -> bronze -> silver -> gold, reconciliada e medida.

Dentro do contêiner:

    docker compose run --rm spark python jobs/fatia1_clientes.py

Sem argumento usa a exportação mais recente de data/landing. Sai com código 1 se
a reconciliação com a produção não bater (FR-08) ou se a conta das camadas não
fechar (FR-05).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from estudo import bronze, de_para, gold_reconhecimento, qualidade, silver_clientes  # noqa: E402
from estudo.sessao import criar_sessao  # noqa: E402

DELTA = RAIZ / "data" / "delta"


@contextmanager
def cronometro(tempos: dict, etapa: str):
    inicio = time.perf_counter()
    yield
    tempos[etapa] = round(time.perf_counter() - inicio, 3)


def exportacao_mais_recente() -> Path:
    arquivos = sorted((RAIZ / "data" / "landing").glob("clientes_xpto_*.csv"))
    if not arquivos:
        raise SystemExit("Nenhuma exportação em data/landing. Rode scripts/exportar_dw.py no Windows.")
    return arquivos[-1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--arquivo", type=Path, default=None, help="CSV da landing (padrão: o mais recente)")
    args = parser.parse_args()
    arquivo = args.arquivo or exportacao_mais_recente()
    manifesto = bronze.ler_manifesto(arquivo)
    dia = date.fromisoformat(manifesto["dia_referencia"])

    tempos: dict[str, float] = {}
    with cronometro(tempos, "sessao_spark"):
        spark = criar_sessao("fatia1-clientes")

    with cronometro(tempos, "bronze"):
        r_bronze = bronze.ingerir(spark, arquivo, str(DELTA / "bronze_clientes"))
        lote_bronze = spark.read.format("delta").load(str(DELTA / "bronze_clientes")).where(
            f"_lote = '{r_bronze.lote}'"
        )

    with cronometro(tempos, "silver"):
        silver, quarentena = silver_clientes.transformar(lote_bronze)
        silver = silver.cache()
        quarentena = quarentena.cache()
        contagens = silver_clientes.contar(lote_bronze, silver, quarentena)
        silver_clientes.gravar(spark, silver, str(DELTA / "silver_clientes"))
        silver_clientes.gravar_quarentena(quarentena, str(DELTA / "quarentena_clientes"), r_bronze.lote)

    with cronometro(tempos, "qualidade"):
        checagens = qualidade.checar_silver(silver)
        quarentena_por_motivo = {r["motivo"]: r["count"] for r in quarentena.groupBy("motivo").count().collect()}

    with cronometro(tempos, "gold"):
        seed = de_para.ler_seed(spark, str(RAIZ / "seeds" / "de_para_vendedor_base.csv"))
        gold = gold_reconhecimento.calcular(silver, de_para.bases_no_dia(seed, dia), dia).cache()
        gold_reconhecimento.gravar(gold, str(DELTA / "gold_reconhecimento"), dia)

    with cronometro(tempos, "reconciliacao"):
        reconciliacao = qualidade.reconciliar(gold, manifesto["controles"])

    tempos["total_sem_sessao"] = round(sum(v for k, v in tempos.items() if k != "sessao_spark"), 3)
    tempos_sql = {base: c["segundos_sql"] for base, c in manifesto["controles"].items()}

    relatorio = {
        "executado_em": datetime.now().isoformat(timespec="seconds"),
        "arquivo": arquivo.name,
        "lote": r_bronze.lote,
        "dia_referencia": dia.isoformat(),
        "bronze": {"linhas_no_arquivo": r_bronze.linhas_no_arquivo, "inseridas": r_bronze.linhas_inseridas,
                   "ja_ingerido": r_bronze.ja_ingerido},
        "contagens": {**contagens.__dict__, "fecha": contagens.fecha},
        "quarentena_por_motivo": quarentena_por_motivo,
        "checagens_silver": checagens,
        "gold": [r.asDict() for r in gold.drop("dia_referencia").collect()],
        "reconciliacao": reconciliacao,
        "tempos_spark_segundos": tempos,
        "tempos_sql_producao_segundos": {"exportacao": manifesto["segundos_exportacao"], **tempos_sql},
    }
    pasta = RAIZ / "reports"
    pasta.mkdir(exist_ok=True)
    destino = pasta / f"fatia1_{datetime.now():%Y%m%d_%H%M%S}.json"
    destino.write_text(json.dumps(relatorio, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    bate = all(r["bate"] for r in reconciliacao)
    print(f"arquivo {arquivo.name}  dia {dia}  lote {r_bronze.lote[:12]}  (já ingerido: {r_bronze.ja_ingerido})")
    print(f"camadas: {contagens}  fecha={contagens.fecha}")
    print(f"quarentena: {quarentena_por_motivo or 'vazia'}")
    print("reconciliação com a produção:")
    for r in reconciliacao:
        print(f"  {'ok ' if r['bate'] else 'DIF'} {r['base']:9} {r['indicador']:34} spark={r['spark']}  sql={r['sql']}")
    print(f"tempos (s): {tempos}")
    print(f"relatório: {destino.relative_to(RAIZ)}")
    spark.stop()
    return 0 if (bate and contagens.fecha) else 1


if __name__ == "__main__":
    raise SystemExit(main())
