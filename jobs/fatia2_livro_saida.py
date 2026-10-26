"""Fatia 2 de ponta a ponta: Livro de Saída da XPTO (milhões de linhas) + benchmark de janela replaceWhere.

Dentro do contêiner:

    docker compose run --rm spark python jobs/fatia2_livro_saida.py

Ou com a amostra sintética:

    docker compose run --rm spark python jobs/fatia2_livro_saida.py --arquivo seeds/amostra_sintetica/livro_saida_xpto_sintetico.csv --sem-janela
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from estudo import bronze as b_base, livro_saida as ls  # noqa: E402
from estudo.sessao import criar_sessao  # noqa: E402

DELTA = RAIZ / "data" / "delta"


@contextmanager
def cronometro(tempos: dict, etapa: str):
    inicio = time.perf_counter()
    yield
    tempos[etapa] = round(time.perf_counter() - inicio, 3)


def _mais_recente(padrao: str) -> Path | None:
    arquivos = sorted(
        p for p in (RAIZ / "data" / "landing").glob(padrao)
        if not p.name.startswith("livro_saida_xpto_janela_") or "janela" in padrao
    )
    return arquivos[-1] if arquivos else None


def _metricas_ultimo_commit(spark, caminho: str) -> dict:
    h = DeltaTable.forPath(spark, caminho).history(1).first()
    m = h["operationMetrics"] or {}
    return {
        "version": h["version"],
        "operation": h["operation"],
        "numOutputRows": int(m.get("numOutputRows", 0)),
        "numFiles": int(m.get("numFiles", 0)),
        "numRemovedFiles": int(m.get("numRemovedFiles", 0)),
    }


def _executar_lote(spark, arquivo: Path, pasta_delta: Path) -> dict:
    manifesto = b_base.ler_manifesto(arquivo)
    tempos: dict[str, float] = {}

    caminho_bronze = str(pasta_delta / "bronze_livro_saida")
    caminho_silver = str(pasta_delta / "silver_livro_saida")
    caminho_quarentena = str(pasta_delta / "quarentena_livro_saida")
    caminho_gold = str(pasta_delta / "gold_livro_saida_mensal")

    with cronometro(tempos, "bronze"):
        r_bronze = ls.ingerir_bronze(spark, arquivo, caminho_bronze)
        lote_bronze = spark.read.format("delta").load(caminho_bronze).where(f"_lote = '{r_bronze.lote}'")

    with cronometro(tempos, "silver"):
        silver, quarentena = ls.transformar_silver(lote_bronze)
        ls.gravar_quarentena(quarentena, caminho_quarentena, r_bronze.lote)
        competencias_silver = ls.gravar_silver_janela(silver, caminho_silver)
        predicado_janela, _ = ls._predicado_competencias(
            spark.createDataFrame(
                [(int(c[:4]), int(c[5:7])) for c in competencias_silver], "ano int, mes int"
            )
        )
        silver_gravada = spark.read.format("delta").load(caminho_silver).where(predicado_janela)
        quarentena_gravada = spark.read.format("delta").load(caminho_quarentena).where(f"_lote = '{r_bronze.lote}'")
        contagens = ls.contar(lote_bronze, silver_gravada, quarentena_gravada)

    with cronometro(tempos, "gold"):
        gold_lote = ls.calcular_gold_mensal(silver_gravada).cache()
        ls.gravar_gold_janela(gold_lote, caminho_gold)

    with cronometro(tempos, "reconciliacao"):
        reconciliacao = ls.reconciliar_mensal(gold_lote, manifesto["controles_mensais"])

    tempos["total_sem_sessao"] = round(sum(tempos.values()), 3)
    quarentena_por_motivo = {r["motivo"]: r["count"] for r in quarentena_gravada.groupBy("motivo").count().collect()}
    metricas_silver = _metricas_ultimo_commit(spark, caminho_silver)

    gold_lote.unpersist()

    return {
        "arquivo": arquivo.name,
        "lote": r_bronze.lote,
        "bronze": {
            "linhas_no_arquivo": r_bronze.linhas_no_arquivo,
            "inseridas": r_bronze.linhas_inseridas,
            "ja_ingerido": r_bronze.ja_ingerido,
        },
        "contagens": {**contagens.__dict__, "fecha": contagens.fecha},
        "quarentena_por_motivo": quarentena_por_motivo,
        "competencias_gravadas": competencias_silver,
        "metricas_delta_silver": metricas_silver,
        "reconciliacao": reconciliacao,
        "bate_100_por_cento": all(r["bate"] for r in reconciliacao),
        "tempos_spark_segundos": tempos,
        "tempos_sql_producao_segundos": {
            "exportacao": manifesto["segundos_exportacao"],
            "controles_sql": manifesto["segundos_sql_controles"],
            "total": round(manifesto["segundos_exportacao"] + manifesto["segundos_sql_controles"], 3),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--arquivo", type=Path, default=None, help="CSV completo da landing")
    parser.add_argument("--arquivo-janela", type=Path, default=None, help="CSV da janela de 3 meses")
    parser.add_argument("--sem-janela", action="store_true", help="Não roda o benchmark da janela")
    args = parser.parse_args()

    arquivo_completo = args.arquivo or _mais_recente("livro_saida_xpto_*.csv")
    if not arquivo_completo:
        raise SystemExit("Nenhuma exportação do Livro de Saída em data/landing. Rode scripts/exportar_livro_saida.py.")

    t0 = time.perf_counter()
    spark = criar_sessao("fatia2-livro-saida")
    t_sessao = round(time.perf_counter() - t0, 3)

    res_completo = _executar_lote(spark, arquivo_completo, DELTA)

    res_janela = None
    reconciliacao_pos_janela_ok = None
    if not args.sem_janela:
        arquivo_janela = args.arquivo_janela or _mais_recente("livro_saida_xpto_janela_*.csv")
        if arquivo_janela:
            res_janela = _executar_lote(spark, arquivo_janela, DELTA)
            # Confere se a tabela gold inteira (30 meses) continua batendo 100% após o replaceWhere de 3 meses!
            manifesto_completo = b_base.ler_manifesto(arquivo_completo)
            gold_completa = spark.read.format("delta").load(str(DELTA / "gold_livro_saida_mensal"))
            rec_pos = ls.reconciliar_mensal(gold_completa, manifesto_completo["controles_mensais"])
            reconciliacao_pos_janela_ok = all(r["bate"] for r in rec_pos)

    relatorio = {
        "executado_em": datetime.now().isoformat(timespec="seconds"),
        "sessao_spark_segundos": t_sessao,
        "carga_completa": res_completo,
        "reprocessamento_janela_3_meses": res_janela,
        "tabela_completa_integra_apos_janela": reconciliacao_pos_janela_ok,
    }
    pasta = RAIZ / "reports"
    pasta.mkdir(exist_ok=True)
    destino = pasta / f"fatia2_livro_saida_{datetime.now():%Y%m%d_%H%M%S}.json"
    destino.write_text(json.dumps(relatorio, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    print(
        f"[COMPLETO] {res_completo['arquivo']} | {res_completo['contagens']} | "
        f"competências={len(res_completo['competencias_gravadas'])} | "
        f"reconciliação={res_completo['bate_100_por_cento']} | "
        f"tempos_spark={res_completo['tempos_spark_segundos']} | "
        f"tempos_sql={res_completo['tempos_sql_producao_segundos']}"
    )
    if res_janela:
        print(
            f"[JANELA 3M] {res_janela['arquivo']} | {res_janela['contagens']} | "
            f"competências={res_janela['competencias_gravadas']} | "
            f"delta_silver={res_janela['metricas_delta_silver']} | "
            f"reconciliação_janela={res_janela['bate_100_por_cento']} | "
            f"30_meses_integros_pos_janela={reconciliacao_pos_janela_ok} | "
            f"tempos_spark={res_janela['tempos_spark_segundos']}"
        )
    print(f"relatório: {destino.relative_to(RAIZ)}")
    spark.stop()

    ok = res_completo["bate_100_por_cento"] and res_completo["contagens"]["fecha"]
    if res_janela:
        ok = ok and res_janela["bate_100_por_cento"] and bool(reconciliacao_pos_janela_ok)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

