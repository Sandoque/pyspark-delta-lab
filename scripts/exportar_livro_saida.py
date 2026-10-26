"""Exporta o Livro de Saída da XPTO do DW para a camada landing (FR-11).

Roda fora do contêiner, num Python com `sqlalchemy` e o driver do banco, com a
conexão em `ESTUDO_DW_URL` (usuário só de leitura):

    python scripts/exportar_livro_saida.py

Para exportar apenas a janela de reprocessamento (ex.: lookback de 3 meses):

    python scripts/exportar_livro_saida.py --data-inicial 2026-08-01 --prefixo livro_saida_xpto_janela

Tudo ocorre numa transação `READ ONLY` com `CONSISTENT SNAPSHOT`, sem escrever
nada no MySQL.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]

from sqlalchemy import create_engine, text  # noqa: E402

# A conexão vem do ambiente -- nenhuma senha no código nem nesta pasta. Exemplo:
#   ESTUDO_DW_URL=mysql+mysqlconnector://usuario_so_leitura@host:3306/dw
# Use um usuário só de leitura: o script não escreve nada, e o banco deve garantir isso.


def get_dw_engine(**opcoes):
    url = os.environ.get("ESTUDO_DW_URL")
    if not url:
        raise SystemExit("Defina ESTUDO_DW_URL (URL do SQLAlchemy de um usuário só de leitura).")
    return create_engine(url, **opcoes)


REVENDA_PADRAO = "XPTO"
COLUNAS = (
    "REVENDA", "NF", "LIVRO", "CHAVE_NF", "DATA_E", "CFOP", "COD_NATUREZA",
    "CODCOMP", "CNPJ", "CODCLI", "COD_PRD_RED", "QTD", "QTD_INTEIRA",
    "QTD_AVULSA", "INCIDENCIA", "VLR_PRODUTO", "VLR_IPI", "VLR_ICMS",
    "VLR_PIS", "VLR_COFINS", "FRETE", "ADIC_FINANCEIRO", "VLR_TOTAL",
    "VLR_TOTAL_NOTA_CALC",
)
COLUNAS_FLOAT = {
    "VLR_PRODUTO", "VLR_IPI", "VLR_ICMS", "VLR_PIS", "VLR_COFINS",
    "FRETE", "ADIC_FINANCEIRO", "VLR_TOTAL", "VLR_TOTAL_NOTA_CALC",
}


def _filtro_sql(data_inicial: str | None, data_final: str | None) -> tuple[str, dict]:
    clausulas = ["REVENDA = :revenda"]
    params: dict = {}
    if data_inicial:
        clausulas.append("DATA_E >= :data_inicial")
        params["data_inicial"] = data_inicial
    if data_final:
        clausulas.append("DATA_E <= :data_final")
        params["data_final"] = data_final
    return " AND ".join(clausulas), params


def _formatar_valor(col: str, val) -> str:
    if val is None:
        return ""
    if col in COLUNAS_FLOAT:
        return f"{float(val):.2f}"
    return str(val)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--revenda", default=REVENDA_PADRAO, help="Revenda (padrão: XPTO)")
    parser.add_argument("--data-inicial", default=None, help="Data inicial AAAA-MM-DD (opcional)")
    parser.add_argument("--data-final", default=None, help="Data final AAAA-MM-DD (opcional)")
    parser.add_argument("--prefixo", default="livro_saida_xpto", help="Prefixo do arquivo em data/landing")
    args = parser.parse_args()

    where_sql, params = _filtro_sql(args.data_inicial, args.data_final)
    params["revenda"] = args.revenda

    consulta_export = (
        f"SELECT {', '.join(COLUNAS)} FROM fiscal.livro_saida WHERE {where_sql}"
    )
    consulta_controles = f"""
        SELECT
            YEAR(DATA_E) AS ano,
            MONTH(DATA_E) AS mes,
            COUNT(*) AS itens,
            COUNT(DISTINCT CONCAT(TRIM(REVENDA), '|', NF, '|', COALESCE(TRIM(LIVRO), ''), '|', COALESCE(TRIM(CHAVE_NF), ''))) AS nfs_distintas,
            COUNT(DISTINCT NF) AS nfs_so_numero,
            CAST(SUM(CAST(ROUND(COALESCE(VLR_PRODUTO, 0), 2) AS DECIMAL(15, 2))) AS CHAR) AS vlr_produto,
            CAST(SUM(CAST(ROUND(COALESCE(VLR_ICMS, 0), 2) AS DECIMAL(15, 2))) AS CHAR) AS vlr_icms,
            CAST(SUM(CAST(ROUND(COALESCE(VLR_PIS, 0), 2) AS DECIMAL(15, 2))) AS CHAR) AS vlr_pis,
            CAST(SUM(CAST(ROUND(COALESCE(VLR_COFINS, 0), 2) AS DECIMAL(15, 2))) AS CHAR) AS vlr_cofins,
            CAST(SUM(CAST(ROUND(COALESCE(VLR_TOTAL_NOTA_CALC, 0), 2) AS DECIMAL(15, 2))) AS CHAR) AS vlr_total_nota_calc,
            SUM(COALESCE(VLR_TOTAL_NOTA_CALC, 0)) AS vlr_total_double_bruto
        FROM fiscal.livro_saida
        WHERE {where_sql}
        GROUP BY YEAR(DATA_E), MONTH(DATA_E)
        ORDER BY ano, mes
    """

    destino = RAIZ / "data" / "landing"
    destino.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_csv = destino / f"{args.prefixo}_{carimbo}.csv"

    engine = get_dw_engine(pool_pre_ping=True)
    total_linhas = 0
    with engine.connect() as conexao:
        conexao.execute(text("SET SESSION TRANSACTION READ ONLY"))
        conexao.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
        try:
            t0 = time.perf_counter()
            resultado = conexao.execution_options(stream_results=True).execute(text(consulta_export), params)
            with caminho_csv.open("w", encoding="utf-8", newline="") as arquivo:
                escritor = csv.writer(arquivo)
                escritor.writerow(COLUNAS)
                while True:
                    bloco = resultado.fetchmany(25_000)
                    if not bloco:
                        break
                    escritor.writerows(
                        [[_formatar_valor(COLUNAS[i], v) for i, v in enumerate(linha)] for linha in bloco]
                    )
                    total_linhas += len(bloco)
            t_export = round(time.perf_counter() - t0, 3)

            t1 = time.perf_counter()
            linhas_ctrl = conexao.execute(text(consulta_controles), params).mappings().all()
            t_sql_ctrl = round(time.perf_counter() - t1, 3)
        finally:
            conexao.execute(text("ROLLBACK"))

    controles = {}
    for r in linhas_ctrl:
        comp = f"{int(r['ano']):04d}-{int(r['mes']):02d}"
        controles[comp] = {
            "ano": int(r["ano"]),
            "mes": int(r["mes"]),
            "itens": int(r["itens"]),
            "nfs_distintas": int(r["nfs_distintas"]),
            "nfs_so_numero": int(r["nfs_so_numero"]),
            "vlr_produto": r["vlr_produto"],
            "vlr_icms": r["vlr_icms"],
            "vlr_pis": r["vlr_pis"],
            "vlr_cofins": r["vlr_cofins"],
            "vlr_total_nota_calc": r["vlr_total_nota_calc"],
            "vlr_total_double_bruto": float(r["vlr_total_double_bruto"] or 0.0),
        }

    sha256 = hashlib.sha256(caminho_csv.read_bytes()).hexdigest()
    manifesto = {
        "arquivo": caminho_csv.name,
        "linhas": total_linhas,
        "sha256": sha256,
        "revenda": args.revenda,
        "data_inicial": args.data_inicial,
        "data_final": args.data_final,
        "consulta": consulta_export,
        "exportado_em": datetime.now().isoformat(timespec="seconds"),
        "segundos_exportacao": t_export,
        "segundos_sql_controles": t_sql_ctrl,
        "competencias": len(controles),
        "controles_mensais": controles,
    }
    caminho_csv.with_suffix(".json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(
        f"{total_linhas} itens ({len(controles)} competências) -> {caminho_csv.name} "
        f"(export={t_export}s, sql_ctrl={t_sql_ctrl}s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

