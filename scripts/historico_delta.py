"""Mostra o histórico (time travel) das tabelas Delta da fatia. Uso: python scripts/historico_delta.py"""

from pathlib import Path
from estudo.sessao import criar_sessao

spark = criar_sessao("historico")
for tabela in (
    "bronze_clientes",
    "silver_clientes",
    "gold_reconhecimento",
    "bronze_livro_saida",
    "silver_livro_saida",
    "gold_livro_saida_mensal",
):
    caminho = Path(f"/work/data/delta/{tabela}")
    if not caminho.exists():
        continue
    print(f"== {tabela}")
    (
        spark.sql(f"DESCRIBE HISTORY delta.`{caminho}`")
        .selectExpr(
            "version", "operation",
            "operationMetrics['numOutputRows'] AS gravadas",
            "operationMetrics['numFiles'] AS arquivos",
            "operationMetrics['numRemovedFiles'] AS removidos",
            "operationMetrics['numTargetRowsInserted'] AS inseridas",
            "operationMetrics['numTargetRowsUpdated'] AS atualizadas",
            "operationMetrics['numTargetRowsDeleted'] AS apagadas",
        )
        .orderBy("version")
        .show(truncate=False)
    )
spark.stop()
