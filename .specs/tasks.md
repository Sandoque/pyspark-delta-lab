# Tasks

Estado: `[ ]` a fazer · `[x]` feito, com evidência. Números do dado real
arredondados (constitution).

## Fatia 1 — cadastro de clientes

- [x] T01 (FR-01) Contêiner com Spark 4.0.1, Delta 4.0.0, JupyterLab e pytest; jars do Delta baixados no build
- [x] T02 (FR-01) `pytest` verde dentro do contêiner
- [x] T03 (FR-02) `scripts/exportar_dw.py`: CSV + manifesto + controles do SQL, `READ ONLY` com snapshot consistente (~30 mil clientes)
- [x] T04 (FR-03) Bronze idempotente pelo sha256 do lote — 2ª execução "já ingerido", uma escrita só no histórico
- [x] T05 (FR-04) Silver: documento, telefones, cancelado, um registro por código — ~74% CPF, ~26% CNPJ, 0,04% fora de formato
- [x] T06 (FR-05) Quarentena com motivo e a conta `bronze = silver + quarentena + duplicatas` fechando
- [x] T07 (FR-06) Seed de/para com vigência — teste da troca de vendedor na data
- [x] T08 (FR-07) Gold de reconhecimento por base e dia — `replaceWhere` por dia
- [x] T09 (FR-08) Reconciliação com o SQL — **12 de 12** indicadores iguais
- [x] T10 (FR-09, FR-10) Tempos e relatório JSON
- [x] T11 Checkpoint com o critério do teste de uso

## Fatia 1b

- [x] T12 MERGE sem reescrever o que não mudou — deletion vectors: reexecução sem mudança grava 0 arquivos; mudança pontual grava 2 linhas + 1 DV, contra a tabela inteira sem DV
- [x] T13 Prova por mutação — `scripts/testar_mutacoes.py`
- [x] T14 Amostra sintética no mesmo formato — `seeds/amostra_sintetica/clientes_xpto_sintetico.{csv,json}` (185 linhas: 180 válidas, 4 quarentena, 1 duplicata exata; 12/12 em `tests/test_fim_a_fim_sintetico.py`)
- [x] T15 Projeto uv com Python 3.12 e dependências travadas
- [x] T16 Dockerfile instalando pelo `uv.lock`

## Fatia 2 — livro fiscal de saída

- [x] T17 (FR-11) `scripts/exportar_livro_saida.py`: completo (~1,6 milhão de itens, 30 competências) e janela de 3 meses (~120 mil itens), com controles mensais do SQL
- [x] T18 (FR-12, FR-13) Bronze idempotente + silver em `DECIMAL(15,2)`, chave da nota completa, quarentena e `replaceWhere` por `(ano, mes)`
- [x] T19 (FR-14, FR-15) Gold mensal, deriva do `float32` e reconciliação ao centavo — **30 de 30 competências, 210 valores, diferença 0,00**
- [x] T20 (FR-17) Amostra sintética fiscal (95 linhas), testes e mutações fiscais — 32 testes, 10/10 mutações mortas
- [x] T21 (FR-16) Benchmark carga completa × janela de 3 meses

## Fatia 3 — Databricks Free Edition

- [x] T22 Notebooks da fatia 1 para o workspace (`databricks/00_landing` → `03_gold`), lendo a amostra sintética de um Volume e gravando no Unity Catalog; o pacote `src/estudo` grava por caminho ou por nome de tabela (`estudo/destino.py`), provado no contêiner por `tests/test_destino_tabela.py` (39 testes) (26/10)
- [x] T23 Job definido como código (`databricks.yml`): schema e Volume no Unity Catalog + Job de 4 tarefas encadeadas (landing → bronze → silver → gold) em serverless, com o lote passado por *task values* e o Job falhando se a reconciliação não bater (26/10)
- [x] T24 Rodar o Job e registrar a reconciliação e os tempos — 2 execuções verdes; **12 de 12** em `workspace.lab.reconciliacao_fatia1`; 2ª execução idempotente (bronze sem duplicar, MERGE 0/0/0/0) (26/10)
- [x] T25 CI: GitHub Actions (`.github/workflows/ci.yml`) em todo push e PR: Java 17 + `uv sync --frozen`, `pytest`, prova por mutação e a fatia 1 ponta a ponta com a amostra sintética; simulado antes num contêiner Linux limpo (26/10)
- [ ] T26 Exercício de performance (`databricks/90_performance`, fora do Job, dados de `spark.range`): arquivos lidos antes/depois de `OPTIMIZE ZORDER BY` e com liquid clustering, broadcast × shuffle lido no `explain()`, skew com e sem *salting* provando o mesmo resultado; medidas em `workspace.lab.perf_medidas` — lógica do join e do skew conferida no contêiner com 1 mi de linhas (BroadcastHashJoin 2 Exchange × SortMergeJoin 3; maior partição/mediana 14,7× → 3,2×; 0 clientes diferentes). Estimado: 45 min

## Estimado × realizado

A partir da fatia 3, cada tarefa registra a estimativa antes de começar e o tempo
real ao fechar.
