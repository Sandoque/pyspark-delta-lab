# PySpark + Delta Lab

Estudo de **PySpark 4.0 e Delta Lake 4.0** sobre problemas reais de um pipeline de
dados de uma distribuidora (aqui, **revenda XPTO**), feito também como **teste de
uso**: cada resultado foi **reconciliado com o SQL de produção** e cada etapa,
**medida**. O método é SDD (especificação → plano → tarefas com evidência →
checkpoints), em [`.specs/`](.specs/).

> Este repositório traz só código, testes e **amostras sintéticas**. As execuções
> sobre o dado real ficaram fora dele; os números abaixo estão arredondados ou
> relativos.

## O que tem aqui

| Fatia | Problema real | O que o pipeline faz |
| --- | --- | --- |
| **1 — cadastro de clientes** (~30 mil) | Documento em formato do ERP (CPF com 15 dígitos, zero engolido), telefone espalhado em 3 campos, carteira comercial que muda numa data | Bronze idempotente pelo sha256 do lote → silver normalizada com quarentena e MERGE com deletion vectors → de/para com vigência → gold do reconhecimento do cliente pelo número |
| **2 — livro fiscal de saída** (~1,6 milhão de itens) | Reprocessamento do histórico inteiro toda noite, centavos perdidos em `float32`, notas fundidas por chave incompleta | Silver em `DECIMAL(15,2)` particionada por `(ano, mes)`, chave completa da nota, gold mensal e reprocessamento só da janela com `replaceWhere` |

```text
banco (SELECT, READ ONLY) ──► landing (CSV + manifesto sha256 + controles do SQL)
   └─► bronze (Delta, texto) ──► silver (tipada) + quarentena ──► gold ──► reconciliação com o SQL
```

## Resultados

| | Resultado |
| --- | --- |
| Reconciliação | **12 de 12** indicadores no cadastro; **30 de 30 competências (210 valores) ao centavo** no livro fiscal |
| Janela de 3 meses × carga completa | **−92,6% de linhas, −65% de tempo**, só **3 de 30** arquivos Parquet reescritos e 27 meses intactos |
| MERGE com deletion vectors | Reexecução sem mudança: **0 arquivos** reescritos (sem DV: a tabela inteira) |
| `float32` × `DECIMAL(15,2)` | O `float32` perdeu os centavos em **30 de 30** meses |
| Testes | 39 testes (unitários, ponta a ponta por caminho e por tabela) e **10 de 10 mutações mortas** |
| Spark × SQL | No cadastro (~30 mil linhas) o SQL foi **~5× mais rápido**. No livro fiscal, a agregação mensal levou 5,5 s no Spark contra 15,7 s no MySQL — um indício, não uma regra (ver [checkpoints](.specs/checkpoints.md)) |

**Conclusão do teste de uso:** a maior lição não foi a ferramenta, foi **processar
só a janela que mudou** — o que dá para fazer também em SQL. Spark + Delta valem
como motor de processamento pesado se as tabelas fiscais continuarem crescendo,
publicando no banco da aplicação só a gold agregada. Detalhes, ressalvas e
achados (como o `OutOfMemoryError` de escrita particionada) em
[`.specs/checkpoints.md`](.specs/checkpoints.md).

## Rodar com a amostra sintética

Pré-requisito: Docker. Uma vez:

```bash
docker compose build
```

Fatia 1 e fatia 2 sobre a amostra versionada (sem banco):

```bash
docker compose run --rm spark python jobs/fatia1_clientes.py --arquivo seeds/amostra_sintetica/clientes_xpto_sintetico.csv
```

```bash
docker compose run --rm spark python jobs/fatia2_livro_saida.py --arquivo seeds/amostra_sintetica/livro_saida_xpto_sintetico.csv --sem-janela
```

Testes, prova por mutação e histórico das tabelas Delta:

```bash
docker compose run --rm spark pytest -q
```

```bash
docker compose run --rm spark python scripts/testar_mutacoes.py
```

```bash
docker compose run --rm spark python scripts/historico_delta.py
```

JupyterLab em http://127.0.0.1:8888 e Spark UI em http://127.0.0.1:4040:

```bash
docker compose up
```

## Na Databricks Free Edition

O mesmo pipeline roda como **Job de 4 tarefas** (landing → bronze → silver → gold)
em serverless, com as tabelas no Unity Catalog (`workspace.lab.*`). Schema, Volume
e Job estão definidos como código em [`databricks.yml`](databricks.yml)
(Databricks Asset Bundle); os notebooks de [`databricks/`](databricks/) só chamam
o pacote `src/estudo`.

```bash
databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com --profile lab
```

```bash
databricks bundle deploy -p lab
```

```bash
databricks bundle run fatia1_clientes -p lab
```

A tarefa `gold` reconcilia com os controles do manifesto, grava
`workspace.lab.reconciliacao_fatia1` e **falha o Job** se algum indicador não bater.

## Com um banco de verdade

Os exportadores rodam fora do contêiner, num Python com `sqlalchemy` e o driver do
banco, com a conexão de um **usuário só de leitura** na variável `ESTUDO_DW_URL`:

```bash
python scripts/exportar_dw.py
```

```bash
python scripts/exportar_livro_saida.py --data-inicial 2026-08-01 --prefixo livro_saida_xpto_janela
```

Eles gravam o CSV, o manifesto (linhas e sha256) e os controles calculados pelo
SQL no mesmo retrato do dado; os jobs reconciliam contra esses controles.

## Pastas

| Pasta | O que tem |
| --- | --- |
| `.specs/` | constitution, spec, plan, tasks e checkpoints |
| `src/estudo/` | `bronze`, `silver_clientes`, `de_para`, `gold_reconhecimento`, `livro_saida`, `qualidade`, `sessao` |
| `jobs/` | as duas fatias de ponta a ponta, medidas e reconciliadas |
| `scripts/` | exportadores, gerador da amostra sintética, prova por mutação, histórico Delta |
| `seeds/` | de/para vendedor → base com vigência e as amostras sintéticas |
| `tests/` | pytest com Spark local |
| `notebooks/` | exploração no JupyterLab |
| `docker/` | imagem com Python 3.12, Java 17 e as dependências do `uv.lock` |
| `databricks/`, `databricks.yml` | notebooks e o bundle do Job na Databricks Free Edition |
