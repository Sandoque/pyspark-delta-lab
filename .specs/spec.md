# Spec — PySpark + Delta Lab

Versão: 0.3 — 26/10/2026\
Regras: [constitution.md](constitution.md)

---

# Fatia 1 — cadastro de clientes

## 1. Problema real que a fatia reproduz

Num atendimento automático por WhatsApp, o cliente é reconhecido pelo número de
onde escreve. Clientes que existiam no cadastro caíam num formulário manual
porque:

- o número estava no campo `celular`, e o reconhecimento só olhava o `telefone`;
- o documento digitado não casava com o cadastro, que guarda **CPF com 15
  dígitos** (9 primeiros + `0000` + 2 verificadores) e engole zeros à esquerda;
- a composição de uma carteira comercial ("base") mudou numa data: um vendedor
  saiu e outro entrou.

Medido no banco de produção, na base afetada: olhar também o `celular` levou os
clientes reconhecidos sozinhos de **~36% para ~90%** dos ativos.

A fatia 1 refaz esse caminho em PySpark + Delta — ingestão, normalização,
de/para, validação e o indicador final — e o resultado tem de bater com o SQL de
produção.

## 2. Requisitos

| ID | Requisito | Critério de aceite |
| --- | --- | --- |
| FR-01 | Ambiente local reproduzível: PySpark + Delta Lake + JupyterLab + pytest num contêiner | `docker compose build` e `docker compose run --rm spark pytest` passam numa máquina limpa |
| FR-02 | Exportação do cadastro para arquivo bruto | Script só `SELECT` em `READ ONLY`; CSV em `data/landing/` com **manifesto** (linhas, sha256, consulta, horário) e **controles** calculados pelo SQL de produção |
| FR-03 | Bronze: arquivo bruto → tabela Delta, tudo como texto, com metadados de ingestão | Contagem bronze = linhas do manifesto; o mesmo arquivo de novo não duplica (idempotente pelo sha256 do lote) |
| FR-04 | Silver: tipagem e normalização | Documento no formato normal (CPF de 15 → 11 dígitos; CNPJ com 14); `telefone`, `celular` e `telefone_b2b` nos 8 últimos dígitos; `cancelado` booleano; um registro por `codigo_cliente` |
| FR-05 | Quarentena com motivo | Sem código, revenda diferente, código duplicado com conteúdo diferente: vão para `quarentena_clientes` com `motivo`; `bronze = silver + quarentena + duplicatas exatas` |
| FR-06 | De/para vendedor → base, com vigência | Seed versionado com `valido_de` / `valido_ate`; a base de um vendedor depende do dia de referência |
| FR-07 | Gold: reconhecimento pelo número | Por dia e base: clientes ativos, reconhecíveis só pelo `telefone`, por `telefone` ou `celular`, e com número repetido em mais de um cliente |
| FR-08 | Reconciliação com a produção | Os números do FR-07 **iguais** aos controles do FR-02; qualquer diferença é relatada, não arredondada |
| FR-09 | Medição | Tempo de cada etapa no relatório, ao lado do tempo do SQL de produção |
| FR-10 | Relatório da execução | JSON em `reports/` com contagens por camada, quarentena por motivo, reconciliação e tempos |

## 3. Critério do teste de uso

Ao fim de cada fatia, registrar em `checkpoints.md`: (1) bateu com a produção?
(2) quanto tempo, contra o SQL? (3) o que ficou mais simples e o que ficou mais
complexo? (4) onde, se em algum lugar, Spark + Delta valeriam no pipeline, e o
que precisaria ser verdade para isso.

---

# Fatia 2 — livro fiscal de saída (~1,6 milhão de itens)

## 1. Problema real que a fatia reproduz

Na fatia 1 (~30 mil clientes) o SQL foi ~5× mais rápido que o Spark: em tabela
pequena, o custo fixo da JVM não se paga. O volume de verdade está no **livro
fiscal de saída** (um item por linha de nota; ~1,6 milhão de itens só nesta
revenda, em 30 competências):

1. **Reprocessamento inteiro toda noite:** a extração relê só uma janela de 3
   meses, mas a consolidação seguinte regrava o histórico inteiro — em pandas,
   precisando de um subprocesso por revenda para caber na memória.
2. **Perda de centavos em `float32` e ruído em `DOUBLE`:** totais mensais de
   alguns milhões de reais não cabem com centavos em precisão simples, e somar
   `DOUBLE` acumula ruído binário (`…08000004`).
3. **Grão da nota fiscal:** agrupar só por `(revenda, nf)` fundia notas
   diferentes; o grão correto é `(revenda, nf, livro, chave_nf)`.

## 2. Requisitos

| ID | Requisito | Critério de aceite |
| --- | --- | --- |
| FR-11 | Exportação `READ ONLY` do livro de saída | Completo ou janela (`--data-inicial`/`--data-final`) para CSV + manifesto SHA-256 + controles mensais do SQL em `DECIMAL(15,2)` |
| FR-12 | Bronze idempotente por lote | Confere SHA-256 e contagem do manifesto; o mesmo arquivo de novo não duplica |
| FR-13 | Silver em `DECIMAL(15,2)`, particionada por `(ano, mes)` | Chave da nota `(revenda, nf, livro, chave_nf)`; quarentena com motivo (`sem_nf`, `sem_data_emissao`, `revenda_diferente`, `valor_invalido`) fechando a conta; gravação substituindo só as partições `(ano, mes)` do lote (`replaceWhere`) |
| FR-14 | Gold mensal | Por `(revenda, ano, mes)`: itens, NFs distintas pela chave completa, somas em `DECIMAL(15,2)` e a diferença medida contra `float32`; `replaceWhere` nos meses do lote |
| FR-15 | Reconciliação ao centavo | 100% das competências com diferença `0,00` contra o SQL, em valores e contagens |
| FR-16 | Benchmark: carga completa × janela de 3 meses | Tempo, linhas gravadas e arquivos Parquet tocados nos dois casos |
| FR-17 | Amostra sintética, testes e mutação | Amostra versionada, `pytest` ponta a ponta e mutações das regras fiscais |

---

# Fatia 3 (próxima) — orquestração na Databricks Free Edition

Rodar a fatia 1 sobre a amostra sintética como um **Job** de três tarefas
(bronze → silver → gold) na Databricks Free Edition, com as tabelas no Unity
Catalog e o arquivo de entrada num Volume. Critério: o Job termina verde e a
reconciliação dá 12 de 12, como no contêiner.
