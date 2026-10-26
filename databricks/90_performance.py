# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Performance: OPTIMIZE, Z-ORDER, liquid clustering, explain e skew
# MAGIC Exercício avulso (fora do Job). Gera ~20 milhões de linhas sintéticas com
# MAGIC `spark.range`, sem dado real, e mede:
# MAGIC
# MAGIC 1. **Arquivos pequenos e data skipping**: a mesma consulta antes e depois do
# MAGIC    `OPTIMIZE ... ZORDER BY`, e numa cópia com **liquid clustering**.
# MAGIC 2. **Plano de execução**: o mesmo join com broadcast e com shuffle, lido no `explain()`.
# MAGIC 3. **Skew**: um cliente com 30% das linhas, a distribuição por partição antes e
# MAGIC    depois do *salting*, e a prova de que o resultado não muda.
# MAGIC
# MAGIC Rode célula por célula. Onde estiver escrito **anote**, o número vai para a
# MAGIC tabela `perf_medidas` no fim. O tempo em serverless é só indicativo (cache e
# MAGIC aquecimento); a prova do data skipping é **arquivos lidos × podados** no perfil
# MAGIC da consulta ("Ver desempenho" embaixo da célula).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "lab")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

import contextlib
import io
import re
import time

from pyspark.sql import functions as F

t = lambda nome: f"{catalogo}.{esquema}.{nome}"  # noqa: E731
medidas = []


def anotar(exercicio, medida, valor):
    medidas.append((exercicio, medida, str(valor)))
    print(f"[{exercicio}] {medida} = {valor}")


def cronometrar(df, vezes=3):
    """Melhor de N execuções; a primeira costuma pagar o aquecimento."""
    tempos = []
    for _ in range(vezes):
        inicio = time.perf_counter()
        df.collect()
        tempos.append(time.perf_counter() - inicio)
    return round(min(tempos), 2)


def arquivos(tabela):
    d = spark.sql(f"DESCRIBE DETAIL {tabela}").select("numFiles", "sizeInBytes").first()
    return d["numFiles"], round(d["sizeInBytes"] / 1024 / 1024, 1)


def plano(df):
    """Texto do explain('formatted'); no serverless (Spark Connect) ele só é impresso."""
    saida = io.StringIO()
    with contextlib.redirect_stdout(saida):
        df.explain("formatted")
    return saida.getvalue()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Dados sintéticos
# MAGIC Fato `perf_vendas` com 20 cargas de 1 milhão de linhas. A escrita otimizada e a
# MAGIC compactação automática ficam **desligadas** de propósito, para começar com
# MAGIC arquivos pequenos, cada um cobrindo todos os clientes (o pior caso do data
# MAGIC skipping). O cliente 1 recebe 30% das linhas (o skew do exercício 3).
# MAGIC `delta.targetFileSize = 8mb` faz o OPTIMIZE gerar vários arquivos em vez de um só.

# COMMAND ----------

PROPRIEDADES = """TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact'   = 'false',
  'delta.targetFileSize'             = '8mb'
)"""
COLUNAS = "(id BIGINT, cliente_id INT, dia DATE, valor DECIMAL(15,2))"


def carga(inicio, n=1_000_000):
    return (
        spark.range(inicio, inicio + n)
        .withColumn("cliente_id",
                    F.when(F.col("id") % 10 < 3, F.lit(1))
                    .otherwise(F.pmod(F.hash("id"), F.lit(50_000)) + 2).cast("int"))
        .withColumn("dia", F.date_add(F.lit("2026-01-01").cast("date"),
                                      F.pmod(F.hash("id", F.lit(7)), F.lit(365)).cast("int")))
        .withColumn("valor", (F.pmod(F.hash("id", F.lit(13)), F.lit(100_000)) / 100).cast("decimal(15,2)"))
    )


spark.sql(f"CREATE OR REPLACE TABLE {t('perf_vendas')} {COLUNAS} {PROPRIEDADES}")
inicio = time.perf_counter()
for i in range(20):
    carga(i * 1_000_000).write.mode("append").saveAsTable(t("perf_vendas"))
anotar("0-dados", "segundos para gravar 20 cargas", round(time.perf_counter() - inicio, 1))

(spark.range(1, 50_002)
 .select(F.col("id").cast("int").alias("cliente_id"),
         F.concat(F.lit("UF"), (F.col("id") % 9).cast("string")).alias("uf"),
         F.concat(F.lit("SEG"), (F.col("id") % 4).cast("string")).alias("segmento"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("perf_clientes")))

total = spark.table(t("perf_vendas")).agg(F.count("*").alias("n"), F.sum("valor").alias("soma")).first()
anotar("0-dados", "linhas na fato", total["n"])
anotar("0-dados", "arquivos e MB na fato", arquivos(t("perf_vendas")))

# COMMAND ----------

# MAGIC %md
# MAGIC Confira o histórico: só deve haver `CREATE` e `WRITE`. Se aparecer um `OPTIMIZE`
# MAGIC que você não rodou, é a *predictive optimization* do workspace agindo sozinha;
# MAGIC nesse caso o "antes" do exercício 1 já não é o pior caso, e vale anotar isso.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {t('perf_vendas')}")
        .groupBy("operation").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Arquivos pequenos, Z-ORDER e liquid clustering
# MAGIC Antes de otimizar, crie a cópia com liquid clustering (mesmos dados).
# MAGIC Z-ORDER e liquid clustering não convivem na mesma tabela; por isso são duas.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {t('perf_vendas_lc')} {COLUNAS} CLUSTER BY (cliente_id) {PROPRIEDADES}")
spark.sql(f"INSERT INTO {t('perf_vendas_lc')} SELECT * FROM {t('perf_vendas')}")
anotar("1-liquid", "arquivos e MB antes do OPTIMIZE", arquivos(t("perf_vendas_lc")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Antes.** Rode a consulta de um cliente só. Depois clique em **Ver desempenho**
# MAGIC embaixo da célula e **anote** "arquivos lidos" e "arquivos podados" (files read /
# MAGIC files pruned). O esperado é ler todos: cada arquivo tem clientes de 2 a 50.001,
# MAGIC então o mínimo/máximo do arquivo não exclui nenhum.

# COMMAND ----------

CONSULTA = "SELECT count(*) AS linhas, sum(valor) AS soma FROM {tabela} WHERE cliente_id = 4242"
antes = spark.sql(CONSULTA.format(tabela=t("perf_vendas")))
resultado_antes = antes.first()  # guardado: o DataFrame é preguiçoso e leria a versão nova
display(antes)
anotar("1-zorder", "segundos antes (melhor de 3)", cronometrar(antes))

# COMMAND ----------

ARQUIVOS_LIDOS_ANTES = None  # preencha com o número do perfil, ex.: "25 de 25"
anotar("1-zorder", "arquivos lidos antes (perfil)", ARQUIVOS_LIDOS_ANTES)

# COMMAND ----------

# MAGIC %md
# MAGIC **OPTIMIZE ZORDER BY.** A saída traz `numFilesRemoved` e `numFilesAdded`.

# COMMAND ----------

inicio = time.perf_counter()
display(spark.sql(f"OPTIMIZE {t('perf_vendas')} ZORDER BY (cliente_id)"))
anotar("1-zorder", "segundos do OPTIMIZE ZORDER", round(time.perf_counter() - inicio, 1))
anotar("1-zorder", "arquivos e MB depois", arquivos(t("perf_vendas")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Depois.** Mesma consulta, mesmo perfil. O esperado é ler 1 ou 2 arquivos.
# MAGIC Se a tabela virou um arquivo só, o `targetFileSize` não pegou: anote e siga
# MAGIC (o ponto da entrevista continua, mas sem a poda visível).

# COMMAND ----------

depois = spark.sql(CONSULTA.format(tabela=t("perf_vendas")))
display(depois)
anotar("1-zorder", "segundos depois (melhor de 3)", cronometrar(depois))
assert depois.first() == resultado_antes, "OPTIMIZE não pode mudar o resultado"

# COMMAND ----------

ARQUIVOS_LIDOS_DEPOIS = None  # ex.: "1 de 24"
anotar("1-zorder", "arquivos lidos depois (perfil)", ARQUIVOS_LIDOS_DEPOIS)

# COMMAND ----------

# MAGIC %md
# MAGIC **Liquid clustering.** `OPTIMIZE` sem `ZORDER BY`: a chave já está na tabela.
# MAGIC Rode a consulta e **anote** os arquivos lidos do perfil.

# COMMAND ----------

inicio = time.perf_counter()
display(spark.sql(f"OPTIMIZE {t('perf_vendas_lc')}"))
anotar("1-liquid", "segundos do OPTIMIZE", round(time.perf_counter() - inicio, 1))
anotar("1-liquid", "arquivos e MB depois", arquivos(t("perf_vendas_lc")))

lc = spark.sql(CONSULTA.format(tabela=t("perf_vendas_lc")))
display(lc)
anotar("1-liquid", "segundos da consulta (melhor de 3)", cronometrar(lc))
assert lc.first() == resultado_antes, "a cópia com liquid clustering tem de dar o mesmo resultado"

# COMMAND ----------

ARQUIVOS_LIDOS_LIQUID = None  # ex.: "1 de 24"
anotar("1-liquid", "arquivos lidos (perfil)", ARQUIVOS_LIDOS_LIQUID)

# COMMAND ----------

# MAGIC %md
# MAGIC **Trocar a chave sem reescrever.** Com liquid clustering a chave muda por
# MAGIC `ALTER TABLE`; os arquivos existentes ficam como estão até um `OPTIMIZE FULL`.
# MAGIC Com Z-ORDER, mudar a coluna é rodar outro OPTIMIZE que reescreve tudo.
# MAGIC Veja em `clusteringColumns` e no histórico.

# COMMAND ----------

spark.sql(f"ALTER TABLE {t('perf_vendas_lc')} CLUSTER BY (dia, cliente_id)")
display(spark.sql(f"DESCRIBE DETAIL {t('perf_vendas_lc')}").select("clusteringColumns", "numFiles"))
display(spark.sql(f"DESCRIBE HISTORY {t('perf_vendas_lc')}")
        .select("version", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Lendo o plano: broadcast × shuffle
# MAGIC O mesmo join (fato 20 mi × dimensão 50 mil) de três jeitos: deixando o Spark
# MAGIC escolher, forçando broadcast e forçando shuffle (`hint("merge")`). No plano,
# MAGIC procure o tipo de join e quantos `Exchange` (shuffle) aparecem. Com Photon os
# MAGIC nomes vêm prefixados (`PhotonBroadcastHashJoin`, `PhotonShuffleExchange...`), e
# MAGIC o Photon pode trocar sort-merge por *shuffled hash join*: o que importa é
# MAGIC **broadcast ou shuffle dos dois lados**.

# COMMAND ----------

fato = spark.table(t("perf_vendas"))
dim = spark.table(t("perf_clientes"))
por_uf = lambda d: fato.join(d, "cliente_id").groupBy("uf").agg(F.sum("valor").alias("total"))  # noqa: E731

consultas = {
    "automático": por_uf(dim),
    "broadcast forçado": por_uf(F.broadcast(dim)),
    "shuffle forçado": por_uf(dim.hint("merge")),
}
JOIN = re.compile(r"\b\w*(?:BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin)\b")
for rotulo, df in consultas.items():
    texto = plano(df)
    joins = sorted(set(JOIN.findall(texto)))
    trocas = sum(1 for linha in texto.splitlines() if re.match(r"^\(\d+\).*Exchange", linha))
    anotar("2-explain", f"{rotulo}: join", ", ".join(joins) or "não achado (leia o plano abaixo)")
    anotar("2-explain", f"{rotulo}: nós Exchange", trocas)
    anotar("2-explain", f"{rotulo}: segundos (melhor de 3)", cronometrar(df))

resultados = {r: sorted(df.collect()) for r, df in consultas.items()}
assert len({tuple(v) for v in resultados.values()}) == 1, "os três joins têm de dar o mesmo total por UF"
print("os três joins dão o mesmo total por UF")

# COMMAND ----------

# MAGIC %md
# MAGIC Leia pelo menos um plano inteiro. No topo vem `AdaptiveSparkPlan isFinalPlan=false`:
# MAGIC é o plano **antes** de rodar; o AQE pode mudá-lo em execução (por exemplo, virar
# MAGIC broadcast quando descobre que um lado ficou pequeno). O plano final está no perfil.

# COMMAND ----------

print(plano(consultas["shuffle forçado"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Skew
# MAGIC Distribui a fato em 32 partições pela chave, como um shuffle de join ou de janela
# MAGIC faria, e conta linhas por partição. O cliente 1 (30% das linhas) cai inteiro em
# MAGIC uma partição: uma tarefa faz o trabalho de ~10. Com *salting* (`id % 16` na
# MAGIC chave) ele se espalha.
# MAGIC
# MAGIC Atenção, que vale para a entrevista: num `groupBy` com `sum` o Spark já faz
# MAGIC agregação parcial antes do shuffle, então o skew pesa pouco ali. Ele dói em
# MAGIC **join** e em **janela** (`Window.partitionBy`). Em join, o AQE do Databricks
# MAGIC trata skew sozinho; salting é para quando ele não cobre.

# COMMAND ----------

def distribuicao(df, chaves, n=32):
    linhas = sorted(r["count"] for r in
                    df.repartition(n, *chaves).groupBy(F.spark_partition_id()).count().collect())
    mediana = linhas[len(linhas) // 2]
    return linhas[-1], mediana, round(linhas[-1] / mediana, 1)


maior, mediana, razao = distribuicao(fato, ["cliente_id"])
anotar("3-skew", "sem sal: maior partição / mediana", f"{maior:,} / {mediana:,} = {razao}x")

maior, mediana, razao = distribuicao(fato.withColumn("sal", F.col("id") % 16), ["cliente_id", "sal"])
anotar("3-skew", "com sal: maior partição / mediana", f"{maior:,} / {mediana:,} = {razao}x")

# COMMAND ----------

# MAGIC %md
# MAGIC O sal muda a forma, não o resultado: agregação em dois estágios
# MAGIC (`cliente_id, sal` e depois `cliente_id`) tem de bater com a de um estágio,
# MAGIC cliente a cliente.

# COMMAND ----------

um = fato.groupBy("cliente_id").agg(F.sum("valor").alias("total"))
dois = (fato.withColumn("sal", F.col("id") % 16)
        .groupBy("cliente_id", "sal").agg(F.sum("valor").alias("parcial"))
        .groupBy("cliente_id").agg(F.sum("parcial").alias("total")))
diferentes = (um.alias("a").join(dois.alias("b"), "cliente_id", "full_outer")
              .where(~F.col("a.total").eqNullSafe(F.col("b.total"))).count())
anotar("3-skew", "clientes com total diferente (1 × 2 estágios)", diferentes)
assert diferentes == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro e limpeza
# MAGIC Grava as medidas em `perf_medidas` (fica para a demonstração) e, se quiser,
# MAGIC apaga as tabelas grandes para não gastar a cota da Free Edition.

# COMMAND ----------

(spark.createDataFrame(medidas, "exercicio string, medida string, valor string")
 .withColumn("medido_em", F.current_timestamp())
 .write.mode("append").saveAsTable(t("perf_medidas")))
display(spark.table(t("perf_medidas")).orderBy("medido_em", "exercicio"))

# COMMAND ----------

LIMPAR = False  # mude para True para apagar as tabelas grandes
if LIMPAR:
    for nome in ("perf_vendas", "perf_vendas_lc", "perf_clientes"):
        spark.sql(f"DROP TABLE IF EXISTS {t(nome)}")
    print("tabelas grandes apagadas; perf_medidas ficou")
