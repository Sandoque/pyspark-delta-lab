# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold e reconciliação
# MAGIC Indicador de reconhecimento pelo número, por base e dia, gravado com
# MAGIC `replaceWhere` no dia. Reconcilia com os controles do manifesto (calculados
# MAGIC pela regra do SQL) e **falha o Job** se algum indicador não bater.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "lab")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath("../src"))

from estudo import bronze, de_para, destino, gold_reconhecimento, qualidade  # noqa: E402

landing = f"/Volumes/{catalogo}/{esquema}/landing"
manifesto = bronze.ler_manifesto(f"{landing}/clientes_xpto_sintetico.csv")
dia = date.fromisoformat(
    dbutils.jobs.taskValues.get(taskKey="bronze", key="dia_referencia", debugValue=manifesto["dia_referencia"])
)

# COMMAND ----------

t = lambda nome: f"{catalogo}.{esquema}.{nome}"  # noqa: E731

seed = de_para.ler_seed(spark, f"{landing}/de_para_vendedor_base.csv")
gold = gold_reconhecimento.calcular(destino.ler(spark, t("silver_clientes")), de_para.bases_no_dia(seed, dia), dia)
gold_reconhecimento.gravar(gold, t("gold_reconhecimento"), dia)

reconciliacao = qualidade.reconciliar(destino.ler(spark, t("gold_reconhecimento")).where(f"dia_referencia = DATE'{dia}'"),
                                      manifesto["controles"])
(
    spark.createDataFrame(reconciliacao)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(t("reconciliacao_fatia1"))
)
display(spark.table(t("reconciliacao_fatia1")))

# COMMAND ----------

diferencas = [r for r in reconciliacao if not r["bate"]]
print(f"reconciliação: {len(reconciliacao) - len(diferencas)} de {len(reconciliacao)} indicadores iguais")
if diferencas:
    raise AssertionError(f"Indicadores diferentes dos controles do SQL: {diferencas}")
