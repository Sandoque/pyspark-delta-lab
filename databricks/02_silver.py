# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Tipagem e normalização do lote (documento no formato do ERP, `fim8` dos
# MAGIC telefones), quarentena com motivo e MERGE na `silver_clientes` com deletion
# MAGIC vectors. Falha se a conta das camadas não fechar.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "lab")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from estudo import destino, qualidade, silver_clientes  # noqa: E402

lote = dbutils.jobs.taskValues.get(taskKey="bronze", key="lote", debugValue="")

# COMMAND ----------

t = lambda nome: f"{catalogo}.{esquema}.{nome}"  # noqa: E731

lote_bronze = destino.ler(spark, t("bronze_clientes"))
if lote:  # no Job vem da tarefa bronze; rodando o notebook sozinho, usa o lote mais recente
    lote_bronze = lote_bronze.where(f"_lote = '{lote}'")
else:
    lote = lote_bronze.orderBy("_ingerido_em", ascending=False).first()["_lote"]
    lote_bronze = lote_bronze.where(f"_lote = '{lote}'")

silver, quarentena = silver_clientes.transformar(lote_bronze)
contagens = silver_clientes.contar(lote_bronze, silver, quarentena)
print(contagens, "fecha =", contagens.fecha)
if not contagens.fecha:
    raise AssertionError(f"A conta das camadas não fecha: {contagens}")

silver_clientes.gravar(spark, silver, t("silver_clientes"))
silver_clientes.gravar_quarentena(quarentena, t("quarentena_clientes"), lote)
print(qualidade.checar_silver(destino.ler(spark, t("silver_clientes"))))
