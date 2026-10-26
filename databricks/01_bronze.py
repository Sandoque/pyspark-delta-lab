# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Arquivo do Volume → tabela Delta `bronze_clientes`, tudo como texto, idempotente
# MAGIC pelo sha256 do lote. Passa o lote e o dia de referência para as próximas tarefas
# MAGIC por *task values*.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "lab")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))  # o pacote do repositório, sincronizado pelo bundle

from estudo import bronze  # noqa: E402

# COMMAND ----------

arquivo = f"/Volumes/{catalogo}/{esquema}/landing/clientes_xpto_sintetico.csv"
resultado = bronze.ingerir(spark, arquivo, f"{catalogo}.{esquema}.bronze_clientes")
manifesto = bronze.ler_manifesto(arquivo)
print(resultado)

dbutils.jobs.taskValues.set(key="lote", value=resultado.lote)
dbutils.jobs.taskValues.set(key="dia_referencia", value=manifesto["dia_referencia"])
