# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Landing
# MAGIC Copia a amostra sintética (CSV + manifesto) e o seed do de/para, que vêm no
# MAGIC bundle, para o Volume `landing`. Num cenário real, esta tarefa seria a
# MAGIC chegada do arquivo (ou um Auto Loader observando o Volume).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "lab")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

# COMMAND ----------

import os
import shutil
from pathlib import Path

raiz = Path(os.getcwd()).parent  # o notebook roda dentro de <bundle>/databricks
volume = Path(f"/Volumes/{catalogo}/{esquema}/landing")
arquivos = [
    raiz / "seeds" / "amostra_sintetica" / "clientes_xpto_sintetico.csv",
    raiz / "seeds" / "amostra_sintetica" / "clientes_xpto_sintetico.json",
    raiz / "seeds" / "de_para_vendedor_base.csv",
]
for arquivo in arquivos:
    shutil.copyfile(arquivo, volume / arquivo.name)
    print(f"{arquivo.name} -> {volume / arquivo.name}")
