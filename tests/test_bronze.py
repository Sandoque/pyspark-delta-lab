"""Bronze: confere o manifesto e é idempotente pelo lote."""

import csv
import hashlib
import json

import pytest

from estudo import bronze


def _exportacao(pasta, linhas):
    caminho = pasta / "clientes_xpto_teste.csv"
    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(bronze.COLUNAS)
        escritor.writerows(linhas)
    manifesto = {"linhas": len(linhas), "sha256": hashlib.sha256(caminho.read_bytes()).hexdigest()}
    caminho.with_suffix(".json").write_text(json.dumps(manifesto), encoding="utf-8")
    return caminho


LINHA = ["XPTO", "1", 'BAR "DO ZE", LTDA', "BAR DO ZE", "CIDADE A", "529982247000025",
         "132", "BAR", "47", "82999990000", "", "", "0"]


def test_ingere_uma_vez_so_e_le_aspas_e_virgulas(spark, tmp_path):
    caminho = _exportacao(tmp_path, [LINHA, ["XPTO", "2"] + [""] * 11])
    destino = str(tmp_path / "bronze")

    primeira = bronze.ingerir(spark, caminho, destino)
    segunda = bronze.ingerir(spark, caminho, destino)

    assert (primeira.linhas_inseridas, primeira.ja_ingerido) == (2, False)
    assert (segunda.linhas_inseridas, segunda.ja_ingerido) == (0, True)
    df = spark.read.format("delta").load(destino)
    assert df.count() == 2
    assert df.where("codigo_cliente = '1'").first()["nome"] == 'BAR "DO ZE", LTDA'


def test_arquivo_diferente_do_manifesto_e_recusado(spark, tmp_path):
    caminho = _exportacao(tmp_path, [LINHA])
    caminho.write_text(caminho.read_text(encoding="utf-8") + "XPTO,9\n", encoding="utf-8")

    with pytest.raises(ValueError, match="sha256"):
        bronze.ingerir(spark, caminho, str(tmp_path / "bronze"))
