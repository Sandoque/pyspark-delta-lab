"""Gera amostra 100% sintética no formato da landing + manifesto (T14).

Serve para rodar a fatia 1 fora da máquina (ex.: Databricks Free Edition ou
clone público do GitHub) sem nenhum dado real da operação (constitution.md).

Gera:
  seeds/amostra_sintetica/clientes_xpto_sintetico.csv
  seeds/amostra_sintetica/clientes_xpto_sintetico.json

Uso:
    python scripts/gerar_amostra_sintetica.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
import re
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "seeds" / "amostra_sintetica"
COLUNAS = (
    "revenda", "codigo_cliente", "nome", "nome_fantasia", "cidade", "cnpj_cpf",
    "vendedor", "categoria", "tabela_preco", "telefone", "celular", "telefone_b2b",
    "cancelado",
)
DIA_REF = date(2026, 10, 23)


def _cpf_erp_15(base9: str) -> str:
    """Gera CPF válido a partir de 9 dígitos e devolve no formato de 15 dígitos do ERP."""
    digs = [int(c) for c in base9]
    for n in (9, 10):
        soma = sum(digs[i] * (n + 1 - i) for i in range(n))
        dv = (soma * 10) % 11 % 10
        digs.append(dv)
    s15 = f"{base9}0000{digs[9]}{digs[10]}"
    # Simula o ERP engolindo o zero à esquerda
    return s15.lstrip("0") or "0"


def _cnpj_erp(base8: str, filial: str = "0001") -> str:
    """Gera CNPJ válido de 14 dígitos e remove zero à esquerda como no ERP."""
    digs = [int(c) for c in (base8 + filial)]
    pesos = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for n in (12, 13):
        p = pesos if n == 12 else [6] + pesos
        soma = sum(digs[i] * p[i] for i in range(n))
        resto = soma % 11
        digs.append(0 if resto < 2 else 11 - resto)
    s14 = "".join(str(d) for d in digs)
    return s14.lstrip("0") or "0"


def _fim8(valor: str) -> str | None:
    d = re.sub(r"[^0-9]", "", valor or "")
    return d[-8:] if len(d) >= 8 else None


def _vendedores_da_base(base: str, dia: date) -> list[str]:
    seed = RAIZ / "seeds" / "de_para_vendedor_base.csv"
    with seed.open(encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    res = []
    for linha in linhas:
        de = date.fromisoformat(linha["valido_de"]) if linha["valido_de"] else date.min
        ate = date.fromisoformat(linha["valido_ate"]) if linha["valido_ate"] else date.max
        if linha["base"] == base and de <= dia < ate:
            res.append(linha["vendedor"])
    return res


def _calcular_controles_sql_equivalente(linhas_validas_unicas: list[dict], dia: date) -> dict:
    """Calcula os mesmos 4 indicadores por base que a CONSULTA_CONTROLES do SQL."""
    pares_ambos: set[tuple[str, str]] = set()
    pares_tel: set[tuple[str, str]] = set()
    for r in linhas_validas_unicas:
        cod = r["codigo_cliente"].strip()
        t = _fim8(r["telefone"])
        c = _fim8(r["celular"])
        if t:
            pares_tel.add((cod, t))
            pares_ambos.add((cod, t))
        if c:
            pares_ambos.add((cod, c))

    donos_tel: dict[str, set[str]] = {}
    for cod, fim in pares_tel:
        donos_tel.setdefault(fim, set()).add(cod)

    donos_ambos: dict[str, set[str]] = {}
    for cod, fim in pares_ambos:
        donos_ambos.setdefault(fim, set()).add(cod)

    controles = {}
    for base in ("sku", "volume", "cobertura"):
        vendedores = _vendedores_da_base(base, dia)
        ativos = [
            r["codigo_cliente"].strip()
            for r in linhas_validas_unicas
            if r["vendedor"].strip() in vendedores and r["cancelado"].strip() != "1"
        ]
        ativos_set = set(ativos)
        rec_tel = {
            cod for cod, fim in pares_tel if cod in ativos_set and len(donos_tel[fim]) == 1
        }
        rec_ambos = {
            cod for cod, fim in pares_ambos if cod in ativos_set and len(donos_ambos[fim]) == 1
        }
        repetidos = {
            cod for cod, fim in pares_ambos if cod in ativos_set and len(donos_ambos[fim]) > 1
        }
        controles[base] = {
            "vendedores": vendedores,
            "clientes_ativos": len(ativos),
            "reconhecidos_so_telefone": len(rec_tel),
            "reconhecidos_telefone_ou_celular": len(rec_ambos),
            "com_numero_repetido": len(repetidos),
            "segundos_sql": 0.005,
        }
    return controles


def gerar() -> Path:
    rng = random.Random(42)
    DESTINO.mkdir(parents=True, exist_ok=True)
    caminho_csv = DESTINO / "clientes_xpto_sintetico.csv"

    vendedores = (
        ["111", "112", "113", "121", "122"]
        + ["701", "702", "703", "704"]
        + ["131", "132", "133", "134"]
    )
    cidades = ["CIDADE ALFA", "VILA BETA", "PORTO GAMA", "SERRA DELTA"]
    categorias = ["MERCEARIA", "BAR", "SUPERMERCADO", "PADARIA", "DEPOSITO"]

    linhas_validas: list[dict] = []
    for idx in range(1, 181):
        cod = str(10000 + idx)
        ven = vendedores[idx % len(vendedores)]
        # Formato de documento variado (CPF 15, CPF 14 com zero engolido, CNPJ 14, CNPJ 13, DV errado, desconhecido)
        mod_doc = idx % 12
        if mod_doc in (0, 1, 2, 3, 4):
            doc = _cpf_erp_15(f"{rng.randint(100000000, 999999999)}")
        elif mod_doc in (5, 6):
            doc = _cpf_erp_15(f"0{rng.randint(10000000, 99999999)}")
        elif mod_doc in (7, 8):
            doc = _cnpj_erp(f"{rng.randint(10000000, 99999999)}")
        elif mod_doc == 9:
            doc = _cnpj_erp(f"0{rng.randint(1000000, 9999999)}")
        elif mod_doc == 10:
            doc = "529982247000026"  # DV inválido proposital
        else:
            doc = "999"  # desconhecido

        # Telefones: únicos, só celular, compartilhados ou vazios
        mod_tel = idx % 10
        tel_unico = f"8291{idx:06d}"
        cel_unico = f"8292{idx:06d}"
        tel_compartilhado = f"82990000{idx % 5:02d}"
        if mod_tel in (0, 1, 2):
            tel, cel = tel_unico, ""
        elif mod_tel in (3, 4):
            tel, cel = "", f"(82) 9 {cel_unico[3:7]}-{cel_unico[7:]}"
        elif mod_tel in (5, 6):
            tel, cel = tel_unico, cel_unico
        elif mod_tel == 7:
            tel, cel = tel_compartilhado, ""
        elif mod_tel == 8:
            tel, cel = tel_unico, tel_compartilhado
        else:
            tel, cel = "1234", ""

        cancelado = "1" if idx % 15 == 0 else "0"
        linhas_validas.append({
            "revenda": "XPTO",
            "codigo_cliente": cod,
            "nome": f'CLIENTE SINTETICO "{idx}", LTDA',
            "nome_fantasia": f"FANTASIA {idx}",
            "cidade": cidades[idx % len(cidades)],
            "cnpj_cpf": doc,
            "vendedor": ven,
            "categoria": categorias[idx % len(categorias)],
            "tabela_preco": str(45 + (idx % 4)),
            "telefone": tel,
            "celular": cel,
            "telefone_b2b": "",
            "cancelado": cancelado,
        })

    # Casos de quarentena e duplicata exata para testar FR-05 na amostra sintética:
    duplicata_exata = dict(linhas_validas[0])
    conflito_a = {
        **linhas_validas[0],
        "codigo_cliente": "99901",
        "nome": "CONFLITO A",
        "vendedor": "132",
        "telefone": "82977770001",
    }
    conflito_b = {
        **conflito_a,
        "nome": "CONFLITO B",
        "vendedor": "133",
    }
    sem_codigo = {
        **linhas_validas[0],
        "codigo_cliente": "   ",
        "nome": "SEM CODIGO",
    }
    outra_revenda = {
        **linhas_validas[0],
        "revenda": "OutraRevenda",
        "codigo_cliente": "99902",
        "nome": "OUTRA REVENDA",
    }

    todas = linhas_validas + [duplicata_exata, conflito_a, conflito_b, sem_codigo, outra_revenda]

    with caminho_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS)
        w.writeheader()
        w.writerows(todas)

    sha256 = hashlib.sha256(caminho_csv.read_bytes()).hexdigest()
    controles = _calcular_controles_sql_equivalente(linhas_validas, DIA_REF)
    manifesto = {
        "arquivo": caminho_csv.name,
        "linhas": len(todas),
        "sha256": sha256,
        "revenda": "XPTO",
        "consulta": "AMOSTRA SINTETICA DETERMINISTICA (seed=42) — SEM DADO REAL",
        "exportado_em": "2026-10-23T16:00:00",
        "segundos_exportacao": 0.01,
        "dia_referencia": DIA_REF.isoformat(),
        "controles": controles,
    }
    caminho_csv.with_suffix(".json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Amostra sintética gerada: {len(todas)} linhas -> {caminho_csv.relative_to(RAIZ)}")
    return caminho_csv


COLUNAS_LIVRO = (
    "REVENDA", "NF", "LIVRO", "CHAVE_NF", "DATA_E", "CFOP", "COD_NATUREZA",
    "CODCOMP", "CNPJ", "CODCLI", "COD_PRD_RED", "QTD", "QTD_INTEIRA",
    "QTD_AVULSA", "INCIDENCIA", "VLR_PRODUTO", "VLR_IPI", "VLR_ICMS",
    "VLR_PIS", "VLR_COFINS", "FRETE", "ADIC_FINANCEIRO", "VLR_TOTAL",
    "VLR_TOTAL_NOTA_CALC",
)


def gerar_livro_saida() -> Path:
    """Gera amostra sintética do Livro de Saída (T20/FR-17) com 3 competências + quarentena."""
    from decimal import Decimal

    rng = random.Random(42)
    DESTINO.mkdir(parents=True, exist_ok=True)
    caminho_csv = DESTINO / "livro_saida_xpto_sintetico.csv"

    competencias = [
        (2026, 8, "2026-08-15"),
        (2026, 9, "2026-09-18"),
        (2026, 10, "2026-10-10"),
    ]
    linhas_validas: list[dict] = []
    for idx in range(1, 91):
        ano, mes, data_e = competencias[idx % len(competencias)]
        # Simula colisão de número de NF (ex.: NF 8 com chaves distintas na mesma competência)
        nf = "8" if idx in (1, 4) else str(5000 + idx)
        chave = f"3526{mes:02d}{idx:038d}"
        vlr_prod = Decimal(f"{rng.randint(100, 50000)}.{rng.randint(0, 99):02d}")
        vlr_icms = (vlr_prod * Decimal("0.18")).quantize(Decimal("0.01"))
        vlr_pis = (vlr_prod * Decimal("0.0165")).quantize(Decimal("0.01"))
        vlr_cofins = (vlr_prod * Decimal("0.0760")).quantize(Decimal("0.01"))
        linhas_validas.append({
            "REVENDA": "XPTO",
            "NF": nf,
            "LIVRO": "S",
            "CHAVE_NF": chave,
            "DATA_E": data_e,
            "CFOP": "5102" if idx % 2 == 0 else "5405",
            "COD_NATUREZA": "1",
            "CODCOMP": "1",
            "CNPJ": f"112223330001{idx % 89 + 10:02d}",
            "CODCLI": str(1000 + (idx % 30)),
            "COD_PRD_RED": str(200 + (idx % 15)),
            "QTD": str(1 + (idx % 10)),
            "QTD_INTEIRA": str(1 + (idx % 10)),
            "QTD_AVULSA": "0",
            "INCIDENCIA": "1",
            "VLR_PRODUTO": f"{vlr_prod:.2f}",
            "VLR_IPI": "0.00",
            "VLR_ICMS": f"{vlr_icms:.2f}",
            "VLR_PIS": f"{vlr_pis:.2f}",
            "VLR_COFINS": f"{vlr_cofins:.2f}",
            "FRETE": "0.00",
            "ADIC_FINANCEIRO": "0.00",
            "VLR_TOTAL": f"{vlr_prod:.2f}",
            "VLR_TOTAL_NOTA_CALC": f"{vlr_prod:.2f}",
        })

    # Duplicata exata + 4 casos de quarentena (revenda_diferente, sem_nf, sem_data_emissao, valor_invalido)
    dup_exata = dict(linhas_validas[0])
    q_revenda = {**linhas_validas[0], "REVENDA": "OutraRevenda", "NF": "9001"}
    q_sem_nf = {**linhas_validas[0], "NF": "   "}
    q_sem_data = {**linhas_validas[0], "NF": "9002", "DATA_E": "data-invalida"}
    q_valor_inv = {**linhas_validas[0], "NF": "9003", "VLR_TOTAL_NOTA_CALC": "abc"}

    todas = linhas_validas + [dup_exata, q_revenda, q_sem_nf, q_sem_data, q_valor_inv]

    with caminho_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_LIVRO)
        w.writeheader()
        w.writerows(todas)

    controles_mensais = {}
    for ano, mes, _ in competencias:
        comp = f"{ano:04d}-{mes:02d}"
        recorte = [r for r in linhas_validas if r["DATA_E"].startswith(comp)]
        chaves = {f"{r['REVENDA']}|{r['NF']}|{r['LIVRO']}|{r['CHAVE_NF']}" for r in recorte}
        nfs_num = {r["NF"] for r in recorte}
        controles_mensais[comp] = {
            "ano": ano,
            "mes": mes,
            "itens": len(recorte),
            "nfs_distintas": len(chaves),
            "nfs_so_numero": len(nfs_num),
            "vlr_produto": f"{sum(Decimal(r['VLR_PRODUTO']) for r in recorte):.2f}",
            "vlr_icms": f"{sum(Decimal(r['VLR_ICMS']) for r in recorte):.2f}",
            "vlr_pis": f"{sum(Decimal(r['VLR_PIS']) for r in recorte):.2f}",
            "vlr_cofins": f"{sum(Decimal(r['VLR_COFINS']) for r in recorte):.2f}",
            "vlr_total_nota_calc": f"{sum(Decimal(r['VLR_TOTAL_NOTA_CALC']) for r in recorte):.2f}",
            "vlr_total_double_bruto": float(sum(Decimal(r["VLR_TOTAL_NOTA_CALC"]) for r in recorte)),
        }

    sha256 = hashlib.sha256(caminho_csv.read_bytes()).hexdigest()
    manifesto = {
        "arquivo": caminho_csv.name,
        "linhas": len(todas),
        "sha256": sha256,
        "revenda": "XPTO",
        "data_inicial": "2026-08-01",
        "data_final": "2026-10-31",
        "consulta": "AMOSTRA SINTETICA LIVRO DE SAIDA (seed=42) — SEM DADO REAL",
        "exportado_em": "2026-10-23T16:50:00",
        "segundos_exportacao": 0.01,
        "segundos_sql_controles": 0.01,
        "competencias": len(controles_mensais),
        "controles_mensais": controles_mensais,
    }
    caminho_csv.with_suffix(".json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Amostra sintética Livro de Saída gerada: {len(todas)} linhas -> {caminho_csv.relative_to(RAIZ)}")
    return caminho_csv


if __name__ == "__main__":
    gerar()
    gerar_livro_saida()

