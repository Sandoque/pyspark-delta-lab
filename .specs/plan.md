# Plan

Versão: 0.3 — 26/10/2026. Atende a [spec.md](spec.md).

## Arquitetura

```text
Fora do contêiner                          Contêiner "spark" (Linux)
─────────────────                          ─────────────────────────
scripts/exportar_dw.py                     src/estudo/  (pacote)
scripts/exportar_livro_saida.py              bronze.py           landing → Delta bronze
  SELECT READ ONLY (ESTUDO_DW_URL)           silver_clientes.py  tipagem, normalização, quarentena, MERGE
        │                                    de_para.py          vendedor → base com vigência
        ▼                                    gold_reconhecimento.py  indicador por base/dia
data/landing/<arquivo>.csv                   livro_saida.py      bronze/silver/gold fiscal, replaceWhere(ano,mes)
data/landing/<arquivo>.json                  qualidade.py        reconciliação e checagens
  (manifesto + controles do SQL)           jobs/fatia1_clientes.py, jobs/fatia2_livro_saida.py
        │                                  tests/  pytest com Spark local
        └──────── volume /work ───────────► data/delta/…        reports/<fatia>_<ts>.json

scripts/gerar_amostra_sintetica.py ──► seeds/amostra_sintetica/  (versionada; roda sem banco)
```

## Decisões

| Decisão | Por quê |
| --- | --- |
| Spark **4.0** + Delta **4.0** + Python 3.12 + Java 17 | Mesma geração do Databricks atual (ANSI ligado por padrão), para o código rodar na Free Edition sem surpresa |
| Contêiner em vez de instalar no Windows | Spark no Windows exige `winutils`/`HADOOP_HOME` para gravar; o contêiner é Linux |
| Exportação fora do contêiner, em CSV | O banco de origem só aceita conexão local; CSV com tudo como texto é o "arquivo bruto" da bronze, como na vida real |
| Controles calculados pelo SQL na mesma transação da exportação | A reconciliação compara Spark e produção sobre **o mesmo retrato** do dado |
| Bronze tudo texto + `_lote` (sha256 do arquivo) + `_ingerido_em` | Bronze é cópia fiel; tipar é trabalho da silver. O sha256 torna a ingestão idempotente |
| Silver com `MERGE` por `codigo_cliente` e **deletion vectors** | Idempotente e incremental sem reescrever arquivos inteiros (medido em T12) |
| De/para em seed CSV versionado | Regra comercial que muda por decisão vive em arquivo legível, não em `CASE` espalhado |
| Gold recalculada por dia (`replaceWhere`) | Recalcular um dia é simples, idempotente e não toca os outros |
| Livro fiscal em `DECIMAL(15,2)`, particionado por `(ano, mes)` | Centavo exato; reprocessar uma janela reescreve só as partições dela |
| `repartition("ano", "mes")` antes do `partitionBy` | Uma competência por tarefa: 1 arquivo por mês e sem centenas de escritores Parquet abertos na heap (T21) |
| Projeto gerido pelo **uv** (`pyproject.toml` + `uv.lock`) | Uma lista de dependências só; o Dockerfile instala pelo `uv.lock` |

## Regras de normalização (FR-04)

- **Documento:** só dígitos, completados com zeros à esquerda até 15 (o cadastro
  engole o zero da frente). Com 15 dígitos, as posições 10–13 são a "filial":
  **`0000` é CPF guardado no formato do ERP** (9 primeiros + 2 últimos = CPF de
  11) — todo CNPJ real tem filial a partir de `0001`; senão é CNPJ (os 14
  últimos). Exemplos sintéticos: `529982247000025` → CPF 529.982.247-25;
  `12345678000090` (14, zero engolido) → CPF 012.345.678-90; `1222333000128` →
  CNPJ 01.222.333/0001-28. Com 11 dígitos, CPF como está. O resto:
  `documento_tipo = 'desconhecido'`. Os dígitos verificadores viram a coluna
  `documento_valido` (marca, não quarentena: o cliente existe mesmo com documento
  errado no cadastro).
- **Telefones:** só dígitos; `fim8` = 8 últimos dígitos quando há pelo menos 8.
- **Reconhecimento (FR-07):** um número (`fim8`) reconhece o cliente quando
  pertence a **um só** cliente do cadastro inteiro (cancelados inclusive),
  olhando os campos considerados (`telefone`, ou `telefone` + `celular`).

## Livro fiscal (fatia 2)

- **Chave da nota:** `concat_ws('|', revenda, nf, coalesce(livro,''), coalesce(chave_nf,''))`
  — não funde notas de mesmo número com livro ou chave diferentes.
- **Valores:** `try_cast` para `DECIMAL(15,2)` (compatível com o ANSI do Spark 4);
  inválido vai para a quarentena com `valor_invalido`.
- **Janela:** a silver e a gold gravam com `replaceWhere` sobre os pares
  `(ano, mes)` presentes no lote; reprocessar 3 meses reescreve 3 partições e
  deixa as outras intactas.

## Ambiente Python (uv)

O contêiner é o ambiente de **execução** (jobs, testes, mutação); o ambiente uv
local serve ao editor e a experimentos.

- **Python 3.12** (`requires-python = ">=3.12,<3.13"`): o PySpark 4.0 suporta até
  o 3.13, e o contêiner usa 3.12.
- **Dependências:** `pyspark==4.0.1`, `delta-spark==4.0.0`; dev: `pytest`,
  `jupyterlab`; opcional: `pandas`, `pyarrow`.
- **Fora do lock, de propósito:** `sqlalchemy` e o driver do banco, usados só
  pelos exportadores, fora do contêiner.
- O Dockerfile instala pelo `uv.lock` (`uv sync --frozen` em `/opt/venv`, fora do
  bind mount de `/work`).

## Testes

Unitários com Spark local (`local[2]`) e DataFrames pequenos; ponta a ponta sobre
a amostra sintética (reconciliação completa); prova por mutação em
`scripts/testar_mutacoes.py`. Sobre o dado real, a reconciliação com a produção
roda no job e vai para o relatório.

## Comandos

Ver `README.md`.
