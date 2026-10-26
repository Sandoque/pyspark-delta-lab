# Checkpoints

Cada checkpoint registra o que foi entregue. As execuções foram feitas sobre o
dado real (fora do repositório); aqui os volumes estão arredondados e os
resultados, em contagens de reconciliação, percentuais e tempos.

## Fatia 3 — o Job na Databricks Free Edition (26/10/2026)

Bundle (`databricks.yml`) validado e implantado: schema `workspace.lab`, Volume
`landing` e o Job `fatia1-clientes` com 4 tarefas encadeadas em serverless,
rodando a fatia 1 sobre a amostra sintética.

| Execução | landing | bronze | silver | gold | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1ª (tabelas novas) | 19,8 s | 51,9 s | 44,1 s | 24,2 s | **141,6 s** |
| 2ª (mesmo arquivo) | 17,5 s | 10,6 s | 30,6 s | 19,7 s | **80,4 s** |

- **Reconciliação: 12 de 12** na tabela `workspace.lab.reconciliacao_fatia1`
  (a tarefa gold falharia o Job se algum indicador não batesse).
- **Idempotência no Unity Catalog:** na 2ª execução a bronze reconheceu o lote
  (continua com 185 linhas e 1 lote) e o MERGE da silver inseriu, atualizou,
  apagou e copiou **0 linhas**, sem gravar arquivo (`DESCRIBE HISTORY`: versão 1,
  `MERGE`, todas as métricas 0) — os deletion vectors funcionando igual ao
  contêiner.
- **O mesmo código** do contêiner: o pacote `src/estudo` grava por nome de tabela
  (`estudo/destino.py`), e os notebooks de `databricks/` só leem os parâmetros e
  chamam o pacote.
- **Tempo:** quase todo o tempo é custo fixo de subir o serverless e de criar ou
  abrir as tabelas no catálogo — a amostra tem 185 linhas. A 1ª execução é mais
  lenta porque cria as tabelas; a bronze da 2ª cai de 52 s para 11 s porque só
  confere o lote. Não é medida de desempenho do Spark, e sim da orquestração.

## Fatia 2 — livro fiscal de saída (~1,6 milhão de itens) e janela `replaceWhere`

**Execução:** carga completa (~1,6 milhão de itens, ~480 mil notas, 30
competências) e janela de 3 meses (~120 mil itens, 7,4% do total).

### Reconciliação ao centavo e a perda do `float32`

- **30 de 30 competências, 210 valores, diferença 0,00** entre a gold do Spark em
  `DECIMAL(15,2)` e o SQL de produção (`CAST(ROUND(…, 2) AS DECIMAL(15,2))`), em
  valores e em contagens de itens e notas.
- Em **todas** as 30 competências a soma em `float32` perdeu os centavos (erro de
  até R$ 1,00 por mês no total da nota): totais de alguns milhões de reais não
  cabem com centavos em precisão simples.

### Carga completa × janela de 3 meses

| Métrica | Carga completa (30 meses) | Janela de 3 meses (`replaceWhere`) | Diferença |
| --- | ---: | ---: | ---: |
| Linhas processadas | ~1,6 milhão | ~120 mil | −92,6% |
| Arquivos Parquet gravados na silver | 30 (1 por mês) | **3** (`numRemovedFiles = 3`) | −90% (27 meses intactos) |
| Spark — bronze | 11,7 s | 5,4 s | −54% |
| Spark — silver | 37,8 s | 10,5 s | −72% |
| Spark — gold + reconciliação | 5,6 s | 3,3 s | −41% |
| **Spark — total (sem subir a sessão)** | **55,1 s** | **19,1 s** | **−65%** |
| Integridade dos 30 meses depois da janela | 30/30 | **30/30** | — |

### Como comparar com o SQL sem enganar

- **A agregação:** a gold do Spark levou **5,5 s** sobre a silver em Parquet; o
  `GROUP BY` equivalente no MySQL de produção levou **15,7 s**. É a comparação
  mais justa que este estudo tem: mesmo dado e mesmo resultado. Mas não são os
  mesmos motores nas mesmas condições (Parquet colunar local contra MySQL com
  linhas largas), então o "~2,8×" é um indício, não uma regra.
- **O pipeline inteiro não empata, perde:** o Spark precisa da exportação
  (42,3 s) antes de começar. Exportação + 55,1 s do Spark ≈ **97 s**, contra
  **58 s** de exportação + `GROUP BY` no MySQL. Num cenário real o Spark leria
  direto da fonte ou do lake, e a exportação sairia da conta — o que este estudo
  não mediu.
- **A lição principal não é a ferramenta, é a janela:** reprocessar 3 meses em
  vez de 30 cortou 92,6% das linhas e 65% do tempo. O mesmo padrão (apagar e
  regravar só as competências da janela, numa transação) dá para aplicar no
  MySQL, sem Spark — e é a primeira recomendação.

### Achado de memória: escrita particionada e `OutOfMemoryError`

Na primeira tentativa, `silver.cache()` na heap padrão de 1 GB, com 8 tarefas
escrevendo em 30 partições `(ano, mes)`, abria até 8 × 30 = 240 escritores
Parquet ao mesmo tempo e estourava `java.lang.OutOfMemoryError: Java heap space`.
Resolvido com:

1. `repartition("ano", "mes")` antes do `partitionBy("ano", "mes")`: cada tarefa
   escreve uma competência por vez — 30 arquivos em vez de 240 escritores;
2. ler a silver do Delta recém-gravado em vez de `.cache()`: o Parquet em disco já
   é a materialização;
3. `spark.driver.memory = 4g`.

## Síntese do teste de uso

| Cenário | Volume | SQL de produção | Spark + Delta (local) | Leitura |
| --- | ---: | ---: | ---: | --- |
| Fatia 1 — cadastro | ~30 mil linhas | ~5,7 s | ~30,7 s (+ ~6 s de sessão) | SQL ~5× mais rápido: o custo fixo da JVM domina |
| Fatia 2 — agregação mensal | ~1,6 milhão de itens | 15,7 s (`GROUP BY`) | 5,5 s (gold sobre Parquet) | Indício de ganho em volume, com as ressalvas acima |
| Fatia 2 — janela de 3 meses | ~120 mil itens | — | 19,1 s, 3 de 30 arquivos | O ganho vem de processar só a janela |

**Recomendações:**

1. **Manter em SQL/dbt** o que é pequeno ou médio (cadastros, modelos finais
   agregados): é mais rápido e mais simples de operar.
2. **Aplicar já, sem Spark:** reprocessar o livro fiscal por janela de
   competência, numa transação, e somar valores fiscais sempre em
   `DECIMAL(15,2)`. A redução esperada no consolidado noturno é uma estimativa a
   medir, não um resultado.
3. **Considerar Spark + Delta** só se as tabelas fiscais continuarem crescendo e
   pressionando memória e disco na janela noturna: como motor do processamento
   pesado (bronze → silver → gold particionadas, com `replaceWhere`), publicando
   no banco da aplicação só a gold agregada.

**Onde Spark + Delta é pior:** JVM, contêiner e ~6 s só para subir a sessão;
o banco da aplicação continua sendo relacional; e é preciso olhar as métricas
físicas (`DESCRIBE HISTORY`) — sem isso, um MERGE sem deletion vectors reescreve
a tabela inteira e uma escrita particionada sem `repartition` estoura a memória.

## Fatia 1b — MERGE com deletion vectors, mutação e amostra sintética

### T12 — estratégias de MERGE na silver (8 arquivos Parquet)

| Estratégia | Sem mudança no retrato | 1 alterado + 1 inserido + 1 removido |
| --- | --- | --- |
| MERGE padrão (sem DV) | **8 de 8 arquivos** reescritos (4,07 s) | 8 de 8 arquivos reescritos (2,21 s) |
| **(a) `delta.enableDeletionVectors = true`** | **0 arquivos**: no-op, a versão nem avança (1,83 s) | **2 linhas** gravadas + 1 deletion vector, 0 linhas copiadas (2,43 s) |
| (b) pré-filtro por `_hash_linha`, sem DV | 0 arquivos, no-op (1,14 s) | 1 de 8 arquivos reescrito (2,39 s) |
| (a + b) | 0 arquivos, no-op (1,58 s) | 2 linhas + 1 DV, mas lê o alvo 2× (5,02 s) |

A cláusula `whenNotMatchedBySourceDelete` obriga o MERGE a olhar todos os
arquivos; sem deletion vectors, todo arquivo tocado é reescrito. **Decisão:**
deletion vectors na `silver_clientes` (estratégia a) — resolve a reexecução e a
atualização diária sem código extra.

### T13 — mutação

`scripts/testar_mutacoes.py` sabota uma regra por vez (formato do documento,
dígitos verificadores, `fim8`, vigência do de/para, unicidade do número, chave da
nota fiscal, `float32` no lugar de `DECIMAL`) e confere que algum teste falha:
**10 de 10 mutações mortas**.

### T14 — amostra sintética

`scripts/gerar_amostra_sintetica.py` (`seed=42`) gera os dois arquivos de
`seeds/amostra_sintetica/` com o mesmo formato e as mesmas armadilhas do dado
real (CPF em 15 dígitos, zero à esquerda engolido, duplicata exata, conflito,
quarentena), e com os controles calculados pela mesma regra do SQL.

## Fatia 1 — cadastro de clientes

**Execução** sobre ~30 mil clientes, num retrato do banco de produção.

- **Camadas:** bronze = silver + quarentena + duplicatas, com 0 de quarentena e 0
  duplicatas nesse retrato.
- **Silver:** ~74% CPF e ~26% CNPJ depois da regra do formato do ERP; 0,04% fora
  de qualquer formato; ~0,2% com dígito verificador errado; código único.
- **Reconciliação:** **12 de 12** indicadores (3 bases × 4 indicadores) iguais ao
  SQL de produção.
- **Idempotência:** a 2ª execução reconheceu o lote e repetiu os números.
- **Tempo:** Spark ~30 s contra ~6 s do SQL — ver a síntese acima.
