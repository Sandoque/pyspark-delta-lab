# Constitution — PySpark + Delta Lab

Versão: 0.2\
Data: 26/10/2026

## Para que existe

Duas coisas ao mesmo tempo, e a segunda decide o desenho:

1. **Estudo** de PySpark e Delta Lake sobre problemas reais de um pipeline de
   dados de uma distribuidora (aqui chamada de **revenda XPTO**): cadastro com
   documento em formato fora do padrão, telefone espalhado em campos diferentes,
   regra comercial que muda com data, e um livro fiscal com milhões de linhas
   reprocessado toda noite.
2. **Teste de uso**: medir se Spark + Delta servem a esse pipeline (hoje Python
   + MySQL + dbt) em algum ponto. Um teste de uso só vale se o resultado **bater
   com o de produção** e se o ganho (ou a perda) for **medido**, não suposto.

## Regras

- **Fonte só leitura.** O único acesso ao banco é `SELECT` numa transação
  `READ ONLY`, pelos scripts de exportação, com um usuário só de leitura.
- **Dado real não sai da máquina.** Este repositório público traz só código,
  testes e **amostras sintéticas** geradas por `scripts/gerar_amostra_sintetica.py`.
  As execuções reais ficam em `data/` e `reports/`, fora do git; os números
  publicados aqui são arredondados ou relativos.
- **Nenhum segredo no código.** A conexão vem da variável de ambiente
  `ESTUDO_DW_URL`.
- **Reconciliar sempre.** Toda camada fecha a conta com a anterior
  (`entrada = saída + quarentena + duplicatas`), e o resultado final é comparado
  com o número que a produção calcula pelo SQL, no mesmo retrato do dado.
- **Quarentena, não descarte.** Linha que não passa na validação vai para a
  quarentena com o motivo; nada some em silêncio.
- **Idempotência.** Rodar a mesma fatia duas vezes sobre o mesmo arquivo não
  muda o resultado.
- **Medir antes de afirmar ganho**, e comparar só o que é comparável.

## Método (SDD)

`spec.md` (o quê e o critério de aceite) → `plan.md` (como) → `tasks.md`
(tarefas com evidência) → `checkpoints.md` (o que foi entregue, com números).
Mudança de escopo entra primeiro na spec.
