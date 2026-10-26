# Changelog · PLHDWA carga Netezza → Databricks

Versão no formato `1.NNN`: cada modificação publicada sobe 1 no número (1.000, 1.001, 1.002, …). A versão atual fica em
`plhdwa_bundle/VERSAO` (vai junto no deploy do bundle) e cada publicação no git ganha a tag `v<versão>`.

## [1.002] · 26/10/2026

- Dashboard **PLHDWA · Execução**: página nova **Vazão** (decisão 40), por hora de conclusão, 30 dias:
  - cartões: arquivos, GB, registros, execuções (`job_run_id`: despachante ou job independente), GB/min médio por arquivo
    e espera média;
  - gráficos por hora, com cor por modo: arquivos, GB, registros e execuções; GB/min ao longo do tempo; espera
    (roteado → iniciado) × carga;
  - tabela "Resumo por hora, modo e grupo"; filtros de período, modo e grupo (valem para todos os widgets da página).
- Datasets novos em `dashboards/datasets.sql`: `vazao` (1 linha por arquivo concluído) e `vazao_hora`; `gerar_dashboards.py`
  ganhou `filtro_varios` (um filtro para vários datasets, inclusive o de período).
- Sem mudança de SQL nas tabelas nem no motor: basta o deploy (o bundle publica o dashboard).

## [1.001] · 26/10/2026

- Modo de limpeza `SINGLE` (`src/limpeza_single.py`): o header sai do DAT **antes** de gravar as partes do arquivo limpo
  e as partes são lidas com `header=false`. Antes, o `header=True` do código do cliente valia para cada parte e
  descartava a 1ª linha de cada uma (1 registro perdido por parte; no DAT de 37 GB, ~262).
  - `limpar(spark, origem, destino, header=None)`: novo parâmetro `header` (a 1ª linha do DAT; `None` = DAT sem header).
    O motor passa o header quando o objeto tem `header = true`.
  - `opcoes_leitura`: `header = 'false'`.
  - Continua como antes: registro em várias linhas cortado na fronteira entre partes, `PERMISSIVE` (sem erro em
    defeito), só UTF-8 e `\|` apagado.
- Testes no interno (matriz de modos): o `SINGLE` leu todos os registros no caso do cliente (1.171.242; o `CLIENTE` para),
  no apóstrofo e no `ANDRADE'` (1.171.558, idêntico ao `CLIENTE`); 7 registros a mais que o código literal do cliente em
  cada arquivo; os 5 modos antigos idênticos ao de antes em 30/30 células.
- Docs: decisão 39 da especificação, manual, README, doc técnica e documento de objetos travados.
- Sem mudança de SQL: basta o deploy (o pós-deploy não é necessário).

## [1.000] · 26/10/2026

Versão base do changelog: o estado publicado no commit `338895b`. Principais entregas até aqui (detalhes no histórico do
git e nas decisões da especificação):

- Motor de carga (despachante, vigia, job independente), landing em S3 ou volume, parâmetros em `ctl_parametros`.
- CTL no formato `Cd_obj|cd_inst|nr_cpr|dt_mov`, `REPLACE WHERE` por data × `nr_cpr`, controle no Db2 só com `UPDATE`.
- Limpeza do DAT: modos `CLIENTE` (padrão, estável com posições gravadas), `AUTO`, `PARALELA`, `DIRETA`, `MULTILINHA` e
  `SINGLE` (código do cliente).
- Encoding padrão UTF-8; DAT só com o cabeçalho é sucesso (substitui por vazio); erro do job lista os objetos travados.
- Contagem de aspas do CSV, linha/byte/texto no `ERRO_DADOS` de texto aberto e notebook `diagnosticar_aspas`.
