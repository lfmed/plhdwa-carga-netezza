# PLHDWA · carga Netezza → Databricks

Motor de carga dos arquivos exportados pelo Netezza (`.dat` + `.CTL`) para tabelas Delta no Databricks, com a
controle de cada arquivo (início, erro, conclusão) gravada no Db2. Entregue como **Databricks Asset Bundle**.

| Pasta | Conteúdo |
|---|---|
| [`plhdwa_bundle/`](plhdwa_bundle/) | O bundle: jobs, biblioteca do motor, SQL de instalação, comando da controle no Db2, dashboards e ferramentas de cadastro. **Comece pelo [`plhdwa_bundle/README.md`](plhdwa_bundle/README.md)** (pré-requisitos, variáveis, instalação, cadastro, testes). |
| [`docs/`](docs/) | Especificação (`documentacao_carga_netezza_databricks.html`) e manual de operação (`manual_operacao_plhdwa.html`), com diagramas. Baixe e abra no navegador. |

Instalação, em resumo:

```bash
cd plhdwa_bundle
# 1) secret scope com usuário/senha do Db2 e demais pré-requisitos (README do bundle, seção 2)
# 2) preencher o target (dev / hml / prd) em databricks.yml
databricks bundle deploy -t dev
databricks bundle run plhdwa_pos_deploy -t dev
databricks bundle run plhdwa_testar_controle_db2 -t dev
```

Configurações de um ambiente específico podem ficar em `plhdwa_bundle/targets/<ambiente>.yml` (não versionado, ver `.gitignore`).
