# PLHDWA · motor de carga Netezza → Databricks (Databricks Asset Bundle)

O Netezza grava um `.dat` e depois um `.CTL` direto no bucket S3 (ou num volume do Unity Catalog, variável `landing_tipo`). A chegada do CTL dispara o **despachante**:
cada execução dele é um **lote**. Cada arquivo é validado, carregado num único commit Delta na tabela destino
(REPLACE WHERE na FATO, substituição total na DIM) e registra na **controle do Db2** o fim:
a conclusão (`CONCLUIDO`, com a quantidade) ou o erro (`ERRO`, com a mensagem). O início fica no log do Databricks
(com `db2_registrar_inicio = true`, o Db2 recebe também o início, `EM_PROCESSAMENTO`).
Tudo é serverless, parametrizado por variáveis do bundle e leva a tag `OWNER`.

Documentação funcional: `documentacao_carga_netezza_databricks.html` · Operação: `manual_operacao_plhdwa.html`.

---

## 1. O que tem no bundle

| Caminho | O que é |
|---|---|
| `databricks.yml` | **Todas as variáveis** (com descrição) e os targets `dev`, `hml`, `prd` para preencher |
| `resources/jobs.yml` | Jobs: despachante (gatilho de chegada de arquivo), cargas independentes (Performance e Standard), vigia, instalação/pós-deploy, testar controle Db2, testes |
| `resources/dashboards.yml` | Dashboards **PLHDWA · Execução** e **PLHDWA · Custo** |
| `src/plhdwa_motor.py` | Biblioteca do motor. A tabela `PARAMETROS` no topo descreve cada parâmetro |
| `src/despachante.py` | Lote: vaga por arquivo, preenchida na hora; modo por objeto (DESPACHANTE/JOB); cadeia mais longa primeiro |
| `src/carga_arquivo.py` | Job independente: começa no arquivo recebido e segue com os próximos do mesmo objeto |
| `src/vigia.py` | Religa o despachante se houver CTL parado (ex.: depois de um DESBLOQUEIO) |
| `src/pos_deploy.py` | Instalação idempotente: tabelas/visões, ids dos jobs, GRANTs |
| `src/testar_controle_db2.py` | Testa conexão, tabela e comando da controle no Db2 |
| `sql/01_tabelas_operacionais.sql` | Tabelas e visões operacionais (executado pelo pós-deploy, com as variáveis) |
| `sql/02_db2_controle_ddl.sql` | DDL da tabela da controle no Db2 (para o DBA) |
| `sql/02_db2_controle_merge.sql` | Comando que grava a controle (editável pelo DBA; 14 parâmetros) |
| `sql/02_db2_controle_merge_tabela_controle.sql` | Variante para uma tabela no layout `CONTROLE` genérico |
| `dashboards/` | Consultas (`datasets.sql`) e gerador dos `.lvdash.json` |
| `ferramentas/` | Cadastro de objetos: DDL do Netezza → tabela Delta com metadados; execução de SQL; cadastro em lote |
| `testes/` | Gerador de arquivos de teste (usado pelo job de teste de tempo) |

## 2. Pré-requisitos (uma vez por ambiente)

1. **Databricks CLI** ≥ 0.250 autenticada no workspace (`databricks auth login --host <workspace>`).
2. **Catálogo** do Unity Catalog para as tabelas operacionais (variável `catalogo`). O schema é criado pelo pós-deploy.
3. **Landing** (variáveis `landing_tipo` e `landing`), com as subpastas `raw/data`, `raw/ctl` e `error`:
   - **`landing_tipo = S3` (padrão): direto no bucket** em que o Netezza grava, `landing = s3://<bucket>/<prefixo>`.
     Não precisa de volume, mas no serverless todo acesso a S3 passa pelo Unity Catalog, então é preciso uma
     **External Location** cobrindo o caminho:
     1. role IAM com `s3:GetObject`, `PutObject`, `DeleteObject` e `ListBucket` no bucket/prefixo (o motor apaga os
        concluídos e move os com erro para `error/`);
     2. *storage credential* com essa role;
     3. external location com **file events** habilitado (o gatilho do despachante fica rápido e escalável):
        ```sql
        CREATE EXTERNAL LOCATION netezza_landing URL 's3://<bucket>/<prefixo>' WITH (STORAGE CREDENTIAL <credencial>);
        GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION netezza_landing TO `<identidade_dos_jobs>`;
        ```
     O caminho não pode se sobrepor a tabelas/volumes do Unity Catalog (use um prefixo só para a landing).
   - **`landing_tipo = VOLUME`**: `landing = /Volumes/<catalogo>/<schema>/<volume>/<pasta>` (volume gerenciado ou
     externo; `READ VOLUME` e `WRITE VOLUME` para a identidade dos jobs).
   - O motor confere se `landing` combina com `landing_tipo` e para com mensagem clara se não combinar. O resto do
     código é o mesmo nos dois casos (`dbutils.fs` e `spark.read`).
   - O Netezza grava o `.dat` completo e **por último** o `.CTL` (o CTL é o sinal de que o par está pronto).
4. **SQL warehouse** para os dashboards (variável `warehouse_id`).
5. **Rede até o Db2**: os jobs serverless chegam no Db2 pela NCC (Network Connectivity Configuration) do
   workspace. Libere host e porta do Db2 para essa rota.
6. **Usuário do Db2** num secret scope (nunca em variável ou código):
   ```bash
   databricks secrets create-scope plhdwa
   databricks secrets put-secret plhdwa db2_usuario --string-value '<usuario>'
   databricks secrets put-secret plhdwa db2_senha   --string-value '<senha>'
   # o usuário/SP que roda os jobs precisa de READ no scope
   databricks secrets put-acl plhdwa <usuario_ou_grupo_dos_jobs> READ
   ```
7. **Pacotes Python**: os notebooks instalam `databricks-sdk`, `pg8000` e `ibm_db` do PyPI. Se o serverless
   do cliente não tiver saída para a internet, configure um repositório PyPI interno (mirror) para o workspace.
8. **Tabela da controle no Db2**: o DBA cria com `sql/02_db2_controle_ddl.sql` (trocando `{tabela}`), ou o
   job de teste cria com `criar_tabela = true`. Se a controle já existe com outro layout, ajuste o comando
   (seção 6).

## 3. Instalar

1. Preencha o target em `databricks.yml` (host e valores `<...>`). Variáveis sem padrão são obrigatórias.
2. Rode:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy   -t dev
   databricks bundle run plhdwa_pos_deploy          -t dev   # tabelas, visões, ids dos jobs, GRANTs
   databricks bundle run plhdwa_testar_controle_db2 -t dev   # conexão + tabela + comando do Db2
   ```
3. Publique os dashboards (pela tela ou API) e dê `CAN_VIEW` ao time de operação.
4. Cadastre os objetos (seção 5) e faça um teste com um arquivo real (seção 7).

O pós-deploy é idempotente: rode depois de **todo** deploy.

## 4. Variáveis (resumo; a descrição completa está em `databricks.yml`)

| Grupo | Variáveis |
|---|---|
| Onde ficam as coisas | `catalogo`, `schema`, `catalogos_objetos` (catálogos das tabelas destino), `landing_tipo`, `landing`, `warehouse_id`, `fuso_horario` |
| Controle (Db2) | `db2_modo` (`DB2` · `SIMULADO` só para testes), `db2_host`, `db2_porta`, `db2_banco`, `db2_ssl`, `db2_certificado`, `db2_extra`, `db2_secret_scope`, `db2_chave_usuario`, `db2_chave_senha`, `db2_tabela`, `db2_comando`, `db2_tentativas`, `db2_espera_base_s` |
| Execução | `max_simult_despachante`, `max_simult_job` (vagas iniciais), `ciclo_s`, `max_horas`, `max_threads`, `job_max_arquivos`, `job_max_minutos`, `precheck_linhas`, `precheck_min_mb`, `gatilho_intervalo_s`, `vigia_cron`, `vigia_parado_min` |
| Governança | `owner_tag`, `grupo_owner`, `email_alertas` |
| Só no modo SIMULADO | `lakebase_instance`, `lakebase_database`, `lakebase_tabela` |

Ajustes **sem deploy**, feitos pelo owner:
- vagas por modo: `UPDATE <catalogo>.<schema>.ctl_execucao SET max_simult = 24 WHERE modo = 'DESPACHANTE'`;
- modo de um objeto: `ALTER TABLE <tabela_destino> SET TAGS ('plhdwa_execucao' = 'JOB', 'plhdwa_tipo_job' = 'STANDARD')`.

## 5. Cadastrar objetos (sem tabela de cadastro)

Os metadados de cada objeto ficam na **própria tabela destino**: o schema é o layout do arquivo, a tag
`plhdwa_cod_objeto` é o código, as tags `plhdwa_execucao`/`plhdwa_tipo_job` dizem onde roda e as
`TBLPROPERTIES plhdwa.*` trazem tipo de carga e formato do arquivo, lidos da DDL do Netezza.

```bash
cd ferramentas
# 1 objeto
python3 converter_ddl_netezza.py exemplos/ddl_external_dwa_ccr_fatparcela.sql --catalogo <cat> --schema <sch> \
        --tabela fat_parcela --cod-objeto 1230 --tipo-carga FATO --col-data-mov SK_TMO_MVM --col-instituicao SK_INS \
        > cadastro/1230_fat_parcela.sql
python3 executar_sql.py cadastro/1230_fat_parcela.sql --warehouse <warehouse_id>
# vários objetos (csv: cod;ddl;tabela;tipo;col_data;col_inst;execucao;tipo_job)
./cadastrar_em_lote.sh exemplos/objetos.csv <pasta_das_ddls> <cat> <sch> [warehouse_id]
```
O catálogo das tabelas destino precisa estar em `catalogos_objetos` (ou ser o mesmo de `catalogo`).

## 6. Controle no Db2

- Driver: **`ibm_db`** (Python, oficial da IBM), instalado pelos notebooks. As threads do despachante
  compartilham um pool de no máximo `db2_max_conexoes` conexões (padrão 8); cada job independente usa 1.
  Total no Db2 = pool + jobs independentes em execução: respeite o limite de conexões do Db2 do cliente.
  Medido no teste: conexão em ~0,8 s, ~0,15 s por gravação; 22 objetos em paralelo com 8 conexões, 0 erros.
- O comando está em `sql/<db2_comando>`: um `MERGE` com 14 parâmetros `?`, em ordem fixa, descrita no arquivo.
  O DBA pode trocar nomes de tabela/colunas e a regra de status sem mexer no Python.
  - `02_db2_controle_merge.sql` (padrão): 1 linha por arquivo, chave objeto + data de movimento + data de geração.
  - `02_db2_controle_merge_tabela_controle.sql`: tabela `CONTROLE` (`ID_CONTROLE`, `NOME_JOB`, `STATUS` em
    INICIADO/EXECUTANDO/CONCLUIDO/ERRO, `DT_INICIO`, `DT_FIM`, `MENSAGEM_ERRO`).
- **O que vai para o Db2** (variável `db2_registrar_inicio`):
  - `false` (padrão): só o fim, `CONCLUIDO` ou `ERRO`. A carga não depende do Db2 para começar. Se o Db2 estiver
    fora, o arquivo é carregado e só a conclusão falha (`ERRO_DB2`, objeto travado): os arquivos ficam em `raw/` e,
    após o DESBLOQUEIO, a retomada refaz só a conclusão, sem carregar de novo.
  - `true`: grava também `EM_PROCESSAMENTO` antes da carga; sem ele gravado, o arquivo **não é carregado**
    (`ERRO_DB2`): com o Db2 fora, nada entra no Delta.
- Certificado: Db2 com certificado de CA própria → coloque a CA num volume e informe em `db2_certificado`.
- Horários (`DT_INICIO`, `DT_FIM`) vão no fuso de `fuso_horario`.
- **Driver e arquitetura:** o driver do Db2 entra como dependência `ibm-db` do environment `plhdwa`
  (`resources/jobs.yml`), instalado antes de o notebook começar. Ele não é Python puro (traz o driver CLI nativo
  da IBM) e só tem pacote para **x86_64**: em máquina ARM (aarch64) a instalação falha e o job termina antes de
  processar ("Unexpected error during library installation"). Nas versões mais novas do ambiente serverless a
  plataforma às vezes aloca ARM (teste com a versão 6: 3 de 3 jobs independentes falharam); na versão 1, todas
  as execuções medidas rodaram em x86_64 (2 lotes de 14 arquivos em job independente, 14/14 nos dois). Por isso
  a variável `serverless_ambiente` fica em `"1"`. O job `plhdwa_testar_controle_db2` imprime a máquina
  (`x86_64` esperado). Se um job falhar assim, o objeto fica travado (`ERRO_EXECUCAO`) e o DESBLOQUEIO reprocessa.

## 7. Testar

- `plhdwa_testar_controle_db2`: grava 2 arquivos fictícios (objeto 0) com o comando real e apaga no fim.
- Arquivo real: copie um par DAT + CTL para `raw/data` e depois `raw/ctl` (o CTL dispara). No S3:
  `aws s3 cp` (ou o próprio Netezza); `plhdwa_gerar_teste_tempo` também grava no S3 com `landing_tipo = S3`. Esperado: carga na
  tabela destino, `CONCLUIDO` em `v_status_arquivo`, linha `CONCLUIDO` na controle do Db2, arquivos apagados.
- `plhdwa_validar_testes`: compara um manifesto (resultado esperado por arquivo) com o log e com a controle.

## 8. Notas de implementação

- **Vaga por arquivo:** cada thread processa 1 arquivo e devolve a vaga; o despachante preenche na hora.
  No máximo 1 arquivo por objeto em andamento (ordem de geração), em qualquer modo.
- **Prioridade:** entre os objetos que podem andar, vai primeiro o que tem mais trabalho pendente.
- **Job independente** segue com o mesmo objeto (até `job_max_arquivos`/`job_max_minutos`): o arranque é
  pago uma vez por objeto.
- **Carga** sempre com `replaceWhere` (DIM usa `1 = 1`), para gravar só os dados e preservar tags,
  propriedades e grants da tabela.
- **DAT vazio** (0 bytes, só header ou em branco) = `ERRO_DAT_VAZIO`, antes da carga.
- **Sem nova tentativa automática** nos jobs: erro = objeto travado + e-mail; outros objetos seguem.
