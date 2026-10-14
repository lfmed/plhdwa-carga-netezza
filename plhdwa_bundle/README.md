# PLHDWA · motor de carga Netezza → Databricks (Databricks Asset Bundle)

O Netezza grava um `.dat` e depois um `.CTL` direto no bucket S3 (ou num volume do Unity Catalog, variável `landing_tipo`). A chegada do CTL dispara o **despachante**:
cada execução dele é um **lote**. Cada arquivo é validado, carregado num único commit Delta na tabela destino
(REPLACE WHERE com `filtrotabela = SIM`, substituição total com `NAO`) e atualiza a **controle do Db2** no fim,
só com `UPDATE` (comandos oficiais do cliente): carga OK (`CONCLUIDO`) ou erro (`ERRO`, com a mensagem). O início fica
no log do Databricks.
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
| `src/pos_deploy.py` | Instalação idempotente: tabelas/visões (inclusive a `ctl_parametros`), ids dos jobs, GRANTs, conferência do cadastro |
| `src/testar_controle_db2.py` | Testa conexão, tabela e os comandos da controle no Db2 (PREPARE, sem gravar) |
| `sql/01_tabelas_operacionais.sql` | Tabelas e visões operacionais (executado pelo pós-deploy, com as variáveis) |
| `sql/02_db2_controle_update.sql` | Comandos oficiais da controle (só `UPDATE`, um por status; editável pelo DBA) |
| `sql/02_db2_controle_update_tabela_controle.sql` | Variante para uma tabela no layout `CONTROLE` genérico (testes) |
| `dashboards/` | Consultas (`datasets.sql`) e gerador dos `.lvdash.json` |
| `ferramentas/` | Cadastro de objetos: DDL do Netezza → tabela Delta + linha na `ctl_parametros`; execução de SQL; cadastro em lote |
| `testes/` | Gerador de arquivos de teste (usado pelo job de teste de tempo) |

## 2. Pré-requisitos (uma vez por ambiente)

1. **Databricks CLI** ≥ 0.250 autenticada no workspace (`databricks auth login --host <workspace>`).
2. **Catálogo** do Unity Catalog para as tabelas operacionais (variável `catalogo`). O schema é criado pelo pós-deploy.
3. **Landing** (variáveis `landing_tipo` e `landing`). Dentro do prefixo ficam as pastas, com os nomes nas
   variáveis `pasta_dat`, `pasta_ctl`, `pasta_erro` e `pasta_tmp` (padrão do cliente):
   ```
   s3://<bucket>/<prefixo>/          ← landing
   ├── dados/   ← pasta_dat:  os .dat (o Netezza grava primeiro)
   ├── ctl/     ← pasta_ctl:  os .CTL (o Netezza grava por último; o gatilho do despachante olha só aqui)
   ├── erro/    ← pasta_erro: o motor move para cá o par (CTL + DAT) com erro de negócio
   └── tmp/     ← pasta_tmp:  transformação temporária do cliente (o motor não lê nem grava aqui)
   ```
   Os nomes podem ser trocados por ambiente (ex.: `pasta_ctl: entrada/ctl`); o motor confere que são diferentes.
   - **`landing_tipo = S3` (padrão): direto no bucket** em que o Netezza grava, `landing = s3://<bucket>/<prefixo>`.
     Não precisa de volume, mas no serverless todo acesso a S3 passa pelo Unity Catalog, então é preciso uma
     **External Location** cobrindo o caminho:
     1. role IAM com `s3:GetObject`, `PutObject`, `DeleteObject` e `ListBucket` no bucket/prefixo (o motor apaga os
        concluídos e move os com erro para `erro/`);
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
7. **Pacotes Python**: o environment dos jobs instala `databricks-sdk`, `pg8000` e `ibm-db` do PyPI. Se o serverless
   do cliente não tiver saída para a internet, configure um repositório PyPI interno (mirror) para o workspace.
8. **Tabela da controle no Db2**: a do cliente (`ctl.OBJETO_DEPENDENCIA_EXECUCAO`). O motor **só faz UPDATE**: a
   linha de cada objeto (`CD_OBJ`) precisa já existir; ele nunca insere nem apaga. O usuário do motor precisa de
   `SELECT` e `UPDATE` nela. Rota de rede (NCC/firewall) do serverless até o host e a porta do Db2.
9. **Service principal da área** (variável `svc_hash` = *application ID* do SP): o bundle fica em
   `/Workspace/Users/<svc_hash>/.bundle/plhdwa/<target>` e todos os jobs rodam como ele (`run_as` nos targets
   dev, hml e prd). Sem padrão: se o CI/CD não preencher (`--var svc_hash=<application id>` ou a variável de
   ambiente `BUNDLE_VAR_svc_hash`), o deploy para com "no value assigned to required variable svc_hash".
   - Quem faz o deploy: o próprio SP (recomendado no CI/CD) ou um usuário com o papel **Service Principal: User**
     nesse SP.
   - O SP precisa de: `USE CATALOG`/`USE SCHEMA`/`CREATE TABLE` no catálogo das tabelas operacionais (e
     `CREATE SCHEMA`, se o schema ainda não existir), `USE CATALOG`/`USE SCHEMA` e `SELECT`/`MODIFY` nas tabelas destino, `READ FILES` e `WRITE FILES` na external location da landing (ou `READ VOLUME`
     e `WRITE VOLUME`), `READ` no secret scope do Db2 e `CAN_USE` no SQL warehouse.
   - Os dashboards não usam o `run_as`: as consultas rodam com a credencial de quem publica (ou de quem vê,
     conforme a publicação).

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
| Onde ficam as coisas | `catalogo`, `schema`, `landing_tipo`, `landing`, `warehouse_id`, `fuso_horario` |
| Controle (Db2) | `usar_db2` (`sim` · `nao` = não executa nenhuma etapa do Db2), `db2_modo` (`DB2` · `SIMULADO` só para testes), `db2_host`, `db2_porta`, `db2_banco`, `db2_ssl`, `db2_certificado`, `db2_extra`, `db2_secret_scope`, `db2_chave_usuario`, `db2_chave_senha`, `db2_tabela`, `db2_comando`, `db2_tentativas`, `db2_espera_base_s`, `db2_timeout_s`, `db2_mensagem_max` |
| Execução | `max_simult_despachante`, `max_simult_job` (vagas iniciais), `ciclo_s`, `max_horas`, `max_threads`, `job_max_arquivos`, `job_max_minutos`, `precheck_linhas`, `precheck_min_mb`, `gatilho_intervalo_s`, `vigia_cron`, `vigia_parado_min`, `vigia_retry_espera_s`, `mover_invalido_min`, `desempenho_vigia` e `desempenho_job_std` (serverless `STANDARD`, padrão, ou `PERFORMANCE_OPTIMIZED`) |
| Identidade | `svc_hash` (application ID do service principal: pasta do bundle e `run_as` dos jobs) |
| Governança | `owner_tag`, `grupo_owner`, `email_alertas` |
| Só no modo SIMULADO | `lakebase_instance`, `lakebase_database`, `lakebase_tabela` |

Ajustes **sem deploy**, feitos pelo owner:
- vagas por modo: `UPDATE <catalogo>.<schema>.ctl_execucao SET max_simult = 24 WHERE modo = 'DESPACHANTE'`;
- parâmetros de um objeto (modo, colunas, formato, limpeza, ativo): `UPDATE` na `ctl_parametros` (seção 5), por exemplo
  `UPDATE <catalogo>.<schema>.ctl_parametros SET execucao = 'JOB', tipo_job = 'STANDARD' WHERE cod_objeto = 1230`.
  Vale em até 1 minuto, sem deploy.

## 5. Cadastrar objetos (tabela `ctl_parametros`)

**Nome dos arquivos:** `<cod>_[<nomegrupo>_]<nome>_<yyyyMMdd>_<yyyyMMddHHmmss>.CTL` (e o `.dat` com o mesmo nome; a
extensão pode ser maiúscula ou minúscula: `.CTL`/`.ctl`, `.DAT`/`.dat`),
por exemplo `57_FINANCEIRO_DIM_BEM_IMOVEL_20260930_20260930155333.CTL`. A carga olha **só o código** (1º campo). O grupo
é o 1º segmento depois do código (sem `_`) e serve só para visualização: coluna `grupo` em `v_status_arquivo`,
`v_arquivo_concluido`, `v_em_execucao`, `v_objeto_travado` e nos dashboards (com filtro). Nome no padrão antigo, sem
grupo, continua valendo; nesse caso o 1º segmento do nome aparece como grupo (nome de um segmento só = grupo vazio).
**Conteúdo do CTL** (4 campos separados por `|`, listas separadas por vírgula, cabeçalho opcional):
```
Cd_obj|cd_inst|nr_cpr|dt_mov
1230|1,2,3|202609|2026-09-01
```
1º campo = código do objeto · 2º = instituições (`0` = todas) · 3º = nr_cpr (`0` = todos) · 4º = data de movimento
(`yyyy-MM-dd`). **O `nr_cpr` filtra o REPLACE WHERE** (objetos com `filtrotabela = SIM`) (`nomecolunatempo = data AND nomecolunainstituicao IN (nr_cpr)`; com
`0`, a data inteira); **o `cd_inst` vai só para a controle (Db2)**. Com `filtrotabela = NAO` o `nr_cpr` é ignorado (a tabela inteira é
substituída). Código e data precisam bater com o nome do arquivo. Dão `ERRO_CTL`: número de campos diferente de 4
(os formatos anteriores, com 3 ou 5 campos ou só com vírgulas, não valem mais), lista vazia, não numérica ou com `0`
junto com outros, data inválida, mais de uma linha de dados. Linha do DAT com `nr_cpr` fora da lista do CTL =
`ERRO_DADOS` (nada é gravado).

Arquivo na pasta de CTL com nome fora desse padrão (ou que não é CTL) vai para `pasta_erro` (com o `.dat` de mesmo
nome, se houver) depois de `mover_invalido_min` minutos na pasta (padrão 5, para não mexer em arquivo sendo gravado) e
gera o evento `NOME_INVALIDO` no log (não trava objeto).

Os parâmetros de cada objeto ficam numa linha da **`<catalogo>.<schema>.ctl_parametros`** (criada pelo pós-deploy).
O motor **não usa tags nem TBLPROPERTIES** (só a tag `OWNER`, de governança). Da tabela destino ele usa apenas o
layout: colunas, ordem e tipos.

| Grupo | Colunas | Vazio (NULL) = |
|---|---|---|
| Chave | `cod_objeto` (código do CTL; 1 linha por código) | obrigatório |
| Destino | `catalogo_destino`, `schema_destino`, `tabela_destino` | obrigatório |
| Carga | `filtrotabela` (`SIM` = REPLACE WHERE pela data de movimento + nr_cpr do CTL · `NAO` = substitui a tabela inteira), `nomecolunatempo` (coluna da data), `nomecolunainstituicao` (coluna comparada com o nr_cpr do CTL) | `SIM` exige `nomecolunatempo`; `nomecolunainstituicao` vazia = `NR_CPR` |
| Execução | `execucao` (`DESPACHANTE` · `JOB`), `tipo_job` (`PERFORMANCE_OPTIMIZED` · `STANDARD`), `ativo` | `DESPACHANTE`, `PERFORMANCE_OPTIMIZED`, `true` |
| Formato do arquivo | `delimitador`, `encoding`, `header`, `escape`, `quote`, `null_value`, `date_format`, `timestamp_format` | `\|`, `ISO-8859-15`, `true`, sem, sem, vazio, `yyyy-MM-dd`, `yyyy-MM-dd HH:mm[:ss]` |
| Limpeza (5b) | `limpeza`, `limpeza_modo`, `limpeza_quote` | variável `limpeza_dat`, `CLIENTE`, `'` |
| Informativo | `tabela_origem` (Netezza), `observacao`, `alterado_por`, `alterado_em` | — |

- **CHECK** na tabela recusam valor inválido no próprio `INSERT`/`UPDATE` (tipo de carga, modo, tipo de job, modo
  de limpeza, `filtrotabela` diferente de `SIM`/`NAO`, `SIM` sem `nomecolunatempo`).
- **Antes de cada carga** o motor confere a linha: código em exatamente 1 linha, tabela destino existente e, com `filtrotabela = SIM`,
  colunas existentes. Se não bater: `ERRO_CADASTRO` com a causa e nada é gravado. O pós-deploy lista as
  inconsistências.
- **`ativo = false`** pausa o objeto: os arquivos ficam na pasta de CTL, sem erro, e são carregados quando ele
  voltar a `true`.
- Mudanças valem em até 1 minuto (cache do motor), sem deploy.

O `converter_ddl_netezza.py` gera, a partir da DDL do Netezza, o `CREATE TABLE` da tabela destino e o `MERGE` da
linha do objeto na `ctl_parametros` (formato do arquivo lido do bloco `USING`):

```bash
cd ferramentas
# 1 objeto
python3 converter_ddl_netezza.py exemplos/ddl_external_dwa_ccr_fatparcela.sql --catalogo <cat> --schema <sch> \
        --tabela fat_parcela --catalogo-controle <catalogo> --schema-controle <schema> \
        --cod-objeto 1230 --filtro-tabela SIM --nome-coluna-tempo SK_TMO_MVM --nome-coluna-instituicao NR_CPR \
        > cadastro/1230_fat_parcela.sql
python3 executar_sql.py cadastro/1230_fat_parcela.sql --warehouse <warehouse_id>
# vários objetos (csv: cod;ddl;tabela;filtrotabela;nomecolunatempo;nomecolunainstituicao;execucao;tipo_job)
./cadastrar_em_lote.sh exemplos/objetos.csv <pasta_das_ddls> <cat> <sch> <catalogo> <schema> [warehouse_id]
```
`<catalogo>`/`<schema>` = variáveis `catalogo` e `schema` do bundle (onde está a `ctl_parametros`).

Conferir o cadastro:
```sql
SELECT cod_objeto, concat_ws('.', catalogo_destino, schema_destino, tabela_destino) AS destino, filtrotabela,
       nomecolunatempo, nomecolunainstituicao, execucao, tipo_job, ativo
FROM <catalogo>.<schema>.ctl_parametros ORDER BY cod_objeto;
```

**Clustering das tabelas com `filtrotabela = SIM`.** O REPLACE WHERE filtra por `nomecolunatempo` e
`nomecolunainstituicao`; as tabelas grandes devem ser clusterizadas por essas duas colunas (o conversor já gera assim).
O liquid clustering exige estatísticas nas colunas do `CLUSTER BY`, e o Delta só as coleta nas 32 primeiras colunas:
se a coluna do nr_cpr estiver depois da 32ª (ex.: `NR_CPR` é a 57ª do FATPARCELA), inclua-a em
`delta.dataSkippingStatsColumns` antes. Em uma tabela que já existe:
```sql
ALTER TABLE <destino> SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '<as 32 primeiras colunas>,<nomecolunainstituicao>');
ALTER TABLE <destino> CLUSTER BY (<nomecolunatempo>, <nomecolunainstituicao>);
ANALYZE TABLE <destino> COMPUTE DELTA STATISTICS;   -- recalcula as estatísticas dos arquivos já gravados
OPTIMIZE <destino>;                                  -- reorganiza os dados (fora da janela de carga)
```
Sem a primeira linha, o `CLUSTER BY` falha com `DELTA_CLUSTERING_COLUMN_MISSING_STATS`.

## 5b. Limpeza do DAT antes da carga (pedido do cliente)

Com `limpeza_dat = true` (padrão), cada DAT passa por esta etapa antes da carga (`preparar_dat` no motor):

1. lido linha a linha **com o encoding do objeto** (coluna `encoding` da `ctl_parametros`, LATIN9 do Netezza);
2. as 3 regex do cliente, nesta ordem (as mesmas do código recebido, em `_RX_LIMPEZA`):
   `r'\\(?![\\\'"|nrt])'` → vazio (barra que não escapa nada sai) · `r'\\\|'` → `|` · `r'\\null'` → vazio (NULL);
3. gravado em `<landing>/<pasta_tmp>/<nome do arquivo>/` e lido para a carga com header, delimitador da tabela,
   `multiLine`, aspas simples (`'`) e escape `\` (padrão do Spark), com o schema da tabela e `FAILFAST`;
4. o arquivo limpo é **apagado no fim**, com sucesso ou com erro. Se uma execução cair no meio, o próximo lote apaga
   a sobra (só pastas com nome de arquivo do motor; o resto da `tmp/` não é tocado).

Ajustes em relação ao código recebido (medidos no teste com 6 milhões de registros, 13 partes):

| Ajuste | Sem o ajuste |
|---|---|
| Arquivo limpo gravado **em 1 arquivo, na ordem original** (`coalesce(1)`) | o CSV com `header=True` descartou a 1ª linha de cada parte: **12 registros perdidos**; registro com quebra de linha pode ser cortado entre partes |
| Leitura com o **encoding da tabela** (CSV de uma coluna) | `spark.read.text` só lê UTF-8: **todos os acentos LATIN9 corrompidos** (852.857 registros) |
| `wholeFile` removido | opção inexistente no leitor de texto (era ignorada) |
| Schema da tabela + `FAILFAST` + conferência do header | tudo como texto e linha inválida descartada sem erro |

Modos (coluna `limpeza_modo` da `ctl_parametros`, também pelo `converter_ddl_netezza.py --limpeza-modo`):

| Modo | Como funciona | Quando usar |
|---|---|---|
| **`CLIENTE`** (padrão) | a abordagem do cliente: `tmp/` em **várias partes gravadas em paralelo**, lidas com `multiLine` em paralelo. Ajustes: header sai antes de gravar; registros com quebra de linha cortados na fronteira entre partes são achados numa passada em paralelo e gravados inteiros numa parte a mais | sempre |
| `AUTO` | conta as linhas com aspa aberta: nenhuma → `PARALELA`; alguma → `CLIENTE` | alternativa |
| `PARALELA` | `tmp/` em partes lidas sem `multiLine` | objeto que nunca tem registro em mais de uma linha |
| `DIRETA` | regex e colunas na própria leitura (`from_csv`), sem `tmp` | alternativa sem gravar `tmp` |
| `MULTILINHA` | `tmp/` em 1 arquivo (`coalesce(1)`) + `multiLine`, tarefa única | **não usar em arquivo grande**: no teste de 80 GB a ordem das linhas não se manteve |

Comparativo (ponta a ponta pelo motor, landing no S3; detalhes na documentação técnica, seção 3b):

| Arquivo | Sem limpeza | MULTILINHA | **CLIENTE** |
|---|---|---|---|
| 5 GB, 1 arquivo | 65 s | 259 s | **104 s** |
| 8 × 1 GB em paralelo | 96 s | 130 s | **148 s** |
| 3 GB, 50% dos registros em mais de uma linha | — | — | **164 s**, 24 partes, 8 registros da fronteira reunidos, 40.670.000/40.670.000 |
| 80 GB com multilinha (5.633.850 registros em mais de uma linha) | — | falhou (ordem) | **338 s** (14,2 GB/min), 640 partes, 2 registros da fronteira reunidos, 1.126.770.000/1.126.770.000 |
| 80 GB com + 80 GB sem, liberados juntos | — | — | **457 s** para os 160 GB (364 s e 354 s cada), tudo confere |

Partes fixas (`limpeza_pedaco_mb`, padrão `0` = divisão do Spark) são opcionais: com 4 MB o 5 GB levou 198 s (cerca de
2× mais lento). Com qualquer divisão, antes de gravar o motor confere se as linhas da fronteira e o número de partes são
os esperados; se não, refaz (até 3 vezes) e, persistindo, para sem gravar.

Outras colunas da `ctl_parametros`: `limpeza` (liga/desliga a limpeza só para o objeto) e `limpeza_quote`
(aspas dos textos; padrão `'`). Detalhes da leitura: fim de linha = LF (o CR escapado do Netezza, `\<CR>`, é dado
e fica no valor; CR no fim da linha, de arquivo CRLF, sai) e a regex só roda nas linhas com barra invertida.

Pontos do próprio código do cliente para confirmar (teste funcional, `implantacao/testes/limpeza/`):
- `\"` (aspas duplas escapadas) fica gravado como `\"`: o escape do CSV só vale para a aspa simples.
- Texto vazio entre aspas (`''`) vira NULL, igual a `\null`.
- Modo `MULTILINHA`: tarefa única e sem garantia de ordem em arquivo grande; use `CLIENTE` (padrão).
- Arquivo reenviado com o **mesmo nome** (mesma geração) não é recarregado: o motor faz a retomada (só a conclusão).

## 6. Controle no Db2

- **`usar_db2`** (`sim` padrão · `nao`): com `nao`, o motor não executa nenhuma etapa do Db2 (não conecta, não grava
  CONCLUIDO/ERRO/EM_PROCESSAMENTO e o log não tem `CONFIRMADO_DB2`); a carga, o log no Databricks e o tratamento de
  erro são os mesmos. As variáveis `db2_*` podem ficar vazias, e o job "testar controle Db2" só avisa.
- Driver: **`ibm_db`** (Python, oficial da IBM), instalado pelo environment dos jobs. As threads do despachante
  compartilham um pool de no máximo `db2_max_conexoes` conexões (padrão 8); cada job independente usa 1.
  Total no Db2 = pool + jobs independentes em execução: respeite o limite de conexões do Db2 do cliente.
  Medido no teste: conexão em ~0,8 s, ~0,15 s por gravação; 22 objetos em paralelo com 8 conexões, 0 erros.
- **Só UPDATE.** Os comandos estão em `sql/<db2_comando>` (padrão `02_db2_controle_update.sql`, os comandos
  oficiais do cliente), uma seção por status, cada uma com **um** `UPDATE`:
  ```sql
  -- [CONCLUIDO]
  UPDATE {tabela} SET IC_PRC_EXC = 0, IC_PRC_FNZ = 1, DH_FIM_PRC = CURRENT TIMESTAMP
   WHERE CD_OBJ IN (CAST(:cod_objeto AS INTEGER))
  -- [ERRO]
  UPDATE {tabela} SET IC_PRC_EXC = 0, IC_PRC_FNZ = 0, DH_FIM_PRC = CURRENT TIMESTAMP,
         DS_MNG_ERR_EXC = CAST(:mensagem AS VARCHAR(2000))
   WHERE CD_OBJ IN (CAST(:cod_objeto AS INTEGER))
  ```
  `{tabela}` = `db2_tabela`. Os dados do arquivo entram como marcadores `:nome` (`:cod_objeto`, `:mensagem`,
  `:arquivo`, `:data_movimento`, `:data_geracao`, `:instituicoes`, `:qtd_registros`, `:dt_inicio`, `:dt_fim`, ...;
  lista no arquivo). O motor **recusa** qualquer comando que não seja `UPDATE` (MERGE, INSERT, DELETE) e mais de
  um comando por seção. A mensagem de erro vai com até `db2_mensagem_max` caracteres (padrão 500: ajuste ao
  tamanho de `DS_MNG_ERR_EXC`).
- **Linha não encontrada:** `UPDATE` que atualiza 0 linhas = `ERRO_DB2` no arquivo ("não encontrou a linha do
  objeto ... o motor não insere"), sem novas tentativas; mais de 1 linha é aceito.
- **Tempo-limite (`db2_timeout_s`, padrão 30 s):** a conexão usa `ConnectTimeout` e cada sessão `SET CURRENT LOCK
  TIMEOUT`. Rede bloqueada ou linha da controle presa por outra transação viram erro com a mensagem do Db2, com as
  novas tentativas de `db2_tentativas`, e depois `ERRO_DB2`, em vez de o job ficar esperando sem fim.
- **Log:** cada gravação aparece na saída do job: "controle CONCLUIDO de <arquivo>: gravando no Db2", "ok, N
  linha(s) em X s" ou "falhou em X s: <erro do Db2>".
- **O que vai para o Db2** (variável `db2_registrar_inicio`):
  - `false` (padrão): só o fim, `CONCLUIDO` ou `ERRO`. A carga não depende do Db2 para começar.
    Com `true`, o arquivo de comandos precisa da seção `-- [EM_PROCESSAMENTO]` (os comandos oficiais não têm). Se o Db2 estiver
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

- `plhdwa_testar_controle_db2`: conecta (com o tempo-limite), confere a tabela e faz o `PREPARE` de cada `UPDATE`
  no Db2 (confere colunas e tipos) **sem gravar nada**. Com o parâmetro `objeto_teste` = um código combinado com o
  DBA, executa de verdade o `UPDATE` de CONCLUIDO e depois o de ERRO nesse objeto (a linha fica como ERRO).
- Arquivo real: copie um par DAT + CTL para `dados` e depois `ctl` (o CTL dispara). No S3:
  `aws s3 cp` (ou o próprio Netezza); `plhdwa_gerar_teste_tempo` também grava no S3 com `landing_tipo = S3`. Esperado: carga na
  tabela destino, `CONCLUIDO` em `v_status_arquivo`, linha `CONCLUIDO` na controle do Db2, arquivos apagados.
- `plhdwa_validar_testes`: compara um manifesto (resultado esperado por arquivo) com o log e com a controle.

**O CTL chegou e nada aconteceu?**
- O despachante não rodou: veja a aba **Trigger** do job (caminho vigiado e erro da external location).
- Rodou e não processou: a mensagem do lote diz o que ele viu na pasta e o motivo de cada CTL ignorado (nome fora do
  padrão, por exemplo `.ctl` minúsculo; objeto travado; objeto com `ativo = false`; já concluído; pasta vazia ou
  inexistente):
  `SELECT status, mensagem FROM <catalogo>.<schema>.log_lote ORDER BY evento_ts DESC LIMIT 4`.
- A pasta não pôde ser listada (permissão, external location com problema): despachante e vigia terminam com
  **erro**, e a mensagem do job e do `log_lote` traz o erro real ("não consegui listar ...").

## 8. Notas de implementação

- **Vaga por arquivo:** cada thread processa 1 arquivo e devolve a vaga; o despachante preenche na hora.
  No máximo 1 arquivo por objeto em andamento (ordem de geração), em qualquer modo.
- **Prioridade:** entre os objetos que podem andar, vai primeiro o que tem mais trabalho pendente.
- **Job independente** segue com o mesmo objeto (até `job_max_arquivos`/`job_max_minutos`): o arranque é
  pago uma vez por objeto.
- **Carga** sempre com `replaceWhere` (`filtrotabela = NAO` usa `1 = 1`), para gravar só os dados e preservar a tag `OWNER`,
  as propriedades e os grants da tabela. Destino, colunas do `replaceWhere` e formato vêm da `ctl_parametros`.
- **DAT vazio** (0 bytes, só header ou em branco) = `ERRO_DAT_VAZIO`, antes da carga.
- **Sem nova tentativa automática** nos jobs: erro = objeto travado + e-mail; outros objetos seguem.
