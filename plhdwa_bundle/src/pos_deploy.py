# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Instalação e pós-deploy (rodar após cada `databricks bundle deploy`)
# MAGIC Pode rodar quantas vezes quiser (tudo é idempotente):
# MAGIC 1. Cria/atualiza as tabelas e visões operacionais a partir de `sql/01_tabelas_operacionais.sql`,
# MAGIC    trocando `${catalogo}`, `${schema}`, `${owner_tag}` e as vagas iniciais pelas variáveis do bundle.
# MAGIC 2. Grava em `ctl_execucao` os jobs independentes de cada tipo (Performance Optimized e Standard).
# MAGIC 3. Só com `db2_modo = SIMULADO` (testes sem Db2): cria o banco e a tabela da controle no Lakebase.
# MAGIC    Com `db2_modo = DB2` não usa Lakebase; a conexão com o Db2 é testada pelo job "testar controle Db2".
# MAGIC 4. Dá ao owner (`grupo_owner`) escrita em `ctl_execucao` e leitura nas tabelas operacionais.

# COMMAND ----------

# Dependências (ibm_db, databricks-sdk, pg8000): vêm do environment do job (resources/jobs.yml, environments ->
# plhdwa) e são instaladas antes de o notebook começar. A versão do ambiente fica fixa (variável
# serverless_ambiente) porque o driver ibm_db só existe para x86_64 (ver README, seção do Db2).

# COMMAND ----------

import re
import ssl
import uuid

import pg8000.dbapi
from databricks.sdk import WorkspaceClient

# Parâmetros (vêm de resources/jobs.yml -> variáveis do bundle)
NOMES = ["catalogo", "schema", "owner_tag", "max_simult_despachante", "max_simult_job", "job_perf", "job_std",
         "db2_modo", "lakebase_instance", "lakebase_database", "lakebase_tabela", "grupo_owner"]
for nome in NOMES:
    dbutils.widgets.text(nome, "")
P = {k: dbutils.widgets.get(k).strip() for k in NOMES}
fq = lambda t: f"{P['catalogo']}.{P['schema']}.{t}"

# Os nomes entram em SQL montado por texto: só aceitamos identificadores simples.
for k in ["catalogo", "schema", "lakebase_database", "lakebase_tabela"]:
    if P[k] and not re.fullmatch(r"[A-Za-z0-9_.]+", P[k]):
        raise ValueError(f"parâmetro {k} inválido: {P[k]!r}")
for k in ["max_simult_despachante", "max_simult_job", "job_perf", "job_std"]:
    if not P[k].isdigit():
        raise ValueError(f"parâmetro {k} precisa ser um número: {P[k]!r}")

# COMMAND ----------

# 1) Tabelas e visões operacionais (sql/01_tabelas_operacionais.sql, com os marcadores trocados)
texto = open("../sql/01_tabelas_operacionais.sql", encoding="utf-8").read()
for k in ["catalogo", "schema", "owner_tag", "max_simult_despachante", "max_simult_job"]:
    texto = texto.replace("${" + k + "}", P[k])
sobrou = set(re.findall(r"\$\{(\w+)\}", texto))
if sobrou:
    raise ValueError(f"marcadores sem valor no SQL: {sobrou}")

# Um comando por ";" no fim da linha. Linhas só de comentário são descartadas.
comandos = [c.strip() for c in re.split(r";\s*\n", texto)]
comandos = ["\n".join(l for l in c.splitlines() if not l.strip().startswith("--")).strip() for c in comandos]
for c in [c for c in comandos if c]:
    print(c.splitlines()[0][:120])
    spark.sql(c)
print(f"{len([c for c in comandos if c])} comandos executados em {P['catalogo']}.{P['schema']}")

# COMMAND ----------

# 2) Jobs independentes de cada tipo em ctl_execucao (o despachante usa estes ids no run_now)
spark.sql(f"""UPDATE {fq('ctl_execucao')} SET job_id_perf = {int(P['job_perf'])}, job_id_std = {int(P['job_std'])},
              alterado_por = current_user(), alterado_em = current_timestamp() WHERE modo = 'JOB'""")
display(spark.table(fq("ctl_execucao")))

# COMMAND ----------

# 3) Controle simulada no Lakebase: banco + schema + tabela (estado atual de cada arquivo)
w = WorkspaceClient()
usuario = w.current_user.me().user_name
if P["db2_modo"] == "SIMULADO":
    inst = w.database.get_database_instance(name=P["lakebase_instance"])
    token = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[inst.name]).token
    conexao = dict(host=inst.read_write_dns, user=usuario, password=token, ssl_context=ssl.create_default_context())
    banco, tabela = P["lakebase_database"], P["lakebase_tabela"]
    schema_pg = tabela.split(".")[0] if "." in tabela else "public"

    con = pg8000.dbapi.connect(database="databricks_postgres", **conexao)   # banco padrão de toda instância
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (banco,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{banco}"')
    con.close()

    con = pg8000.dbapi.connect(database=banco, **conexao)
    cur = con.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_pg}")
    # Chave = objeto + data de movimento + data de geração: 1 linha por arquivo, atualizada a cada status
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {tabela} (
                      cod_objeto     INTEGER       NOT NULL,
                      nome_tabela    VARCHAR(128)  NOT NULL,
                      data_movimento INTEGER       NOT NULL,          -- yyyyMMdd, como no CTL
                      data_geracao   TIMESTAMP     NOT NULL,          -- do nome do arquivo
                      instituicoes   VARCHAR(4000),                   -- "0" = todas
                      status         VARCHAR(20)   NOT NULL,          -- EM_PROCESSAMENTO | ERRO | CONCLUIDO
                      qtd_registros  BIGINT,
                      arquivo        VARCHAR(256),
                      job_run_id     BIGINT,
                      lote_id        BIGINT,
                      modo_execucao  VARCHAR(20),                     -- DESPACHANTE | JOB
                      dt_inicio      TIMESTAMP,
                      dt_fim         TIMESTAMP,
                      mensagem       VARCHAR(2000),
                      confirmado_em  TIMESTAMP     NOT NULL DEFAULT now(),
                      PRIMARY KEY (cod_objeto, data_movimento, data_geracao))""")
    # Instalações antigas (antes das colunas de lote/início/fim): acrescenta o que faltar
    for coluna in ["lote_id BIGINT", "modo_execucao VARCHAR(20)", "dt_inicio TIMESTAMP", "dt_fim TIMESTAMP",
                   "mensagem VARCHAR(2000)"]:
        cur.execute(f"ALTER TABLE {tabela} ADD COLUMN IF NOT EXISTS {coluna}")
    con.commit()
    cur.execute(f"SELECT count(*) FROM {tabela}")
    print(f"controle simulada {banco}.{tabela}: {cur.fetchone()[0]} linhas")
    con.close()
else:
    # Db2 do cliente: nada a criar aqui. A conexão e a tabela são conferidas pelo job "PLHDWA · testar controle Db2".
    print("db2_modo = DB2: controle no Db2 do cliente (sem Lakebase). Rode o job 'PLHDWA · testar controle Db2'.")

# COMMAND ----------

# 4) Owner: escrita nos limites e leitura nas tabelas operacionais (governança: só o owner muda as vagas)
if P["grupo_owner"] and P["grupo_owner"] != usuario:
    spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {P['catalogo']}.{P['schema']} TO `{P['grupo_owner']}`")
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {fq('ctl_execucao')} TO `{P['grupo_owner']}`")
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {fq('log_carga_evento')} TO `{P['grupo_owner']}`")  # DESBLOQUEIO
print("instalação / pós-deploy concluído")
