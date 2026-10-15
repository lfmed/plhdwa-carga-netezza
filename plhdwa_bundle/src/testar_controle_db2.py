# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Testar a controle no Db2
# MAGIC Rode antes da primeira carga (e sempre que mudar host, usuário, tabela ou os comandos da controle):
# MAGIC 1. conecta no Db2 com as variáveis `db2_*` e o usuário/senha do secret scope, com o tempo-limite `db2_timeout_s`;
# MAGIC 2. confere se a tabela `db2_tabela` existe;
# MAGIC 3. lê os comandos de `db2_comando` (só UPDATE, uma seção por status) e faz o PREPARE de cada um no Db2, que
# MAGIC    confere tabela, colunas e tipos **sem executar nada**;
# MAGIC 4. só com `objeto_teste` preenchido: executa de verdade o UPDATE de CONCLUIDO e depois o de ERRO nesse objeto e
# MAGIC    mostra quantas linhas cada um atualizou. Use um código de teste combinado com o DBA (a linha fica como ERRO).
# MAGIC    `cd_inst_teste` (padrão 0) = o cd_inst de um CTL, ex. `1,3`: só as linhas dessas instituições (CD_INS) mudam.
# MAGIC Falha a execução se algo não funcionar, com a mensagem do Db2.

# COMMAND ----------

# Dependências (ibm-db, databricks-sdk, pg8000): vêm do environment do job (resources/jobs.yml, environments ->
# plhdwa) e são instaladas antes de o notebook começar. Versão do ambiente: variável serverless_ambiente.

# COMMAND ----------

import platform
import sys
import time

import plhdwa_motor as m

# máquina e Python em que o job rodou (o driver ibm-db vem do environment do job)
print(f"máquina {platform.machine()} · Python {sys.version.split()[0]}")

P = m.ler_parametros(dbutils, m.COMUNS)
dbutils.widgets.text("objeto_teste", "")
P["objeto_teste"] = dbutils.widgets.get("objeto_teste").strip()
dbutils.widgets.text("cd_inst_teste", "0")
P["cd_inst_teste"] = dbutils.widgets.get("cd_inst_teste").strip() or "0"
P["db2_modo"] = "DB2"
m.iniciar(spark, dbutils, P)
if not m.usa_db2():
    dbutils.notebook.exit("usar_db2 = nao: o motor não executa as etapas do Db2; nada a testar")
tabela = m.CTX["db2_tabela"]
if not tabela:
    raise ValueError("variável db2_tabela vazia")


def consultar(sql, params=()):
    with m.conexao_db2() as con:
        cur = con.cursor()
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []

# COMMAND ----------

# 1) Conexão (com ConnectTimeout e CURRENT LOCK TIMEOUT = db2_timeout_s)
t = time.time()
servidor = consultar("SELECT CURRENT SERVER, CURRENT USER, CURRENT TIMESTAMP, CURRENT LOCK TIMEOUT FROM SYSIBM.SYSDUMMY1")[0]
print(f"conectado em {time.time() - t:.1f} s · banco {servidor[0]} · usuário {servidor[1]} · hora do Db2 {servidor[2]} "
      f"· lock timeout {servidor[3]} s")

# 2) Tabela da controle
n = consultar(f"SELECT COUNT(*) FROM {tabela}")[0][0]
print(f"tabela {tabela}: {n} linhas")

# COMMAND ----------

# 3) Comandos: só UPDATE, uma seção por status; PREPARE no Db2 (confere colunas e tipos sem executar)
import ibm_db

comandos = m._comandos_db2()
with m.conexao_db2() as con:
    for status, (sql_, marcadores) in comandos.items():
        sql_ = sql_.replace(m._LISTA_DB2, "?")                     # :lista_cd_inst com 1 instituição
        if not ibm_db.prepare(con.conn_handler, sql_):
            raise Exception(f"[{status}] PREPARE recusado pelo Db2: {ibm_db.stmt_errormsg()}")
        print(f"[{status}] PREPARE ok · marcadores: {', '.join(marcadores) or '(nenhum)'}")
print(f"comandos de {m.CTX['db2_comando']}: {', '.join(comandos)} · só UPDATE")

# COMMAND ----------

# 4) Execução de verdade, só com objeto_teste (a linha do objeto precisa existir: o motor não insere)
if P["objeto_teste"]:
    cod = int(P["objeto_teste"])
    arq = m.ler_nome(f"{cod}_PLHDWA_TESTE_20000101_{time.strftime('%Y%m%d%H%M%S')}.CTL")
    inicio = time.time()
    insts = m._lista_ctl(P["cd_inst_teste"], "cd_inst_teste")
    m._gravar_db2(arq, "CONCLUIDO", insts, "DESPACHANTE", 123, inicio, time.time(), None)
    m._gravar_db2(arq, "ERRO", insts, "DESPACHANTE", None, inicio, time.time(),
                  "ERRO_DADOS: teste de gravação do job 'testar controle Db2'")
    print(f"objeto {cod}, cd_inst {insts}: UPDATE de CONCLUIDO e de ERRO executados (as linhas ficaram como ERRO, "
          f"com a mensagem de teste)")
else:
    print("objeto_teste vazio: nenhuma linha da controle foi alterada")
