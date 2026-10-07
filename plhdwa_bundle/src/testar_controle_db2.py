# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Testar a controle no Db2
# MAGIC Rode antes da primeira carga (e sempre que mudar host, usuário, tabela ou o comando da controle):
# MAGIC 1. conecta no Db2 com as variáveis `db2_*` e o usuário/senha do secret scope;
# MAGIC 2. confere se a tabela `db2_tabela` existe; com `criar_tabela = true`, cria a partir de
# MAGIC    `sql/02_db2_controle_ddl.sql` (só para o layout padrão);
# MAGIC 3. com `gravar_teste = true`, grava um arquivo fictício (objeto 0) que termina CONCLUIDO e outro que termina
# MAGIC    ERRO, do jeito que o motor grava (com `db2_registrar_inicio = true`, passam antes por EM_PROCESSAMENTO),
# MAGIC    usando o MESMO comando do motor (`db2_comando`); mostra as linhas e, com `apagar_teste = true`, apaga no fim.
# MAGIC Falha a execução se algo não funcionar, com a mensagem do Db2.

# COMMAND ----------

# Dependências (ibm-db, databricks-sdk, pg8000): vêm do environment do job (resources/jobs.yml, environments ->
# plhdwa) e são instaladas antes de o notebook começar. Versão do ambiente: variável serverless_ambiente.

# COMMAND ----------

import os
import platform
import sys
import time

import plhdwa_motor as m

# máquina e Python em que o job rodou (o driver ibm-db vem do environment do job)
print(f"máquina {platform.machine()} · Python {sys.version.split()[0]}")

P = m.ler_parametros(dbutils, m.COMUNS)
for nome, padrao in [("criar_tabela", "false"), ("gravar_teste", "true"), ("apagar_teste", "true")]:
    dbutils.widgets.text(nome, padrao)
    P[nome] = dbutils.widgets.get(nome).lower() == "true"
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

# 1) Conexão
t = time.time()
servidor = consultar("SELECT CURRENT SERVER, CURRENT USER, CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")[0]
print(f"conectado em {time.time() - t:.1f} s · banco {servidor[0]} · usuário {servidor[1]} · hora do Db2 {servidor[2]}")

# 2) Tabela da controle
try:
    n = consultar(f"SELECT COUNT(*) FROM {tabela}")[0][0]
    print(f"tabela {tabela}: {n} linhas")
except Exception as e:
    if not P["criar_tabela"]:
        raise Exception(f"tabela {tabela} não acessível ({e}). Peça ao DBA para criar com sql/02_db2_controle_ddl.sql "
                        f"ou rode este job com criar_tabela = true") from e
    caminho = os.path.join(os.path.dirname(m.__file__), "..", "sql", "02_db2_controle_ddl.sql")
    ddl = "\n".join(l for l in open(caminho, encoding="utf-8").read().splitlines() if not l.strip().startswith("--"))
    with m.conexao_db2() as con:
        con.cursor().execute(ddl.strip().rstrip(";").replace("{tabela}", tabela))
        con.commit()
    print(f"tabela {tabela} criada")

# COMMAND ----------

# 3) Ciclo completo com o comando do motor (o mesmo que roda nas cargas)
if P["gravar_teste"]:
    agora = time.strftime("%Y%m%d%H%M%S")
    arquivos = [m.ler_nome(f"0_PLHDWA_TESTE_CONCLUIDO_20000101_{agora}.CTL"),
                m.ler_nome(f"0_PLHDWA_TESTE_ERRO_20000102_{agora}.CTL")]
    inicio = time.time()
    # mesma sequência do motor: o início só vai para o Db2 com db2_registrar_inicio = true
    com_inicio = str(m.CTX["db2_registrar_inicio"]).lower() == "true"
    if com_inicio:
        m._gravar_db2(arquivos[0], "EM_PROCESSAMENTO", "0", "DESPACHANTE", None, inicio, None, None)
    m._gravar_db2(arquivos[0], "CONCLUIDO", "0", "DESPACHANTE", 123, inicio, time.time(), None)
    if com_inicio:
        m._gravar_db2(arquivos[1], "EM_PROCESSAMENTO", "0", "JOB", None, inicio, None, None)
    m._gravar_db2(arquivos[1], "ERRO", "0", "JOB", None, inicio, time.time(), "ERRO_DADOS: teste de gravação do motor")
    print(f"{4 if com_inicio else 2} gravações em {time.time() - inicio:.1f} s com o comando {m.CTX['db2_comando']} "
          f"(db2_registrar_inicio = {m.CTX['db2_registrar_inicio']})")

    # Mostra as linhas de teste. O nome do CTL fica em ARQUIVO (layout padrão) ou NOME_JOB (layout CONTROLE).
    nomes = tuple(a["nome_ctl"] for a in arquivos)
    for coluna in ("ARQUIVO", "NOME_JOB"):
        try:
            linhas = consultar(f"SELECT * FROM {tabela} WHERE {coluna} IN (?, ?)", nomes)
            break
        except Exception:
            pass
    else:
        raise Exception("não achei a coluna com o nome do arquivo (ARQUIVO ou NOME_JOB) para conferir")
    for l in linhas:
        print(l)
    status = sorted(str(l).upper() for l in linhas)
    if len(linhas) != 2 or not any("CONCLUIDO" in s for s in status) or not any("ERRO" in s for s in status):
        raise Exception(f"esperado 2 linhas (1 CONCLUIDO e 1 ERRO), veio: {linhas}")
    print("controle OK: 1 linha por arquivo, com o fim (CONCLUIDO / ERRO)"
          + (", depois do EM_PROCESSAMENTO" if com_inicio else ""))

    if P["apagar_teste"]:
        with m.conexao_db2() as con:
            con.cursor().execute(f"DELETE FROM {tabela} WHERE {coluna} IN (?, ?)", nomes)
            con.commit()
        print("linhas de teste apagadas")
