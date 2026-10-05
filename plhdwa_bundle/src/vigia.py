# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Vigia (agendado; padrão a cada 15 min)
# MAGIC Rede de segurança: se houver CTL pendente há mais de `vigia_parado_min` minutos (ex.: depois de um
# MAGIC DESBLOQUEIO, que não gera arquivo novo, ou um gatilho perdido) e o despachante não estiver rodando,
# MAGIC dispara o despachante. Não processa nada sozinho.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
import plhdwa_motor as m

# Parâmetros: ver a tabela PARAMETROS em plhdwa_motor.py (valores vêm de resources/jobs.yml)
P = m.ler_parametros(dbutils, m.COMUNS + ["despachante_job_id", "vigia_parado_min"])
m.iniciar(spark, dbutils, P)
w = WorkspaceClient()

# CTLs que estão na pasta há mais tempo que o limite e ainda não foram concluídos (nem são de objeto travado)
limite_ms = (time.time() - float(P["vigia_parado_min"]) * 60) * 1000
antigos = {f.name for f in m._ls(m.pasta(m.CTX["pasta_ctl"])) if f.modificationTime < limite_ms}
pend = [a for arqs in m.pendentes().values() for a in arqs if a["nome_ctl"] in antigos]
rodando = list(w.jobs.list_runs(job_id=int(P["despachante_job_id"]), active_only=True))
print(f"pendentes há mais de {P['vigia_parado_min']} min: {len(pend)} · despachante rodando: {bool(rodando)}")
if pend and not rodando:
    w.jobs.run_now(job_id=int(P["despachante_job_id"]))
    print("despachante disparado")
