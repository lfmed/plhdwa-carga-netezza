# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Job independente (objetos com plhdwa_execucao = JOB)
# MAGIC Disparado pelo despachante com os parâmetros `ctl` (caminho do CTL), `modo` e `lote_id`.
# MAGIC Depois do arquivo recebido, **segue com os próximos arquivos do mesmo objeto** (em ordem de geração),
# MAGIC até `max_arquivos` ou `max_minutos`: o arranque do job (~2 min) é pago uma vez por objeto, e não por arquivo.
# MAGIC Enquanto esta execução estiver ativa, o despachante não manda outro arquivo do objeto (a ordem fica garantida).
# MAGIC O tipo do job (Standard ou Performance Optimized) é o deste job: existe um job por tipo.
# MAGIC Qualquer erro termina a execução como FAILED (o objeto fica travado no log).

# COMMAND ----------

# Dependências (ibm-db, databricks-sdk, pg8000): vêm do environment do job (resources/jobs.yml, environments ->
# plhdwa) e são instaladas antes de o notebook começar. Versão do ambiente: variável serverless_ambiente.

# COMMAND ----------

import time

import plhdwa_motor as m

# Parâmetros: ver a tabela PARAMETROS em plhdwa_motor.py (valores vêm de resources/jobs.yml)
P = m.ler_parametros(dbutils, m.COMUNS + ["ctl", "modo", "lote_id", "max_arquivos", "max_minutos"])
m.iniciar(spark, dbutils, P)

arq = m.ler_nome(P["ctl"].split("/")[-1])
if not arq:
    raise ValueError(f"nome de CTL inválido: {P['ctl']}")
# Proteção contra nova tentativa automática da tarefa: não reprocessa objeto travado nem arquivo concluído.
if m.ultimo_status(arq) == "CONCLUIDO":
    dbutils.notebook.exit("arquivo já concluído")
if arq["cod"] in m.objetos_travados():
    raise Exception(f"objeto {arq['cod']} está travado: ver v_objeto_travado (nada foi feito)")
inicio = time.time()
m.processar_arquivo(arq, "JOB", P["tipo_job"])
feitos = 1

# Segue com o objeto: próximo arquivo pendente (inclui os que chegaram depois do disparo).
# Os limites devolvem a vaga ao despachante de tempos em tempos, para um objeto não ficar com ela a noite toda.
while feitos < int(P["max_arquivos"]) and time.time() - inicio < float(P["max_minutos"]) * 60:
    prox = m.pendentes(arq["cod"]).get(arq["cod"], [])
    if not prox:
        break
    m.processar_arquivo(prox[0], "JOB", P["tipo_job"], roteado_ts=time.time())
    feitos += 1
print(f"objeto {arq['cod']}: {feitos} arquivo(s) nesta execução")
