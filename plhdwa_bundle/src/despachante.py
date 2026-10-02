# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Despachante (cada execução = 1 lote)
# MAGIC Disparado pela chegada de CTL em `raw/ctl/`. Regras:
# MAGIC - o **modo de execução vem da tag do objeto** (`plhdwa_execucao`):
# MAGIC   - **DESPACHANTE**: o arquivo roda aqui mesmo, numa thread;
# MAGIC   - **JOB**: dispara um job independente (`plhdwa_tipo_job` escolhe Standard ou Performance), que segue
# MAGIC     sozinho com os próximos arquivos do mesmo objeto;
# MAGIC - **a vaga é do arquivo, não do objeto**: cada thread processa 1 arquivo e devolve a vaga; o despachante
# MAGIC   preenche a vaga na hora (não espera o próximo ciclo);
# MAGIC - entre os objetos que podem andar, vai primeiro o que tem **mais trabalho pendente** (cadeia mais longa),
# MAGIC   para não sobrar um objeto sozinho no fim do lote;
# MAGIC - no máximo **1 arquivo por objeto** em andamento, em qualquer modo (`max_simult` = objetos em paralelo);
# MAGIC - a cada `ciclo_s` relê `ctl_execucao`, a pasta e os jobs; o lote termina quando não há pendentes nem nada rodando.
# MAGIC
# MAGIC Cada arquivo sensibiliza a controle sozinho (início, erro, conclusão), mesmo rodando dentro do despachante.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.65" "pg8000>=1.31"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Driver do Db2 (ibm_db). Instalado à parte para não derrubar o notebook se a máquina não suportar (ARM):
# nesse caso só a gravação na controle falha, com mensagem clara (ver _conexao_db2 em plhdwa_motor.py).
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "ibm_db>=3.2"], capture_output=True)

# COMMAND ----------

import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

import plhdwa_motor as m

# Parâmetros: ver a tabela PARAMETROS em plhdwa_motor.py (valores vêm de resources/jobs.yml)
P = m.ler_parametros(dbutils, m.COMUNS + ["ciclo_s", "max_horas", "max_threads"])
P["lote_id"] = P["job_run_id"]                 # o lote é esta execução do despachante
m.iniciar(spark, dbutils, P)
w = WorkspaceClient()

# COMMAND ----------

def processar_no_despachante(arq, roteado_ts):
    """Thread: processa UM arquivo e devolve a vaga. Erro = objeto travado (o despachante deixa de mandar arquivos dele)."""
    try:
        m.processar_arquivo(arq, "DESPACHANTE", P["tipo_job"], roteado_ts=roteado_ts)
        return "OK"
    except m.ErroCarga:
        return "ERRO"
    except Exception as e:                     # nunca deixar uma thread derrubar o despachante
        print(f"falha inesperada em {arq['nome_ctl']}: {e}")
        return "ERRO"


def prioridade(item):
    """Cadeia mais longa primeiro: mais arquivos pendentes, depois mais bytes, depois a geração mais antiga."""
    _, arquivos = item
    if not arquivos:                           # objeto sem arquivo na memória (ex.: entregue a um job): vai para o fim
        return (0, 0, "")
    return (-len(arquivos), -sum(tamanhos.get(a["nome_dat"], 0) for a in arquivos), arquivos[0]["dt_ger"])

# COMMAND ----------

pool = ThreadPoolExecutor(max_workers=int(P["max_threads"]))   # teto físico; o limite efetivo é max_simult do modo DESPACHANTE
threads = {}                                   # future -> (cod, arquivo) · 1 arquivo por thread
lancados = {}                                  # run_id -> arquivo inicial dos jobs independentes disparados por este lote
run_cod = {}                                   # run_id -> cod (cache: evita get_run a cada ciclo)
em_job = {}                                    # cod -> run_id dos jobs independentes ativos
estado_run = {}                                # run_id -> (estado no Jobs, início em epoch) · vai para log_job_ativo
pend, tamanhos = {}, {}                        # foto da pasta (refeita a cada ciclo; entre ciclos, atualizada em memória)
erros = 0
execucao = None
listado_em = 0.0
inicio = time.time()
m.fechar_lotes_orfaos()                        # lote anterior que caiu/foi cancelado sai de "em andamento"
m.lote("INICIADO", pendentes=sum(len(v) for v in m.pendentes().values()))


def runs_ativos():
    """Jobs independentes em andamento, por objeto. Sobrevive a reinício do despachante (lê o Jobs)."""
    ativos = {}
    for job_id in {execucao["JOB"]["job_id_perf"], execucao["JOB"]["job_id_std"]}:
        for r in w.jobs.list_runs(job_id=job_id, active_only=True):
            if r.run_id not in run_cod:
                if r.run_id in lancados:
                    run_cod[r.run_id] = lancados[r.run_id]["cod"]
                else:                          # disparado por um lote anterior: descobre o objeto uma vez
                    run = w.jobs.get_run(r.run_id)
                    ctl = next((p.value for p in (run.job_parameters or []) if p.name == "ctl"), "")
                    arq = m.ler_nome(ctl.split("/")[-1])
                    run_cod[r.run_id] = arq["cod"] if arq else None
            if run_cod[r.run_id] is not None:
                ativos[run_cod[r.run_id]] = r.run_id
                estado_run[r.run_id] = _estado(r)
    return ativos


def _estado(run):
    """(QUEUED | PENDING | RUNNING | ..., início em epoch ou None) de uma execução do Jobs."""
    st = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else None
    return st, (run.start_time / 1000 if run.start_time else None)


def ciclo_completo():
    """Relê limites, jobs e pasta. Trata os jobs que terminaram e grava a foto do ciclo."""
    global execucao, em_job, pend, tamanhos, erros, listado_em
    nova = m.ler_execucao()
    if nova:
        execucao = nova
    elif execucao is None:
        raise Exception("ctl_execucao inválida (precisa das linhas DESPACHANTE e JOB, com os job_id)")
    em_job = runs_ativos()
    for run_id in [r for r in lancados if r not in em_job.values()]:
        run = w.jobs.get_run(run_id)
        if run.state.life_cycle_state in (RunLifeCycleState.PENDING, RunLifeCycleState.QUEUED,
                                          RunLifeCycleState.RUNNING, RunLifeCycleState.BLOCKED):
            em_job[lancados[run_id]["cod"]] = run_id    # ainda na fila do Jobs
            estado_run[run_id] = _estado(run)
            continue
        arq = lancados.pop(run_id)
        if run.state.result_state != RunResultState.SUCCESS:
            erros += 1
            # o job pode ter feito vários arquivos do objeto: o que ficou em aberto é o que caiu
            aberto = m.arquivo_em_aberto(arq["cod"])
            if aberto:
                m.evento(aberto, "ERRO_EXECUCAO", fila="JOB",
                         mensagem=f"execução {run_id} terminou {run.state.result_state} sem evento final")
    pend, tamanhos = m.pendentes(), m.tamanhos_dat()
    listado_em = time.time()
    uso = uso_por_modo()
    foto = {md: [0, 0] for md in m.MODOS}
    for cod, arquivos in pend.items():
        md = m.execucao_do_objeto(cod)[0]
        foto[md][0] += len(arquivos)
        foto[md][1] += sum(tamanhos.get(a["nome_dat"], 0) for a in arquivos)
    ts = time.time()                            # mesmo horário nas duas fotos: o dashboard junta as duas pelo ciclo
    m.ciclo([(int(P["job_run_id"]), md, foto[md][0], foto[md][1], uso[md], execucao[md]["max_simult"]) for md in m.MODOS],
            ts=ts)
    # lista dos jobs independentes ativos (o cartão do dashboard conta; esta foto mostra quais são)
    m.jobs_ativos([(cod, run_id, m.execucao_do_objeto(cod)[1]) + estado_run.get(run_id, (None, None))
                   for cod, run_id in em_job.items()], ts=ts)


def uso_por_modo():
    return {"DESPACHANTE": len(threads), "JOB": len(em_job)}


def despachar():
    """Preenche as vagas livres de cada modo, cadeia mais longa primeiro, 1 arquivo por objeto."""
    ocupados = {c for c, _ in threads.values()} | set(em_job)
    uso = uso_por_modo()
    for cod, arquivos in sorted(pend.items(), key=prioridade):
        if cod in ocupados or not arquivos:
            continue
        modo, tipo_job = m.execucao_do_objeto(cod)
        if uso[modo] >= execucao[modo]["max_simult"]:
            continue                            # modo sem vaga: espera a próxima vaga
        uso[modo] += 1
        ocupados.add(cod)
        prox = arquivos[0]
        if modo == "DESPACHANTE":
            threads[pool.submit(processar_no_despachante, prox, time.time())] = (cod, prox)
        else:
            roteado_ts = time.time()            # horário do disparo: fica antes do INICIADO gravado pelo job
            run = w.jobs.run_now(job_id=m.job_do_tipo(execucao, tipo_job),
                                 job_parameters={"ctl": prox["ctl"], "modo": "JOB", "lote_id": P["lote_id"]})
            # ROTEADO já com a execução do job (e não a do despachante): o dashboard liga o arquivo ao job
            m.evento(prox, "ROTEADO", fila="JOB", tipo_job=tipo_job, tamanho=tamanhos.get(prox["nome_dat"], 0),
                     job_run_id=run.run_id, tabela=m.tabela_do_objeto(cod), ts=roteado_ts)
            lancados[run.run_id] = prox
            run_cod[run.run_id] = cod
            em_job[cod] = run.run_id
            pend.pop(cod, None)                 # o job segue sozinho com o objeto; a pasta é relida no próximo ciclo

# COMMAND ----------

while time.time() - inicio < float(P["max_horas"]) * 3600:
    if time.time() - listado_em >= float(P["ciclo_s"]):
        ciclo_completo()
        if not pend and not threads and not em_job and not lancados:
            break
    despachar()
    # Espera a 1ª thread terminar (vaga livre) ou a hora do próximo ciclo, o que vier primeiro
    espera = max(0.5, float(P["ciclo_s"]) - (time.time() - listado_em))
    if threads:
        prontas, _ = wait(list(threads), timeout=espera, return_when=FIRST_COMPLETED)
    else:
        time.sleep(espera)
        prontas = []
    for f in prontas:
        cod, arq = threads.pop(f)
        if f.result() == "ERRO":
            erros += 1
            pend.pop(cod, None)                 # objeto travado: nada mais dele neste lote
        else:
            pend[cod] = [a for a in pend.get(cod, []) if a["nome_ctl"] != arq["nome_ctl"]]
            if not pend[cod]:
                pend.pop(cod)                   # sem mais arquivos deste objeto na memória

pool.shutdown(wait=True)
# Conta pelo log, e não só pela memória: o serverless pode refazer a tarefa automaticamente, e a nova
# tentativa (mesmo job_run_id) precisa terminar FAILED também, para a execução notificar.
erros = max(erros, m.sql(f"""SELECT count(DISTINCT cod_objeto) AS n FROM {m.fq('log_carga_evento')}
                              WHERE lote_id = :r AND status LIKE 'ERRO%'""", r=int(P["lote_id"])).first().n)
m.lote("FINALIZADO", mensagem=f"{erros} objeto(s) com erro" if erros else "sem erros")
print(f"fim do lote {P['lote_id']}: {erros} objeto(s) com erro")
if erros:
    # A execução termina FAILED para disparar a notificação; os outros objetos já foram processados.
    raise Exception(f"{erros} objeto(s) com erro: ver v_objeto_travado")
