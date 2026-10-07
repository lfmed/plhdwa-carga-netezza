# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Validação dos testes
# MAGIC Compara o manifesto (fila e resultado **esperados** de cada arquivo) com o que o motor registrou,
# MAGIC e faz as verificações de concorrência, ordem, reprocessamento, Db2 simulado, metadados e escape.
# MAGIC Resultado em `resultado_testes` e na tela. Falha a execução se algum item reprovar.

# COMMAND ----------

# Dependências (ibm-db, databricks-sdk, pg8000): vêm do environment do job (resources/jobs.yml, environments ->
# plhdwa) e são instaladas antes de o notebook começar. Versão do ambiente: variável serverless_ambiente.

# COMMAND ----------

import csv
import uuid
import ssl
import pg8000.dbapi
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

import plhdwa_motor as m

# Parâmetros (vêm de resources/jobs.yml -> variáveis do bundle): os do motor + manifesto e tag OWNER
P = m.ler_parametros(dbutils, m.COMUNS)
for nome, padrao in [("manifesto", ""), ("owner_tag", "PLHDWA")]:
    dbutils.widgets.text(nome, padrao)
    P[nome] = dbutils.widgets.get(nome) or padrao
m.iniciar(spark, dbutils, P)
P = {**m.CTX, **P}
fq = lambda t: f"{P['catalogo']}.{P['schema']}.{t}"
spark.conf.set("spark.sql.session.timeZone", P["fuso_horario"])

manifesto = list(csv.DictReader(open(P["manifesto"])))
nomes = [r["arquivo_ctl"] for r in manifesto]
resultados = []


def registrar(cenario, verificacao, ok, detalhe=""):
    resultados.append((cenario, verificacao, "OK" if ok else "FALHA", str(detalhe)[:1000]))

# COMMAND ----------

# 1) Status final e fila de cada arquivo do manifesto
ev = spark.table(fq("log_carga_evento")).filter(F.col("arquivo_ctl").isin(nomes))
status = {r.arquivo_ctl: r.status for r in spark.table(fq("v_status_arquivo")).filter(F.col("arquivo_ctl").isin(nomes)).collect()}
fila_real = {r.arquivo_ctl: r.f for r in ev.filter("status = 'ROTEADO'").groupBy("arquivo_ctl")
             .agg(F.max_by("fila", "evento_ts").alias("f")).collect()}
for r in manifesto:
    st = status.get(r["arquivo_ctl"])
    registrar(r["cenario"], f"{r['arquivo_ctl']} status", st == r["resultado_esperado"],
              f"esperado {r['resultado_esperado']}, obtido {st}")
    if r["resultado_esperado"] in ("CONCLUIDO", "ERRO_DB2"):
        registrar(r["cenario"], f"{r['arquivo_ctl']} fila", fila_real.get(r["arquivo_ctl"]) == r["fila_esperada"],
                  f"esperada {r['fila_esperada']}, obtida {fila_real.get(r['arquivo_ctl'])}")

# COMMAND ----------

# 2) Concorrência: intervalos ROTEADO -> fim (CONCLUIDO/ERRO) por arquivo
inter = (ev.groupBy("arquivo_ctl", "cod_objeto")
           .agg(F.min(F.when(F.col("status") == "ROTEADO", F.col("evento_ts"))).alias("ini"),
                F.max("evento_ts").alias("fim")).filter("ini IS NOT NULL").collect())
max_sim, mesmo_obj = 0, []
for a in inter:
    juntos = [b for b in inter if b.ini < a.fim and b.fim > a.ini]
    max_sim = max(max_sim, len({b.cod_objeto for b in juntos}))
    mesmo_obj += [(a.arquivo_ctl, b.arquivo_ctl) for b in juntos if b.cod_objeto == a.cod_objeto and b.arquivo_ctl != a.arquivo_ctl]
if any(r["cenario"] == "concorrencia" for r in manifesto):
    registrar("concorrencia", "objetos diferentes ao mesmo tempo (> 1)", max_sim > 1, f"máximo simultâneo: {max_sim}")
registrar("concorrencia", "nunca 2 arquivos do mesmo objeto ao mesmo tempo", not mesmo_obj, mesmo_obj[:3])

# 3) Ordem: o objeto 9040 concluído na ordem da data de geração
ordem = [r.arquivo_ctl for r in ev.filter("cod_objeto = 9040 AND status = 'CARREGADO'").orderBy("evento_ts").collect()]
if ordem:
    registrar("ordem", "9040 processado por data de geração", ordem == sorted(ordem), ordem)

# 4) Roteamento: 9004 passou por P -> G -> P em ordem
seq = [r.fila for r in ev.filter("cod_objeto = 9004 AND status = 'ROTEADO'").orderBy("evento_ts").collect()]
if seq:
    registrar("roteamento", "9004 roteado P -> G -> P", seq == ["P", "G", "P"], seq)

# COMMAND ----------

# 5) Reprocessamento 9041: a 2ª carga trocou só 501204 e 501207
if spark.catalog.tableExists(fq("teste_fato_9041")) and any(r["cenario"] == "reprocessamento" for r in manifesto):
    cont = {r.SK_INS: r.n for r in spark.table(fq("teste_fato_9041")).groupBy("SK_INS").agg(F.count("*").alias("n")).collect()}
    seg = ev.filter("cod_objeto = 9041 AND status = 'CARREGADO'").orderBy(F.col("evento_ts").desc()).first()
    ok = seg is not None and len(cont) == 5 and cont.get(501204, 0) + cont.get(501207, 0) == seg.qtd_inserida
    registrar("reprocessamento", "REPLACE WHERE só nas instituições do CTL", ok,
              f"linhas por instituição {cont}; inseridas na 2ª carga {seg and seg.qtd_inserida}")

# 6) Escape 9109: '2\|3' virou '2|3' e '\\' virou uma barra
if spark.catalog.tableExists(fq("teste_fato_9109")) and any(r["cenario"] == "escape" for r in manifesto):
    v = spark.table(fq("teste_fato_9109")).select("DS_CON_MAE").distinct().collect()
    registrar("escape", "texto com | e barra lido corretamente", [r.DS_CON_MAE for r in v] == ["CONTRATO 2|3 VIAS C:\\TEMP"],
              [r.DS_CON_MAE for r in v])

# COMMAND ----------

# 7) Controle: cada CONCLUIDO tem linha CONCLUIDO no histórico Delta e na controle (Db2 ou Lakebase simulado).
#    Com usar_db2 = nao, a controle não é gravada e esta verificação é pulada.
if m.usa_db2():
    concluidos = [n for n, s in status.items() if s == "CONCLUIDO"]
    delta = {r.arquivo for r in spark.table(fq("db2_controle_sim"))
             .filter(F.col("arquivo").isin(concluidos) & (F.col("status") == "CONCLUIDO")).collect()}
    registrar("db2", "CONCLUIDO com linha no histórico da controle (Delta)", set(concluidos) <= delta, set(concluidos) - delta)
    if P["db2_modo"] == "DB2":
        # o nome do CTL fica em ARQUIVO (layout padrão) ou NOME_JOB (layout CONTROLE); status CONCLUIDO
        no_db2 = set()
        for coluna in ("ARQUIVO", "NOME_JOB"):
            try:
                with m.conexao_db2() as con:
                    cur = con.cursor()
                    cur.execute(f"SELECT TRIM({coluna}) FROM {P['db2_tabela']} WHERE TRIM(STATUS) = 'CONCLUIDO'")
                    no_db2 = {r[0] for r in cur.fetchall()}
                break
            except Exception:
                pass
        registrar("db2", f"CONCLUIDO na controle do Db2 ({P['db2_tabela']})", set(concluidos) <= no_db2,
                  set(concluidos) - no_db2)
    else:
        w = WorkspaceClient()
        inst = w.database.get_database_instance(name=P["lakebase_instance"])
        tok = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[inst.name]).token
        con = pg8000.dbapi.connect(host=inst.read_write_dns, database=P["lakebase_database"],
                                   user=w.current_user.me().user_name, password=tok, ssl_context=ssl.create_default_context())
        cur = con.cursor()
        cur.execute(f"SELECT arquivo FROM {P['lakebase_tabela']} WHERE status = 'CONCLUIDO'")
        lake = {r[0] for r in cur.fetchall()}
        con.close()
        registrar("db2", f"CONCLUIDO na controle simulada ({P['lakebase_tabela']}, Lakebase)", set(concluidos) <= lake,
                  set(concluidos) - lake)
else:
    print("usar_db2 = nao: o motor não grava a controle; verificação da controle pulada")

# 8) Travamento: objetos com erro travados; objetos concluídos não travados
travados = {r.cod_objeto for r in spark.table(fq("v_objeto_travado")).collect()}
erro_esp = {int(r["cod_objeto"]) for r in manifesto if r["resultado_esperado"].startswith("ERRO")}
ok_esp = {int(r["cod_objeto"]) for r in manifesto if r["resultado_esperado"] == "CONCLUIDO"} - erro_esp
registrar("erros", "objetos com erro estão travados", erro_esp <= travados, erro_esp - travados)
registrar("erros", "objetos sem erro não estão travados", not (ok_esp & travados), ok_esp & travados)

# 9) Arquivos: concluídos apagados; erros de negócio na pasta de erro; ERRO_DB2 continua nas pastas de entrada
def existe(c):
    try:
        dbutils.fs.ls(c); return True
    except Exception:
        return False
d_ctl, d_dat, d_erro = (m.pasta(m.CTX[k]) for k in ("pasta_ctl", "pasta_dat", "pasta_erro"))
for r in manifesto:
    n, st = r["arquivo_ctl"], status.get(r["arquivo_ctl"])
    if st == "CONCLUIDO":
        registrar(r["cenario"], f"{n} apagado da landing", not existe(f"{d_ctl}/{n}") and not existe(f"{d_dat}/{n[:-4]}.dat"))
    elif st in ("ERRO_DB2", "ERRO_EXECUCAO"):
        registrar(r["cenario"], f"{n} mantido na pasta de CTL", existe(f"{d_ctl}/{n}"))
    elif st and st.startswith("ERRO"):
        registrar(r["cenario"], f"{n} movido para a pasta de erro", existe(f"{d_erro}/{n}") and not existe(f"{d_ctl}/{n}"))

# 10) Cadastro preservado depois das cargas: a linha do objeto na ctl_parametros aponta para a tabela (que segue
#     existindo) e o motor não depende de tags nem TBLPROPERTIES plhdwa.* na tabela destino
for t, cod in [("teste_fato_9001", 9001), ("teste_dim_9110", 9110), ("fat_parcela", 1230)]:
    if spark.catalog.tableExists(fq(t)):
        linha = spark.sql(f"""SELECT count(*) AS n FROM {fq('ctl_parametros')} WHERE cod_objeto = {cod}
                              AND catalogo_destino = '{P['catalogo']}' AND schema_destino = '{P['schema']}'
                              AND tabela_destino = '{t}'""").first()
        registrar("metadados", f"{t} com 1 linha na ctl_parametros", linha.n == 1)

# COMMAND ----------

execucao = str(uuid.uuid4())[:8]
df = (spark.createDataFrame(resultados, "cenario STRING, verificacao STRING, resultado STRING, detalhe STRING")
      .select(F.current_timestamp().alias("executado_em"), F.lit(execucao).alias("execucao"),
              F.lit(P["manifesto"].split("/")[-1]).alias("manifesto"), "*"))
df.write.mode("append").saveAsTable(fq("resultado_testes"))
spark.sql(f"ALTER TABLE {fq('resultado_testes')} SET TAGS ('OWNER' = '{P['owner_tag']}')")
display(df.groupBy("cenario", "resultado").count().orderBy("cenario"))
display(df.filter("resultado = 'FALHA'"))
falhas = df.filter("resultado = 'FALHA'").count()
print(f"execução {execucao}: {len(resultados) - falhas} OK, {falhas} FALHA")
if falhas:
    raise Exception(f"{falhas} verificação(ões) reprovada(s): ver resultado_testes, execucao = '{execucao}'")
