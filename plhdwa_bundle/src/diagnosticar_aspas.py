# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Diagnóstico de aspas num DAT (não carrega nada)
# MAGIC Para o `ERRO_DADOS` "registro com quebra de linha maior que um pedaço inteiro do arquivo" da limpeza CLIENTE.
# MAGIC Lê o DAT com as mesmas regras da limpeza e mostra:
# MAGIC - quantas linhas parecem abrir ou fechar um texto em várias linhas, pela **regra atual** (aspas do CSV: só no começo
# MAGIC   ou no fim de um campo) e pela **regra antiga** (qualquer aspa da linha);
# MAGIC - as linhas em que as duas discordam (em geral, apóstrofo no meio do texto, como `D'AVILA`);
# MAGIC - se a limpeza vai parar e em qual linha (a que abre um texto que não fecha no pedaço seguinte inteiro).
# MAGIC
# MAGIC Parâmetros: `cod_objeto` (lê formato e encoding da `ctl_parametros`) ou só `dat`; `limite` = linhas mostradas.

# COMMAND ----------

import re

from pyspark.sql import functions as F

import plhdwa_motor as m

dbutils.widgets.text("catalogo", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("dat", "")                 # caminho completo do DAT (s3://... ou /Volumes/...)
dbutils.widgets.text("cod_objeto", "")          # vazio = formato padrão (| e aspas simples, header, UTF-8)
dbutils.widgets.text("limite", "20")
P = {k: dbutils.widgets.get(k) for k in ("catalogo", "schema", "dat", "cod_objeto", "limite")}
m.CTX.update({n: v[0] for n, v in m.PARAMETROS.items()})
m.CTX.update({"spark": spark, "dbutils": dbutils, "catalogo": P["catalogo"], "schema": P["schema"]})
if P["cod_objeto"]:
    meta = {"params": m._linha_objeto(int(P["cod_objeto"])) or {}}
else:
    meta = {"params": {}}
arq = {"dat": P["dat"], "nome_dat": P["dat"].rsplit("/", 1)[-1]}
LIMITE = int(P["limite"] or 20)
header = 1 if m._p(meta, "header", "true") == "true" else 0
print(f"DAT {P['dat']} · delimitador {m._p(meta, 'delimitador', '|')!r} · aspas {m._p(meta, 'limpeza_quote', chr(39))!r} · "
      f"encoding {m._p(meta, 'encoding', m.ENCODING_PADRAO)} · header {bool(header)}")

# COMMAND ----------

def regra_antiga(meta):
    """Contagem até 21/10/2026: todas as aspas não precedidas de barra."""
    return F.size(F.split(F.col("value"), f"(?<!\\\\){re.escape(m._p(meta, 'limpeza_quote', chr(39)))}")) - 1


df = (m._linhas_posicionadas(arq, meta)
      .withColumn("_impar_antigo", (regra_antiga(meta) % 2 == 1).cast("int")))
resumo = sorted(df.groupBy("_bloco").agg(
    F.min("_pos").alias("ini"), F.max("_pos").alias("fim"),
    F.sum("_impar").alias("impares"), F.sum("_impar_antigo").alias("impares_antigo"),
    F.min(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("pri"), F.max(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("ult"),
    F.min(F.when(F.col("_impar_antigo") == 1, F.col("_pos"))).alias("pri_a"),
    F.max(F.when(F.col("_impar_antigo") == 1, F.col("_pos"))).alias("ult_a")).collect(), key=lambda r: r["_bloco"])
antes = {}                                     # linhas antes de cada pedaço: nº da linha no arquivo
total = 0
for r in resumo:
    antes[r["_bloco"]] = total
    total += int(r["fim"]) - int(r["ini"]) + 1


def n_linha(bloco, pos, ini):
    return antes[bloco] + int(pos) - int(ini) + 1 + header


def simular(campo_imp, campo_pri, campo_ult):
    """Encadeia os pedaços como a limpeza: devolve None (passa) ou (pedaço que abre, posição da linha que abre)."""
    aberto = False
    for i, r in enumerate(resumo):
        if aberto and r[campo_pri] is None:
            return resumo[i - 1]["_bloco"], resumo[i - 1][campo_ult], resumo[i - 1]["ini"], r["_bloco"]
        aberto = (int(aberto) + int(r[campo_imp] or 0)) % 2 == 1
    return None


print(f"{total} linha(s) de dados em {len(resumo)} pedaço(s)")
print(f"linhas que abrem/fecham texto em várias linhas: regra atual {sum(int(r['impares'] or 0) for r in resumo)}"
      f" · regra antiga {sum(int(r['impares_antigo'] or 0) for r in resumo)}")
for nome, campos in (("regra atual", ("impares", "pri", "ult")), ("regra antiga", ("impares_antigo", "pri_a", "ult_a"))):
    s = simular(*campos)
    if s is None:
        print(f"- {nome}: a limpeza passa (todo texto aberto fecha no pedaço seguinte)")
    else:
        b, pos, ini, b2 = s
        v = df.filter((F.col("_bloco") == F.lit(b)) & (F.col("_pos") == F.lit(pos))).select("value").first()[0]
        print(f"- {nome}: a limpeza PARA na linha {n_linha(b, pos, ini)} (pedaço do byte {b}; nada fecha a partir do byte {b2}):"
              f"\n    {v[:500]}")

# COMMAND ----------

# Linhas em que as regras discordam (a antiga via um texto aberto/fechado que o leitor de CSV não vê): em geral apóstrofo
# no meio do texto. E as primeiras linhas que abrem/fecham pela regra atual (registros em várias linhas de verdade).
inis = {r["_bloco"]: r["ini"] for r in resumo}
ini_col = F.create_map(*[x for b, i in inis.items() for x in (F.lit(b), F.lit(i))])
antes_col = F.create_map(*[x for b, a in antes.items() for x in (F.lit(b), F.lit(a))])
com_linha = df.withColumn("linha", antes_col[F.col("_bloco")] + F.col("_pos") - ini_col[F.col("_bloco")] + 1 + header)
discordam = com_linha.filter(F.col("_impar") != F.col("_impar_antigo"))
print(f"\n{discordam.count()} linha(s) em que as regras discordam (primeiras {LIMITE}):")
display(discordam.orderBy("_bloco", "_pos").select("linha", F.col("_bloco").alias("byte_do_pedaco"),
                                                   "_impar", "_impar_antigo", F.substring("value", 1, 500).alias("linha_texto"))
        .limit(LIMITE))
print(f"Primeiras {LIMITE} linhas que abrem/fecham texto em várias linhas pela regra atual:")
display(com_linha.filter("_impar = 1").orderBy("_bloco", "_pos")
        .select("linha", F.col("_bloco").alias("byte_do_pedaco"), F.substring("value", 1, 500).alias("linha_texto"))
        .limit(LIMITE))
