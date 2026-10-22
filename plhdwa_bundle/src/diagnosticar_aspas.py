# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Diagnóstico de aspas num DAT (não carrega nada)
# MAGIC Para o `ERRO_DADOS` "registro com quebra de linha maior que um pedaço inteiro do arquivo" da limpeza CLIENTE.
# MAGIC Lê o DAT com as mesmas regras da limpeza e mostra:
# MAGIC - quantas linhas parecem abrir ou fechar um texto em várias linhas, pela **regra atual** (aspas do CSV: só no começo
# MAGIC   ou no fim de um campo) e pela **regra antiga** (qualquer aspa da linha);
# MAGIC - as linhas em que as duas discordam (em geral, apóstrofo no meio do texto, como `D'AVILA`);
# MAGIC - se a limpeza vai parar e em qual linha (a que abre um texto que não fecha no pedaço seguinte inteiro, ou que
# MAGIC   não fecha até o fim do arquivo: arquivo cortado);
# MAGIC - **alternância abre/fecha** (22/10/2026): a linha que abre um texto em várias linhas começa como um registro; a que
# MAGIC   fecha começa no meio do texto. Duas "abre" ou duas "fecha" seguidas mostram onde está a linha mal contada que
# MAGIC   inverte a contagem até o fim do arquivo (sinal: total de linhas ímpares ímpar).
# MAGIC
# MAGIC Parâmetros: `cod_objeto` (lê formato e encoding da `ctl_parametros`) ou só `dat`; `limite` = linhas mostradas;
# MAGIC `inicio_registro` = regex do começo de um registro (vazio = número seguido do delimitador).

# COMMAND ----------

import re

from pyspark.sql import functions as F

import plhdwa_motor as m

dbutils.widgets.text("catalogo", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("dat", "")                 # caminho completo do DAT (s3://... ou /Volumes/...)
dbutils.widgets.text("cod_objeto", "")          # vazio = formato padrão (| e aspas simples, header, UTF-8)
dbutils.widgets.text("limite", "20")
dbutils.widgets.text("inicio_registro", "")    # vazio = ^-?[0-9]+ seguido do delimitador (1º campo numérico)
P = {k: dbutils.widgets.get(k) for k in ("catalogo", "schema", "dat", "cod_objeto", "limite", "inicio_registro")}
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


def simular(campo_imp, campo_pri, campo_ult, fim=True):
    """Encadeia os pedaços como a limpeza: devolve None (passa) ou (pedaço que abre, posição da linha que abre, início
    do pedaço, byte do pedaço seguinte; None = o arquivo termina com o texto aberto, só com fim = True)."""
    aberto = False
    for i, r in enumerate(resumo):
        if aberto and r[campo_pri] is None:
            return resumo[i - 1]["_bloco"], resumo[i - 1][campo_ult], resumo[i - 1]["ini"], r["_bloco"]
        aberto = (int(aberto) + int(r[campo_imp] or 0)) % 2 == 1
    if aberto and fim:
        return resumo[-1]["_bloco"], resumo[-1][campo_ult], resumo[-1]["ini"], None
    return None


print(f"{total} linha(s) de dados em {len(resumo)} pedaço(s)")
print(f"linhas que abrem/fecham texto em várias linhas: regra atual {sum(int(r['impares'] or 0) for r in resumo)}"
      f" · regra antiga {sum(int(r['impares_antigo'] or 0) for r in resumo)}")
# a regra antiga nunca olhou o fim do arquivo (fim = False): a comparação mostra o que o código antigo faria
for nome, campos, fim in (("regra atual", ("impares", "pri", "ult"), True),
                          ("regra antiga", ("impares_antigo", "pri_a", "ult_a"), False)):
    s = simular(*campos, fim=fim)
    if s is None:
        print(f"- {nome}: a limpeza passa (todo texto aberto fecha no pedaço seguinte)")
        continue
    b, pos, ini, b2 = s
    achada = df.filter((F.col("_bloco") == F.lit(b)) & (F.col("_pos") == F.lit(pos))).select("value").first()
    v = achada[0] if achada else "(linha não encontrada: a divisão do arquivo mudou; rode de novo)"
    if b2 is None:
        print(f"- {nome}: o arquivo termina com um texto aberto na linha {n_linha(b, pos, ini)} (pedaço do byte {b}). A "
              f"limpeza segue; se a leitura falhar, é esta linha (arquivo cortado ou aspa sem escape):\n    {v[:500]}")
    else:
        print(f"- {nome}: a limpeza PARA na linha {n_linha(b, pos, ini)} (pedaço do byte {b}; nada fecha a partir do "
              f"byte {b2}):\n    {v[:500]}")

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


# COMMAND ----------

# Alternância abre/fecha. Cada registro em várias linhas tem 1 linha que abre (começa como registro) e 1 que fecha
# (começa no meio do texto): na ordem do arquivo, elas alternam. Quebra da alternância = linha mal contada por perto
# (a anterior ou a própria). Depois de cada quebra, a conferência segue a partir do papel da linha (ressincroniza).
inicio_rx = P["inicio_registro"] or f"^-?[0-9]+{re.escape(m._p(meta, 'delimitador', '|'))}"
impares = (com_linha.filter("_impar = 1").orderBy("_bloco", "_pos")
           .select("linha", "_bloco", F.col("value").rlike(inicio_rx).alias("abre"),
                   F.substring("value", 1, 300).alias("texto")).limit(500000).collect())
n_abre = sum(1 for r in impares if r["abre"])
print(f"\nAlternância (começo de registro = {inicio_rx!r}): {len(impares)} linha(s) ímpar(es) · {n_abre} abrem · "
      f"{len(impares) - n_abre} fecham" + (" · total ímpar: há linha mal contada" if len(impares) % 2 else ""))
quebras, aberto, anterior = [], False, None
for r in impares:
    if r["abre"] == aberto:                    # abre com texto já aberto, ou fecha sem texto aberto
        quebras.append((anterior, r, "abre de novo sem fechar o anterior" if r["abre"] else "fecha sem ter aberto"))
    aberto = bool(r["abre"])
    anterior = r
print(f"{len(quebras)} quebra(s) da alternância" + (f" (primeiras {LIMITE}):" if quebras else ": a sequência alterna certo"))
for ant, r, motivo in quebras[:LIMITE]:
    print(f"\n- linha {r['linha']} ({'abre' if r['abre'] else 'fecha'}: {motivo}); byte do pedaço {r['_bloco']}")
    if ant:
        print(f"    anterior ímpar, linha {ant['linha']} ({'abre' if ant['abre'] else 'fecha'}): {ant['texto']}")
    print(f"    esta,           linha {r['linha']}: {r['texto']}")
