# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Gera um arquivo grande na landing (teste de tempo de processamento)
# MAGIC Usa o mesmo gerador dos testes locais e grava como o Netezza: primeiro o DAT na pasta `pasta_dat`, por último
# MAGIC o CTL na pasta `pasta_ctl` (o CTL dispara o despachante).
# MAGIC - landing num volume: o gerador escreve direto na landing.
# MAGIC - landing no S3: o gerador escreve na pasta `preparo` (um volume do Unity Catalog; no serverless o
# MAGIC   `dbutils.fs.cp` não lê o disco local da máquina) e o notebook copia para o bucket.

# COMMAND ----------

import os
import subprocess
import sys

NOMES = [("landing", ""), ("pasta_dat", "dados"), ("pasta_ctl", "ctl"), ("preparo", ""), ("tamanho_mb", "5120"),
         ("cod_objeto", "9050")]
for nome, padrao in NOMES:
    dbutils.widgets.text(nome, padrao)
P = {k: dbutils.widgets.get(k) or padrao for k, padrao in NOMES}
L = P["landing"].rstrip("/")
if not L.startswith("/Volumes/") and not P["preparo"].startswith("/Volumes/"):
    raise ValueError("landing no S3: informe preparo = uma pasta num volume (/Volumes/...) para gerar o arquivo")
preparo = (P["preparo"] or f"{L}/_preparo_teste").rstrip("/")   # o gerador escreve <preparo>/raw/data e raw/ctl

gerador = "../testes/gerar_arquivos_teste.py"
amostra = "../testes/fatparcela.dat"
r = subprocess.run([sys.executable, gerador, "--cenario", "tempo", "--tamanho-mb", P["tamanho_mb"],
                    "--cod-tempo", P["cod_objeto"], "--amostra", amostra, "--destino", preparo],
                   capture_output=True, text=True)
print(r.stdout, r.stderr)
r.check_returncode()

for origem, destino in (("raw/data", P["pasta_dat"]), ("raw/ctl", P["pasta_ctl"])):   # DAT antes, CTL por último
    for nome in sorted(os.listdir(os.path.join(preparo, origem))):
        dbutils.fs.mv(f"{preparo}/{origem}/{nome}", f"{L}/{destino.strip('/')}/{nome}")
        print(f"copiado para {L}/{destino}/{nome}")
dbutils.fs.rm(preparo, True)
