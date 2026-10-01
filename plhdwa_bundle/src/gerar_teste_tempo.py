# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Gera um arquivo grande direto no volume (teste de tempo de processamento)
# MAGIC Usa o mesmo gerador dos testes locais. Grava o DAT e depois o CTL: isso já dispara o despachante.

# COMMAND ----------

import subprocess
import sys

for nome, padrao in [("landing", ""), ("tamanho_mb", "5120"), ("cod_objeto", "9050")]:
    dbutils.widgets.text(nome, padrao)
P = {k: dbutils.widgets.get(k) for k in ["landing", "tamanho_mb", "cod_objeto"]}

gerador = "../testes/gerar_arquivos_teste.py"
amostra = "../testes/fatparcela.dat"
r = subprocess.run([sys.executable, gerador, "--cenario", "tempo", "--tamanho-mb", P["tamanho_mb"],
                    "--cod-tempo", P["cod_objeto"], "--amostra", amostra, "--destino", P["landing"]],
                   capture_output=True, text=True)
print(r.stdout, r.stderr)
r.check_returncode()
