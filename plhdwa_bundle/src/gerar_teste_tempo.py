# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Gera um arquivo grande na landing (teste de tempo de processamento)
# MAGIC Usa o mesmo gerador dos testes locais: gera no disco local da máquina e copia para a landing (S3 ou volume),
# MAGIC primeiro o DAT na pasta `pasta_dat` e por último o CTL na pasta `pasta_ctl`, como o Netezza: o CTL dispara o
# MAGIC despachante.

# COMMAND ----------

import os
import subprocess
import sys
import tempfile

NOMES = [("landing", ""), ("pasta_dat", "dados"), ("pasta_ctl", "ctl"), ("tamanho_mb", "5120"), ("cod_objeto", "9050")]
for nome, padrao in NOMES:
    dbutils.widgets.text(nome, padrao)
P = {k: dbutils.widgets.get(k) or padrao for k, padrao in NOMES}
local = tempfile.mkdtemp(prefix="plhdwa_teste_")       # o gerador escreve <local>/raw/data e <local>/raw/ctl

gerador = "../testes/gerar_arquivos_teste.py"
amostra = "../testes/fatparcela.dat"
r = subprocess.run([sys.executable, gerador, "--cenario", "tempo", "--tamanho-mb", P["tamanho_mb"],
                    "--cod-tempo", P["cod_objeto"], "--amostra", amostra, "--destino", local],
                   capture_output=True, text=True)
print(r.stdout, r.stderr)
r.check_returncode()

L = P["landing"].rstrip("/")
for origem, destino in (("raw/data", P["pasta_dat"]), ("raw/ctl", P["pasta_ctl"])):   # DAT antes, CTL por último
    for nome in sorted(os.listdir(os.path.join(local, origem))):
        dbutils.fs.cp(f"file:{os.path.join(local, origem, nome)}", f"{L}/{destino.strip('/')}/{nome}")
        print(f"copiado para {L}/{destino}/{nome}")
