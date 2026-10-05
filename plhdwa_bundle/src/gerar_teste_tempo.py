# Databricks notebook source
# MAGIC %md
# MAGIC # PLHDWA · Gera um arquivo grande na landing (teste de tempo de processamento)
# MAGIC Usa o mesmo gerador dos testes locais. Grava o DAT e depois o CTL, como o Netezza: o CTL já dispara o despachante.
# MAGIC - `landing_tipo = VOLUME`: o gerador escreve direto no volume.
# MAGIC - `landing_tipo = S3`: o gerador escreve no disco local da máquina e o notebook copia para o bucket
# MAGIC   (`dbutils.fs.cp`, pela external location), primeiro o DAT e por último o CTL.

# COMMAND ----------

import os
import subprocess
import sys
import tempfile

for nome, padrao in [("landing", ""), ("landing_tipo", "S3"), ("tamanho_mb", "5120"), ("cod_objeto", "9050")]:
    dbutils.widgets.text(nome, padrao)
P = {k: dbutils.widgets.get(k) for k in ["landing", "landing_tipo", "tamanho_mb", "cod_objeto"]}
s3 = P["landing_tipo"].upper() == "S3"
destino = tempfile.mkdtemp(prefix="plhdwa_teste_") if s3 else P["landing"]

gerador = "../testes/gerar_arquivos_teste.py"
amostra = "../testes/fatparcela.dat"
r = subprocess.run([sys.executable, gerador, "--cenario", "tempo", "--tamanho-mb", P["tamanho_mb"],
                    "--cod-tempo", P["cod_objeto"], "--amostra", amostra, "--destino", destino],
                   capture_output=True, text=True)
print(r.stdout, r.stderr)
r.check_returncode()

if s3:
    for sub in ("raw/data", "raw/ctl"):                  # DAT antes, CTL por último (o CTL dispara o despachante)
        for nome in sorted(os.listdir(os.path.join(destino, sub))):
            dbutils.fs.cp(f"file:{os.path.join(destino, sub, nome)}", f"{P['landing'].rstrip('/')}/{sub}/{nome}")
            print(f"copiado para {P['landing']}/{sub}/{nome}")
