"""
Executa um arquivo .sql no Databricks, comando por comando, via SQL Statement API.
Usa a Databricks CLI já autenticada (perfil DEFAULT ou o informado em --profile).

  python3 executar_sql.py cadastro/1230_fat_parcela.sql --warehouse <id> [--profile DEFAULT]
"""
import argparse
import json
import re
import subprocess
import sys
import time


def cli(args, profile):
    r = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr)
    return json.loads(r.stdout)


def executar(sql, warehouse, profile):
    resp = cli(["api", "post", "/api/2.0/sql/statements", "--json",
                json.dumps({"statement": sql, "warehouse_id": warehouse, "wait_timeout": "50s"})], profile)
    while resp["status"]["state"] in ("PENDING", "RUNNING"):          # warehouse ligando ou consulta longa
        time.sleep(5)
        resp = cli(["api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}"], profile)
    return resp


def main():
    p = argparse.ArgumentParser()
    p.add_argument("arquivo")
    p.add_argument("--warehouse", required=True)
    p.add_argument("--profile", default="DEFAULT")
    a = p.parse_args()

    texto = open(a.arquivo, encoding="utf-8").read()
    texto = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    comandos = [c.strip() for c in re.split(r";\s*\n", texto + "\n") if c.strip()]
    for c in comandos:
        resp = executar(c, a.warehouse, a.profile)
        estado = resp["status"]["state"]
        print(f"[{estado}] {c.splitlines()[0][:100]}")
        if estado != "SUCCEEDED":
            sys.exit(resp["status"].get("error", {}).get("message", estado))
        linhas = resp.get("result", {}).get("data_array")
        if linhas:
            cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
            print("   ", cols)
            for l in linhas[:50]:
                print("   ", l)


if __name__ == "__main__":
    main()
