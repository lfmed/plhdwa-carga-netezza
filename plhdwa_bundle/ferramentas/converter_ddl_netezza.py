"""
Converte a DDL de uma EXTERNAL TABLE do Netezza em:
  1) CREATE TABLE da tabela Delta de destino (layout: colunas, ordem e tipos; CLUSTER BY; tag OWNER);
  2) MERGE da linha do objeto na ctl_parametros (tabelas operacionais do PLHDWA): destino, tipo de carga, colunas
     do REPLACE WHERE, modo de execução, formato do arquivo e limpeza.
O motor não usa tags nem TBLPROPERTIES plhdwa.* (07/10/2026): tudo o que é parâmetro fica na ctl_parametros.
O formato do arquivo (delimitador, header, encoding, escape...) é lido do bloco USING da DDL
do Netezza, que é quem gera o .dat. Assim, os dois lados nunca ficam diferentes.

Uso (local, só Python 3, sem dependências):
  python3 converter_ddl_netezza.py exemplos/ddl_external_dwa_ccr_fatparcela.sql \
      --catalogo <catalogo_destino> --schema <schema_destino> --tabela fat_parcela \
      --catalogo-controle <catalogo_plhdwa> --schema-controle <schema_plhdwa> \
      --cod-objeto 1230 --filtro-tabela SIM --nome-coluna-tempo SK_TMO_MVM --nome-coluna-instituicao NR_CPR \
      [--execucao DESPACHANTE|JOB] [--tipo-job PERFORMANCE_OPTIMIZED|STANDARD] [--owner-tag PLHDWA] \
      > 1230_fat_parcela.sql
--catalogo-controle/--schema-controle = variáveis catalogo e schema do bundle (onde está a ctl_parametros).
"""
import argparse
import re
import sys

# Tipos Netezza -> tipos Delta. TIMESTAMP vira TIMESTAMP_NTZ porque o Netezza não guarda fuso:
# assim o valor é gravado exatamente como veio, sem conversão de horário.
MAPA_TIPOS = {
    "BYTEINT": "TINYINT", "SMALLINT": "SMALLINT", "INTEGER": "INT", "INT": "INT", "BIGINT": "BIGINT",
    "REAL": "FLOAT", "FLOAT4": "FLOAT", "DOUBLE": "DOUBLE", "FLOAT8": "DOUBLE", "FLOAT": "DOUBLE",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP_NTZ", "TIME": "STRING", "BOOLEAN": "BOOLEAN",
    "CHAR": "STRING", "VARCHAR": "STRING", "NCHAR": "STRING", "NVARCHAR": "STRING",
    "CHARACTER": "STRING",
}

# Padrões do Netezza quando a opção não aparece no USING.
# Encoding 'internal' do Netezza para CHAR/VARCHAR = LATIN9 (ISO-8859-15).
PADROES_NETEZZA = {"delim": "|", "includeheader": "false", "encoding": "ISO-8859-15",
                   "escapechar": "", "quotedvalue": "NO"}


def ler_colunas(ddl):
    """Extrai (nome, tipo_netezza) na ordem em que aparecem entre os parênteses da DDL."""
    corpo = ddl[ddl.index("(") + 1: re.search(r"\)\s*ON\s", ddl, re.I).start()]
    colunas = []
    for linha in corpo.splitlines():
        linha = linha.strip().rstrip(",").strip()
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z0-9]+)\s*(\(\s*\d+\s*(?:,\s*\d+\s*)?\))?", linha)
        if m:
            colunas.append((m.group(1).upper(), m.group(2).upper(), (m.group(3) or "").replace(" ", "")))
    return colunas


def tipo_delta(tipo, args):
    if tipo in ("NUMERIC", "DECIMAL"):
        return f"DECIMAL{args or '(38,0)'}"
    if tipo not in MAPA_TIPOS:
        sys.exit(f"Tipo Netezza não mapeado: {tipo}")
    return MAPA_TIPOS[tipo]


def ler_opcoes_using(ddl):
    """Lê o bloco USING (...) ignorando linhas comentadas com --."""
    m = re.search(r"USING\s*\((.*)\)\s*;", ddl, re.I | re.S)
    bloco = "\n".join(l for l in (m.group(1) if m else "").splitlines() if not l.strip().startswith("--"))
    opcoes = dict(PADROES_NETEZZA)
    for chave, valor in re.findall(r"([A-Za-z]+)\s*(?:\(\s*)?'([^']*)'", bloco):
        opcoes[chave.lower()] = valor
    return opcoes


def main():
    p = argparse.ArgumentParser()
    p.add_argument("ddl")
    p.add_argument("--catalogo", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--tabela", required=True)
    p.add_argument("--catalogo-controle", required=True, help="catálogo das tabelas operacionais (variável catalogo)")
    p.add_argument("--schema-controle", required=True, help="schema das tabelas operacionais (variável schema)")
    p.add_argument("--cod-objeto", required=True)
    p.add_argument("--filtro-tabela", choices=["SIM", "NAO"], required=True,
                   help="SIM = REPLACE WHERE pela data de movimento (+ nr_cpr do CTL) · NAO = substitui a tabela inteira")
    p.add_argument("--nome-coluna-tempo", default="", help="SIM: coluna da data de movimento")
    p.add_argument("--nome-coluna-instituicao", default="NR_CPR",
                   help="SIM: coluna comparada com o nr_cpr do CTL (REPLACE WHERE). Padrão NR_CPR")
    p.add_argument("--timestamp-format", default="yyyy-MM-dd HH:mm[:ss]")
    p.add_argument("--date-format", default="yyyy-MM-dd")
    p.add_argument("--owner-tag", default="PLHDWA")
    p.add_argument("--execucao", choices=["DESPACHANTE", "JOB"], default="DESPACHANTE",
                   help="onde o objeto roda: dentro do despachante (threads) ou em job independente")
    p.add_argument("--tipo-job", choices=["PERFORMANCE_OPTIMIZED", "STANDARD"], default="PERFORMANCE_OPTIMIZED",
                   help="tipo do job independente (só vale com --execucao JOB)")
    p.add_argument("--limpeza", choices=["true", "false"],
                   help="sobrepõe a variável limpeza_dat só para este objeto (coluna limpeza)")
    p.add_argument("--limpeza-modo", choices=["CLIENTE", "AUTO", "DIRETA", "PARALELA", "MULTILINHA"],
                   help="CLIENTE (padrão do motor) = tmp em partes paralelas + multiLine; AUTO detecta por arquivo; "
                        "DIRETA = regex na própria leitura, sem tmp; PARALELA = tmp em partes sem multiLine; "
                        "MULTILINHA = 1 arquivo + multiLine (não usar em arquivo grande)")
    p.add_argument("--limpeza-quote", help="aspas dos textos no arquivo limpo (padrão do motor: aspas simples)")
    a = p.parse_args()

    ddl = open(a.ddl, encoding="utf-8").read()
    ddl = re.sub(r"/\*.*?\*/", "", ddl, flags=re.S)        # remove comentários /* ... */
    ddl = ddl[:re.search(r";", ddl).end()]                  # só o CREATE (ignora o INSERT de teste)
    colunas = ler_colunas(ddl)
    opcoes = ler_opcoes_using(ddl)
    nome_origem = re.search(r"EXTERNAL\s+TABLE\s+([\w.]+)", ddl, re.I).group(1)
    alvo = f"{a.catalogo}.{a.schema}.{a.tabela}"

    filtro = a.filtro_tabela == "SIM"
    if filtro and not a.nome_coluna_tempo:
        sys.exit("--filtro-tabela SIM exige --nome-coluna-tempo")
    for nome, valor in (("--nome-coluna-tempo", a.nome_coluna_tempo), ("--nome-coluna-instituicao", a.nome_coluna_instituicao)):
        if filtro and valor.lower() not in {c[0].lower() for c in colunas}:
            sys.exit(f"{nome} = {valor} não é coluna da DDL")

    largura = max(len(c[0]) for c in colunas)
    defs = ",\n".join(f"  {n.ljust(largura)}  {tipo_delta(t, x)}" for n, t, x in colunas)
    cluster = f"CLUSTER BY ({a.nome_coluna_tempo}, {a.nome_coluna_instituicao})" if filtro else "CLUSTER BY AUTO"
    # Liquid clustering exige estatísticas nas colunas do CLUSTER BY, e o Delta só as coleta nas 32 primeiras
    # colunas: as do filtro entram na lista mesmo se estiverem depois (ex.: NR_CPR é a 57ª no FATPARCELA).
    propriedades = ""
    if filtro:
        nomes = [c[0] for c in colunas]
        stats = nomes[:32] + [n for n in nomes if n.lower() in (a.nome_coluna_tempo.lower(),
                                                              a.nome_coluna_instituicao.lower()) and n not in nomes[:32]]
        propriedades = f"\nTBLPROPERTIES ('delta.dataSkippingStatsColumns' = '{','.join(stats)}')"

    parametros = {   # colunas da ctl_parametros (None = NULL = padrão do motor)
        "cod_objeto": int(a.cod_objeto),
        "catalogo_destino": a.catalogo, "schema_destino": a.schema, "tabela_destino": a.tabela,
        "filtrotabela": a.filtro_tabela,
        "nomecolunatempo": a.nome_coluna_tempo or None,
        "nomecolunainstituicao": a.nome_coluna_instituicao if filtro else None,
        "execucao": a.execucao, "tipo_job": a.tipo_job, "ativo": True,
        "delimitador": opcoes["delim"],
        "encoding": opcoes["encoding"],
        "header": opcoes["includeheader"].lower() == "true",
        "escape": opcoes["escapechar"] or None,
        "quote": '"' if opcoes["quotedvalue"].upper() == "DOUBLE" else None,
        # NULLVALUE só entra se estiver na DDL; senão o motor usa vazio = NULL (pendência S1).
        "null_value": opcoes.get("nullvalue") or None,
        "date_format": a.date_format,
        "timestamp_format": a.timestamp_format,
        # limpeza do DAT (sem estas, vale a variável limpeza_dat e os padrões do motor)
        "limpeza": None if a.limpeza is None else a.limpeza == "true",
        "limpeza_modo": a.limpeza_modo,
        "limpeza_quote": a.limpeza_quote,
        "tabela_origem": nome_origem,
    }
    controle = f"{a.catalogo_controle}.{a.schema_controle}.ctl_parametros"
    print(f"""-- Gerado por converter_ddl_netezza.py a partir de {a.ddl}
-- Origem Netezza: {nome_origem} · {len(colunas)} colunas
CREATE SCHEMA IF NOT EXISTS {a.catalogo}.{a.schema};

-- A ordem das colunas é a mesma do header do .dat: o motor compara header x schema.
CREATE TABLE IF NOT EXISTS {alvo} (
{defs}
)
{cluster}{propriedades}
COMMENT 'PLHDWA · carga Netezza {nome_origem} · objeto {a.cod_objeto} · filtrotabela {a.filtro_tabela}';

-- Tag OWNER (custo/governança). Os parâmetros do objeto ficam na ctl_parametros, abaixo.
ALTER TABLE {alvo} SET TAGS ('OWNER' = '{a.owner_tag}');
ALTER SCHEMA {a.catalogo}.{a.schema} SET TAGS ('OWNER' = '{a.owner_tag}');

{merge_parametros(controle, parametros)}""")


def literal_sql(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"   # escapa \ e ' no literal SQL


def merge_parametros(controle, parametros):
    """MERGE da linha do objeto na ctl_parametros: cria ou atualiza (a coluna observacao não é tocada)."""
    origem = ",\n         ".join(f"{literal_sql(v)} AS {k}" for k, v in parametros.items())
    cols = list(parametros) + ["alterado_por", "alterado_em"]
    atualiza = ",\n  ".join(f"{k} = s.{k}" for k in cols if k != "cod_objeto")
    return f"""-- Parâmetros do objeto (cria ou atualiza a linha; observacao é preservada)
MERGE INTO {controle} t
USING (SELECT {origem},
         current_user() AS alterado_por, current_timestamp() AS alterado_em) s
ON t.cod_objeto = s.cod_objeto
WHEN MATCHED THEN UPDATE SET
  {atualiza}
WHEN NOT MATCHED THEN INSERT ({", ".join(cols)})
  VALUES ({", ".join("s." + k for k in cols)});"""


if __name__ == "__main__":
    main()
