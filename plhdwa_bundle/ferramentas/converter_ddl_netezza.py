"""
Converte a DDL de uma EXTERNAL TABLE do Netezza em DDL de tabela Delta para o PLHDWA.

Por que existe: os metadados de carga NÃO ficam numa tabela de cadastro. Eles ficam na própria
tabela Delta de destino:
  - layout (colunas, ordem e tipos)  -> o schema da tabela
  - código do objeto                 -> tag UC  plhdwa_cod_objeto (consultável no information_schema)
  - regras de carga e de formato     -> TBLPROPERTIES  plhdwa.*
O formato do arquivo (delimitador, header, encoding, escape...) é lido do bloco USING da DDL
do Netezza, que é quem gera o .dat. Assim, os dois lados nunca ficam diferentes.

Uso (local, só Python 3, sem dependências):
  python3 converter_ddl_netezza.py exemplos/ddl_external_dwa_ccr_fatparcela.sql \
      --catalogo <catalogo_destino> --schema <schema_destino> --tabela fat_parcela \
      --cod-objeto 1230 --tipo-carga FATO --col-data-mov SK_TMO_MVM --col-instituicao SK_INS \
      [--execucao DESPACHANTE|JOB] [--tipo-job PERFORMANCE_OPTIMIZED|STANDARD] [--owner-tag PLHDWA] \
      > 1230_fat_parcela.sql
O catálogo destino precisa estar na variável catalogos_objetos do bundle (ou ser o mesmo das tabelas
operacionais), porque é lá que o motor procura a tag plhdwa_cod_objeto.
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
    p.add_argument("--cod-objeto", required=True)
    p.add_argument("--tipo-carga", choices=["FATO", "DIM"], required=True)
    p.add_argument("--col-data-mov", default="")
    p.add_argument("--col-instituicao", default="")
    p.add_argument("--formato-data-mov", default="yyyyMMdd", help="formato da coluna de data de movimento")
    p.add_argument("--timestamp-format", default="yyyy-MM-dd HH:mm[:ss]")
    p.add_argument("--date-format", default="yyyy-MM-dd")
    p.add_argument("--owner-tag", default="PLHDWA")
    p.add_argument("--execucao", choices=["DESPACHANTE", "JOB"], default="DESPACHANTE",
                   help="onde o objeto roda: dentro do despachante (threads) ou em job independente")
    p.add_argument("--tipo-job", choices=["PERFORMANCE_OPTIMIZED", "STANDARD"], default="PERFORMANCE_OPTIMIZED",
                   help="tipo do job independente (só vale com --execucao JOB)")
    a = p.parse_args()

    ddl = open(a.ddl, encoding="utf-8").read()
    ddl = re.sub(r"/\*.*?\*/", "", ddl, flags=re.S)        # remove comentários /* ... */
    ddl = ddl[:re.search(r";", ddl).end()]                  # só o CREATE (ignora o INSERT de teste)
    colunas = ler_colunas(ddl)
    opcoes = ler_opcoes_using(ddl)
    nome_origem = re.search(r"EXTERNAL\s+TABLE\s+([\w.]+)", ddl, re.I).group(1)
    alvo = f"{a.catalogo}.{a.schema}.{a.tabela}"

    if a.tipo_carga == "FATO" and not (a.col_data_mov and a.col_instituicao):
        sys.exit("FATO exige --col-data-mov e --col-instituicao")

    largura = max(len(c[0]) for c in colunas)
    defs = ",\n".join(f"  {n.ljust(largura)}  {tipo_delta(t, x)}" for n, t, x in colunas)
    cluster = f"CLUSTER BY ({a.col_data_mov}, {a.col_instituicao})" if a.tipo_carga == "FATO" else "CLUSTER BY AUTO"

    props = {
        "plhdwa.cod_objeto": a.cod_objeto,
        "plhdwa.tabela_origem": nome_origem,
        "plhdwa.tipo_carga": a.tipo_carga,
        "plhdwa.col_data_mov": a.col_data_mov,
        "plhdwa.formato_data_mov": a.formato_data_mov,
        "plhdwa.col_instituicao": a.col_instituicao,
        "plhdwa.arquivo.delimitador": opcoes["delim"],
        "plhdwa.arquivo.header": opcoes["includeheader"].lower(),
        "plhdwa.arquivo.encoding": opcoes["encoding"],
        "plhdwa.arquivo.escape": opcoes["escapechar"],
        "plhdwa.arquivo.quote": '"' if opcoes["quotedvalue"].upper() == "DOUBLE" else "",
        # NULLVALUE só entra se estiver na DDL; senão o motor usa vazio = NULL (pendência S1).
        "plhdwa.arquivo.null_value": opcoes.get("nullvalue", ""),
        "plhdwa.arquivo.timestamp_format": a.timestamp_format,
        "plhdwa.arquivo.date_format": a.date_format,
    }
    sql_str = lambda v: str(v).replace("\\", "\\\\").replace("'", "\\'")   # escapa \ e ' no literal SQL
    props_sql = ",\n".join(f"  '{k}' = '{sql_str(v)}'" for k, v in props.items())

    print(f"""-- Gerado por converter_ddl_netezza.py a partir de {a.ddl}
-- Origem Netezza: {nome_origem} · {len(colunas)} colunas
CREATE SCHEMA IF NOT EXISTS {a.catalogo}.{a.schema};

-- A ordem das colunas é a mesma do header do .dat: o motor compara header x schema.
CREATE TABLE IF NOT EXISTS {alvo} (
{defs}
)
{cluster}
COMMENT 'PLHDWA · carga Netezza {nome_origem} · objeto {a.cod_objeto} · {a.tipo_carga}'
TBLPROPERTIES (
{props_sql}
);

-- Tags: OWNER (custo/governança) · plhdwa_cod_objeto (o motor acha a tabela pelo código do CTL)
--       plhdwa_execucao / plhdwa_tipo_job (onde o objeto roda: despachante ou job independente)
ALTER TABLE {alvo} SET TAGS ('OWNER' = '{a.owner_tag}', 'plhdwa_cod_objeto' = '{a.cod_objeto}',
                             'plhdwa_execucao' = '{a.execucao}', 'plhdwa_tipo_job' = '{a.tipo_job}');
ALTER SCHEMA {a.catalogo}.{a.schema} SET TAGS ('OWNER' = '{a.owner_tag}');""")


if __name__ == "__main__":
    main()
