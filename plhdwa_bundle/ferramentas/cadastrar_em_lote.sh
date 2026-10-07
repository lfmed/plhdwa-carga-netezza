#!/usr/bin/env bash
# Cadastra vários objetos de uma vez: converte cada DDL do Netezza e (opcionalmente) cria a tabela destino e
# grava a linha do objeto na ctl_parametros.
#   ./cadastrar_em_lote.sh <objetos.csv> <pasta_das_ddls> <catalogo> <schema> <catalogo_plhdwa> <schema_plhdwa> [warehouse_id]
# <catalogo>/<schema> = onde ficam as tabelas destino · <catalogo_plhdwa>/<schema_plhdwa> = variáveis catalogo e
# schema do bundle (onde está a ctl_parametros)
# objetos.csv (com cabeçalho): cod;ddl;tabela;tipo;col_data;col_inst[;execucao;tipo_job]
#   col_data/col_inst vazios para DIM · execucao = DESPACHANTE (padrão) | JOB · tipo_job = PERFORMANCE_OPTIMIZED | STANDARD
# Sem warehouse_id, só gera os SQLs em ferramentas/cadastro/ (para revisar antes de criar).
# Exemplo: ./cadastrar_em_lote.sh exemplos/objetos.csv exemplos <catalogo_destino> <schema_destino> <catalogo_plhdwa> plhdwa <warehouse_id>
set -euo pipefail
CSV=${1:?objetos.csv}; DDLS=${2:?pasta das DDLs}; CAT=${3:?catalogo}; SCH=${4:?schema}
CAT_C=${5:?catalogo_plhdwa}; SCH_C=${6:?schema_plhdwa}; WH=${7:-}
AQUI=$(cd "$(dirname "$0")" && pwd)
SAIDA="$AQUI/cadastro"; mkdir -p "$SAIDA"

tail -n +2 "$CSV" | while IFS=';' read -r cod ddl tab tipo cdata cinst execucao tipo_job; do
  [ -z "$cod" ] && continue
  extra=(--execucao "${execucao:-DESPACHANTE}" --tipo-job "${tipo_job:-PERFORMANCE_OPTIMIZED}")
  [ "$tipo" = "FATO" ] && extra+=(--col-data-mov "$cdata" --col-instituicao "$cinst")
  python3 "$AQUI/converter_ddl_netezza.py" "$DDLS/$ddl" --catalogo "$CAT" --schema "$SCH" \
          --tabela "$tab" --catalogo-controle "$CAT_C" --schema-controle "$SCH_C" --cod-objeto "$cod" --tipo-carga "$tipo" "${extra[@]}" > "$SAIDA/${cod}_${tab}.sql"
  echo "gerado: cadastro/${cod}_${tab}.sql"
  if [ -n "$WH" ]; then
    python3 "$AQUI/executar_sql.py" "$SAIDA/${cod}_${tab}.sql" --warehouse "$WH" | grep -E "FAILED|^\[" | sed 's/^/   /'
  fi
done
