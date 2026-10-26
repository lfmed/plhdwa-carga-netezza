"""
PLHDWA · limpeza do DAT no modo SINGLE (limpeza_modo = 'SINGLE' na ctl_parametros).

Pedido do cliente em 26/10/2026: reproduz exatamente o código enviado por ele. Fica em arquivo separado para o cliente
manter: o motor (plhdwa_motor.py) só chama limpar() antes da carga e opcoes_leitura() na leitura; o resto da carga
(conferência do header, regras de data e nr_cpr, REPLACE WHERE, controle no Db2, log, tmp apagado no fim) é o mesmo dos
outros modos.

  1) limpar: lê o DAT linha a linha (spark.read.text, só UTF-8), aplica as 4 regex e grava o arquivo limpo em pasta_tmp,
     em várias partes, em paralelo;
  2) opcoes_leitura: a carga lê as partes como CSV com header, delimitador, multiLine e aspas simples, no modo padrão do
     Spark (PERMISSIVE). A única diferença para o código do cliente é o schema da tabela na leitura (o motor grava com os
     tipos da tabela); valor que não converte vira nulo, como no PERMISSIVE.

Atenção (comportamento do próprio código, medido nos testes do interno; decisão 39 da especificação):
  - header = true com várias partes: o Spark trata a 1ª linha de CADA parte como cabeçalho e a descarta;
  - registro com quebra de linha na fronteira entre duas partes fica cortado (no PERMISSIVE, sem erro);
  - spark.read.text só lê UTF-8 (o encoding da ctl_parametros não vale neste modo);
  - \\| (barra + barra vertical) é apagado: o caractere | some do texto (no modo CLIENTE ele é mantido).
"""
from pyspark.sql.functions import col, regexp_replace

# As 4 regex do cliente, na ordem dele
REGEX = [
    (r'\\(?![\\\'"|nrt])', ''),   # barra que não escapa \ ' " | n r t: sai
    (r'\\\|', ''),                # \| sai
    (r'\\null', ''),              # \null vira vazio
    (r"[\r\n]+", ''),             # CR/LF dentro do valor sai
]


def limpar(spark, origem, destino):
    """Passo 1 do código do cliente: DAT (origem) -> arquivo limpo em destino, em várias partes."""
    df = spark.read.option('wholeFile', True).text(origem)   # 'wholeFile' não existe no .text(): o Spark lê linha a linha
    for rx, novo in REGEX:
        df = df.withColumn('value', regexp_replace(col('value'), rx, novo))
    df.write.mode('overwrite').text(destino)


def opcoes_leitura(delimitador='|', aspas="'"):
    """Passo 2 do código do cliente: opções da leitura do arquivo limpo (sem escape explícito e sem mode)."""
    return {'header': 'true',        # primeira linha -> cabeçalho
            'delimiter': delimitador,  # separador de colunas
            'multiLine': 'true',     # permite quebras de linha dentro de um registro
            'quote': aspas}          # campos são delimitados por aspas simples
