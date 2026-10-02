"""
PLHDWA · motor de carga Netezza -> Databricks (biblioteca única).

Fluxo de um arquivo (processar_arquivo), igual no despachante (threads) e nos jobs independentes:
  CTL válido? -> DAT existe e não está vazio? -> tabela destino pela tag do código
  -> controle: EM_PROCESSAMENTO -> sequência -> pré-checagem -> leitura + validação + carga atômica (1 commit)
  -> controle: CONCLUIDO (síncrono) -> apaga CTL e DAT -> log CONCLUIDO
  Em erro: controle: ERRO (com a mensagem) + log ERRO_* + arquivos para error/ + objeto travado.

Modelo v2 (01/10/2026):
  - sem filas por tamanho: cada objeto diz onde roda, por tag na tabela destino
      plhdwa_execucao = DESPACHANTE (padrão) | JOB      plhdwa_tipo_job = PERFORMANCE_OPTIMIZED | STANDARD
  - ctl_execucao guarda só os limites de simultâneos de cada modo
  - cada execução do despachante é um LOTE (lote_id); cada arquivo sensibiliza a controle sozinho
  - metadados do objeto ficam na própria tabela destino (tags + TBLPROPERTIES plhdwa.*)
  - todo evento vai para log_carga_evento (só INSERT: vários workers ao mesmo tempo sem conflito)
"""
import re
import threading
import time
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Nome dos arquivos: <cod>_<tabela>_<data_mov yyyyMMdd>_<data_geracao yyyyMMddHHmmss>.<dat|CTL>
RX_NOME = re.compile(r"^(\d+)_(.+)_(\d{8})_(\d{14})\.(dat|CTL)$")
MODOS = ("DESPACHANTE", "JOB")

# Contexto da execução (preenchido por iniciar()). É lido por todas as threads, e só o cache muda.
CTX = {}
_cache_meta = {}
_trava_cache = threading.Lock()


class ErroCarga(Exception):
    """Erro de negócio ou de processamento. tipo = status gravado no log (ERRO_*)."""
    def __init__(self, tipo, mensagem, mover=True):
        super().__init__(f"{tipo}: {mensagem}")
        self.tipo, self.mensagem, self.mover = tipo, mensagem[:3000], mover


# =============================================================================
# 1. Parâmetros
# =============================================================================
# Todos os parâmetros do motor, num lugar só. Os valores chegam como parâmetros do job (resources/jobs.yml),
# que por sua vez vêm das variáveis do bundle (databricks.yml). O padrão abaixo só vale se o job não mandar nada.
PARAMETROS = {
    # --- onde ficam as coisas ---
    "catalogo":           ("",                 "catálogo das tabelas operacionais do PLHDWA (log, lotes, controle)"),
    "schema":             ("",                 "schema das tabelas operacionais"),
    "catalogos_objetos":  ("",                 "catálogos onde procurar as tabelas destino (pela tag plhdwa_cod_objeto), "
                                               "separados por vírgula; vazio = o mesmo de 'catalogo'"),
    "landing":            ("",                 "pasta raiz da landing (volume): raw/data, raw/ctl e error"),
    "fuso_horario":       ("America/Sao_Paulo", "fuso das datas de geração dos nomes de arquivo e dos horários do log"),
    # --- controle (Db2) ---
    "db2_modo":           ("DB2",              "DB2 (controle no Db2 do cliente) ou SIMULADO (Lakebase, só para testes)"),
    "db2_host":           ("",                 "host do Db2"),
    "db2_porta":          ("50000",            "porta do Db2"),
    "db2_banco":          ("",                 "nome do banco (DATABASE) no Db2"),
    "db2_ssl":            ("true",             "true = conexão com SSL (SECURITY=SSL)"),
    "db2_certificado":    ("",                 "(opcional) arquivo da CA do Db2 num volume (SSLServerCertificate)"),
    "db2_extra":          ("",                 "(opcional) opções extras da conexão ibm_db: CHAVE=valor;CHAVE=valor;"),
    "db2_secret_scope":   ("plhdwa",           "secret scope com o usuário e a senha do Db2"),
    "db2_chave_usuario":  ("db2_usuario",      "chave do usuário no secret scope"),
    "db2_chave_senha":    ("db2_senha",        "chave da senha no secret scope"),
    "db2_tabela":         ("",                 "schema.tabela da controle no Db2 (troca o {tabela} do comando)"),
    "db2_comando":        ("02_db2_controle_merge.sql", "arquivo em sql/ com o comando que grava a controle"),
    "lakebase_instance":  ("",                 "instância Lakebase que simula o Db2 Controle"),
    "lakebase_database":  ("plhdwa",           "banco (database) da controle simulada no Lakebase"),
    "lakebase_tabela":    ("controle.db2_controle", "schema.tabela da controle simulada no Lakebase"),
    "db2_max_conexoes":   ("8",                "conexões simultâneas com o Db2 por processo (as threads compartilham)"),
    "db2_tentativas":     ("3",                "tentativas de gravar na controle antes de ERRO_DB2"),
    "db2_espera_base_s":  ("30",               "espera antes da 2ª tentativa (dobra a cada tentativa)"),
    # --- leitura ---
    "precheck_linhas":    ("10000",            "linhas lidas na pré-checagem (header, tipos e detector de formato)"),
    "precheck_min_mb":    ("256",              "a partir deste tamanho, todo arquivo faz a pré-checagem completa"),
    # --- despachante ---
    "ciclo_s":            ("5",                "segundos entre releituras da pasta, dos limites e dos jobs"),
    "max_horas":          ("10",               "duração máxima de um lote (segurança)"),
    "max_threads":        ("64",               "teto físico de threads do despachante (o limite efetivo é ctl_execucao)"),
    "tipo_job":           ("PERFORMANCE_OPTIMIZED", "tipo do job atual (informativo no log)"),
    # --- job independente ---
    "ctl":                ("",                 "caminho do CTL inicial (passado pelo despachante)"),
    "modo":               ("JOB",              "modo de execução do arquivo (passado pelo despachante)"),
    "lote_id":            ("0",                "lote (execução do despachante) que disparou o job"),
    "max_arquivos":       ("20",               "o job segue com o objeto até N arquivos e devolve a vaga"),
    "max_minutos":        ("60",               "... ou até M minutos"),
    # --- vigia ---
    "despachante_job_id": ("0",              "job do despachante (o vigia dispara se houver CTL parado)"),
    "vigia_parado_min":   ("5",                "CTL pendente há mais de N minutos sem despachante rodando = disparar"),
    # --- referências dinâmicas do Jobs ---
    "job_id":             ("0",                "{{job.id}}"),
    "job_run_id":         ("0",                "{{job.run_id}}: liga o log ao custo (system.billing.usage)"),
}


# Parâmetros que todo notebook do motor recebe (os específicos de cada um vêm somados a esta lista)
COMUNS = ["catalogo", "schema", "catalogos_objetos", "landing", "fuso_horario",
          "db2_modo", "db2_host", "db2_porta", "db2_banco", "db2_ssl", "db2_certificado", "db2_extra",
          "db2_secret_scope",
          "db2_chave_usuario", "db2_chave_senha", "db2_tabela", "db2_comando", "db2_max_conexoes", "db2_tentativas",
          "db2_espera_base_s",
          "lakebase_instance", "lakebase_database", "lakebase_tabela",
          "precheck_linhas", "precheck_min_mb", "tipo_job", "job_id", "job_run_id"]


def ler_parametros(dbutils, nomes):
    """Cria os widgets dos parâmetros pedidos (com o padrão da tabela PARAMETROS) e devolve {nome: valor}."""
    for n in nomes:
        dbutils.widgets.text(n, PARAMETROS[n][0])
    return {n: dbutils.widgets.get(n) for n in nomes}


def iniciar(spark, dbutils, params):
    """Guarda os parâmetros no contexto. Quem não veio em params fica com o padrão da tabela PARAMETROS."""
    CTX.update({n: v[0] for n, v in PARAMETROS.items()})
    # parâmetro vazio não apaga um padrão (ex.: precheck_linhas = "" continua 10000)
    CTX.update({k: v for k, v in params.items() if v not in ("", None) or not PARAMETROS.get(k, ("",))[0]})
    CTX["spark"], CTX["dbutils"] = spark, dbutils
    CTX["job_id"] = int(params.get("job_id") or 0)
    CTX["job_run_id"] = int(params.get("job_run_id") or 0)
    # lote = execução do despachante. Nos jobs independentes, chega por parâmetro.
    CTX["lote_id"] = int(params.get("lote_id") or CTX["job_run_id"] or 0)
    # Datas de geração vêm em horário de Brasília: a sessão inteira trabalha nesse fuso.
    spark.conf.set("spark.sql.session.timeZone", CTX["fuso_horario"])
    CTX["catalogos_objetos"] = [c.strip() for c in (CTX["catalogos_objetos"] or CTX["catalogo"]).split(",") if c.strip()]
    for c in CTX["catalogos_objetos"] + [CTX["lakebase_tabela"], CTX["db2_tabela"] or "x"]:
        if not re.fullmatch(r"[A-Za-z0-9_.]+", c):   # vão em SQL montado por texto: só nomes simples
            raise ValueError(f"nome inválido em parâmetro: {c!r}")


def fq(nome):
    return f"{CTX['catalogo']}.{CTX['schema']}.{nome}"


def pasta(sub):
    return f"{CTX['landing'].rstrip('/')}/{sub}"


def sql(texto, **args):
    return CTX["spark"].sql(texto, args=args or None)


# =============================================================================
# 2. Log de eventos (append-only)
# =============================================================================
_SCHEMA_EVENTO = ("ts_s DOUBLE, job_id BIGINT, job_run_id BIGINT, lote_id BIGINT, fila STRING, tipo_job STRING, cod_objeto INT, "
                  "tabela_destino STRING, arquivo_ctl STRING, arquivo_dat STRING, tamanho_bytes BIGINT, "
                  "data_referencia INT, dg STRING, instituicoes STRING, status STRING, inicio_s DOUBLE, "
                  "fim_s DOUBLE, qtd_inserida BIGINT, qtd_removida BIGINT, versao_delta BIGINT, mensagem STRING")


def _linha_evento(arq, status, c):
    return {
        "ts_s": c.get("ts"),                        # horário real da etapa (None = agora)
        "job_id": CTX["job_id"], "lote_id": CTX["lote_id"],
        "job_run_id": c.get("job_run_id") or CTX["job_run_id"],   # ROTEADO de job: a execução disparada
        "fila": c.get("fila"), "tipo_job": c.get("tipo_job"),          # fila = modo de execução
        "cod_objeto": arq.get("cod") if arq else c.get("cod"),
        "tabela_destino": c.get("tabela"),
        "arquivo_ctl": arq.get("nome_ctl") if arq else None,
        "arquivo_dat": arq.get("nome_dat") if arq else None,
        "tamanho_bytes": c.get("tamanho"),
        "data_referencia": int(arq["dt_mov"]) if arq else None,
        "dg": arq.get("dt_ger") if arq else None,
        "instituicoes": c.get("instituicoes"),
        "status": status,
        "inicio_s": c.get("inicio"), "fim_s": c.get("fim"),
        "qtd_inserida": c.get("qtd_inserida"), "qtd_removida": c.get("qtd_removida"),
        "versao_delta": c.get("versao"), "mensagem": c.get("mensagem"),
    }


def eventos(lista):
    """Grava VÁRIOS eventos numa única escrita Delta: [(arq, status, campos), ...].
    Cada gravação custa ~1–2 s; juntar os eventos de um arquivo reduz o custo fixo por arquivo.
    'ts' em campos guarda o horário real de cada etapa (as datas são convertidas no Spark, não no Python)."""
    if not lista:
        return
    df = CTX["spark"].createDataFrame([_linha_evento(a, s, c) for a, s, c in lista], _SCHEMA_EVENTO)
    (df.select(F.coalesce(F.timestamp_seconds("ts_s"), F.current_timestamp()).alias("evento_ts"), "job_id",
               "job_run_id", "fila", "tipo_job", "cod_objeto", "tabela_destino", "arquivo_ctl", "arquivo_dat",
               "tamanho_bytes", "data_referencia", F.to_timestamp("dg", "yyyyMMddHHmmss").alias("data_geracao"),
               "instituicoes", "status", F.timestamp_seconds("inicio_s").alias("inicio"),
               F.timestamp_seconds("fim_s").alias("fim"), "qtd_inserida", "qtd_removida", "versao_delta", "mensagem",
               F.expr("current_user()").alias("usuario"), "lote_id")
       .write.mode("append").option("mergeSchema", "false").saveAsTable(fq("log_carga_evento")))
    for a, s, c in lista:
        print(f"[{time.strftime('%H:%M:%S')}] {s:15} {a.get('nome_ctl') if a else c.get('cod')} "
              f"{(c.get('mensagem') or '')[:150]}")


def evento(arq, status, **c):
    eventos([(arq, status, c)])


def _horario_ciclo(ts):
    return F.timestamp_seconds(F.lit(ts)) if ts else F.current_timestamp()


def ciclo(linhas, ts=None):
    """Foto do backlog por modo de execução em cada ciclo do despachante (alimenta o dashboard)."""
    if not linhas:
        return
    df = CTX["spark"].createDataFrame(
        linhas, "job_run_id BIGINT, fila STRING, pendentes_qtd INT, pendentes_bytes BIGINT, em_andamento INT, max_simult INT")
    (df.select(_horario_ciclo(ts).alias("ciclo_ts"), "*").write.mode("append")
       .saveAsTable(fq("log_ciclo_despachante")))


def jobs_ativos(linhas, ts=None):
    """Foto dos jobs independentes ativos no ciclo: [(cod, run_id, tipo_job, estado_run, inicio_epoch), ...].
    Mesmo ciclo_ts da foto de log_ciclo_despachante (o cartão conta; esta tabela lista)."""
    if not linhas:
        return
    df = CTX["spark"].createDataFrame(
        [(int(CTX["lote_id"]), c, r, t, e, i) for c, r, t, e, i in linhas],
        "lote_id BIGINT, cod_objeto INT, job_run_id BIGINT, tipo_job STRING, estado_run STRING, inicio_s DOUBLE")
    (df.select(_horario_ciclo(ts).alias("ciclo_ts"), "lote_id", "cod_objeto", "job_run_id", "tipo_job", "estado_run",
               F.timestamp_seconds("inicio_s").alias("inicio_run"))
       .write.mode("append").saveAsTable(fq("log_job_ativo")))


def lote(status, pendentes=None, mensagem=None):
    """Início e fim do lote (execução do despachante) em log_lote."""
    (CTX["spark"].createDataFrame([(CTX["lote_id"], status, pendentes, mensagem)],
                                  "lote_id BIGINT, status STRING, pendentes INT, mensagem STRING")
     .select("lote_id", F.current_timestamp().alias("evento_ts"), "status", "pendentes", "mensagem")
     .write.mode("append").saveAsTable(fq("log_lote")))
    print(f"[{time.strftime('%H:%M:%S')}] LOTE {CTX['lote_id']} {status} {mensagem or ''}")


def fechar_lotes_orfaos():
    """Lotes que começaram e não terminaram (despachante caiu ou foi cancelado). Como só existe 1 despachante
    rodando por vez, qualquer outro lote aberto está morto: registra INTERROMPIDO para ele sair de "em andamento"."""
    orfaos = [r.lote_id for r in sql(f"""
        SELECT lote_id FROM {fq('log_lote')} GROUP BY lote_id
        HAVING count_if(status = 'INICIADO') > 0 AND count_if(status IN ('FINALIZADO', 'INTERROMPIDO')) = 0
           AND lote_id <> :atual""", atual=int(CTX["lote_id"])).collect()]
    if orfaos:
        (CTX["spark"].createDataFrame([(int(l),) for l in orfaos], "lote_id BIGINT")
         .select("lote_id", F.current_timestamp().alias("evento_ts"), F.lit("INTERROMPIDO").alias("status"),
                 F.lit(None).cast("int").alias("pendentes"),
                 F.lit(f"execução terminou sem registrar o fim (fechado pelo lote {CTX['lote_id']})").alias("mensagem"))
         .write.mode("append").saveAsTable(fq("log_lote")))
        print(f"lotes órfãos marcados como INTERROMPIDO: {orfaos}")


# =============================================================================
# 3. Arquivos
# =============================================================================
def ler_nome(nome_ctl):
    """Quebra o nome do CTL nas partes. Devolve None se o nome não seguir o padrão."""
    m = RX_NOME.match(nome_ctl)
    if not m or m.group(5) != "CTL":
        return None
    base = nome_ctl[:-4]
    return {"cod": int(m.group(1)), "tabela_nome": m.group(2), "dt_mov": m.group(3), "dt_ger": m.group(4),
            "base": base, "nome_ctl": nome_ctl, "nome_dat": base + ".dat",
            "ctl": pasta(f"raw/ctl/{nome_ctl}"), "dat": pasta(f"raw/data/{base}.dat")}


def _ls(caminho):
    try:
        return CTX["dbutils"].fs.ls(caminho)
    except Exception:
        return []


def tamanhos_dat():
    """Tamanho em bytes de cada DAT, direto da listagem (sem abrir o arquivo)."""
    return {f.name: f.size for f in _ls(pasta("raw/data"))}


def status_por_arquivo(nomes):
    if not nomes:
        return {}
    df = CTX["spark"].table(fq("v_status_arquivo")).filter(F.col("arquivo_ctl").isin(list(nomes)))
    return {r.arquivo_ctl: r.status for r in df.select("arquivo_ctl", "status").collect()}


def objetos_travados():
    return {r.cod_objeto for r in CTX["spark"].table(fq("v_objeto_travado")).select("cod_objeto").collect()}


def pendentes(cod=None):
    """CTLs em raw/ctl ainda não concluídos, de objetos não travados, agrupados por objeto e em ordem de geração.
    cod: só os desse objeto (usado pelo job independente para seguir com o próximo arquivo do mesmo objeto)."""
    arqs = [a for a in (ler_nome(f.name) for f in _ls(pasta("raw/ctl"))) if a and (cod is None or a["cod"] == cod)]
    st = status_por_arquivo([a["nome_ctl"] for a in arqs])
    travados = objetos_travados()
    grupos = {}
    for a in arqs:
        if st.get(a["nome_ctl"]) == "CONCLUIDO":
            try:
                apagar_par(a)          # sobra de uma execução que caiu entre o log e a remoção
            except Exception:
                pass
            continue
        if a["cod"] in travados:
            continue
        grupos.setdefault(a["cod"], []).append(a)
    return {cod: sorted(v, key=lambda a: a["dt_ger"]) for cod, v in grupos.items()}


def apagar_par(arq):
    """Sucesso: apaga o CTL primeiro. Se cair no meio, sobra só um DAT órfão (inofensivo)."""
    CTX["dbutils"].fs.rm(arq["ctl"])
    try:
        CTX["dbutils"].fs.rm(arq["dat"])
    except Exception:
        pass


def mover_para_erro(arq):
    """Erro de negócio: move o CTL primeiro, depois o DAT (se existir)."""
    for origem, nome in ((arq["ctl"], arq["nome_ctl"]), (arq["dat"], arq["nome_dat"])):
        try:
            CTX["dbutils"].fs.mv(origem, pasta(f"error/{nome}"))
        except Exception:
            pass   # DAT ausente (ERRO_PAR)


# =============================================================================
# 4. Metadados (vêm da tabela destino; não existe tabela de cadastro)
# =============================================================================
_mapa = {"quando": 0, "objetos": {}}


def mapa_objetos(validade_s=60):
    """UMA consulta traz todas as tabelas PLHDWA e as tags de cada uma:
       plhdwa_cod_objeto (obrigatória) · plhdwa_execucao (DESPACHANTE | JOB) · plhdwa_tipo_job.
    Evita uma consulta por objeto: com centenas de objetos pendentes, o ciclo do despachante ficaria lento."""
    with _trava_cache:
        if time.time() - _mapa["quando"] < validade_s:
            return _mapa["objetos"]
    # um catálogo por vez (cada catálogo tem o seu information_schema); parâmetro catalogos_objetos
    linhas = []
    for cat in CTX["catalogos_objetos"]:
        linhas += sql(f"""SELECT concat_ws('.', catalog_name, schema_name, table_name) AS t, tag_name, tag_value
                          FROM `{cat}`.information_schema.table_tags
                          WHERE tag_name IN ('plhdwa_cod_objeto', 'plhdwa_execucao', 'plhdwa_tipo_job')""").collect()
    tags = {}
    for r in linhas:
        tags.setdefault(r.t, {})[r.tag_name] = r.tag_value
    objetos = {}
    for t, tg in tags.items():
        cod = tg.get("plhdwa_cod_objeto", "")
        if not cod.isdigit():
            continue
        o = objetos.setdefault(int(cod), {"tabelas": []})
        o["tabelas"].append(t)
        o["execucao"] = (tg.get("plhdwa_execucao") or "DESPACHANTE").upper()
        o["tipo_job"] = (tg.get("plhdwa_tipo_job") or "PERFORMANCE_OPTIMIZED").upper()
    with _trava_cache:
        _mapa.update(quando=time.time(), objetos=objetos)
    return objetos


def tabela_do_objeto(cod):
    """Tabela destino do objeto (None se não houver exatamente uma cadastrada)."""
    tabelas = mapa_objetos().get(cod, {}).get("tabelas", [])
    return tabelas[0] if len(tabelas) == 1 else None


def execucao_do_objeto(cod):
    """(modo, tipo_job) do objeto. Objeto sem tabela cadastrada roda no despachante (e dá ERRO_CADASTRO)."""
    o = mapa_objetos().get(cod, {})
    modo = o.get("execucao", "DESPACHANTE")
    return (modo if modo in MODOS else "DESPACHANTE"), o.get("tipo_job", "PERFORMANCE_OPTIMIZED")


def metadados(cod):
    with _trava_cache:
        if cod in _cache_meta:
            return _cache_meta[cod]
    achadas = mapa_objetos().get(cod, {"tabelas": []})["tabelas"]
    if len(achadas) != 1:
        raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: {len(achadas)} tabela(s) com a tag plhdwa_cod_objeto {achadas}")
    t = achadas[0]
    props = {r.key: r.value for r in sql(f"SHOW TBLPROPERTIES {t}").collect()}
    meta = {"tabela": t, "props": props, "schema": CTX["spark"].table(t).schema,
            "tipo": props.get("plhdwa.tipo_carga", "FATO"),
            "col_data": props.get("plhdwa.col_data_mov", ""), "col_inst": props.get("plhdwa.col_instituicao", "")}
    with _trava_cache:
        _cache_meta[cod] = meta
    return meta


# =============================================================================
# 5. Leitura do DAT
# =============================================================================
def _p(meta, chave, padrao=""):
    return meta["props"].get(f"plhdwa.arquivo.{chave}", padrao)


def ler_dat(arq, meta):
    """CSV nativo, com as opções das TBLPROPERTIES e o schema da tabela. FAILFAST: linha inválida = erro."""
    if _p(meta, "escape") and not _p(meta, "quote"):
        return ler_dat_escape(arq, meta)
    tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
    leitor = (CTX["spark"].read.format("csv").schema(meta["schema"])
              .option("sep", _p(meta, "delimitador", "|"))
              .option("header", _p(meta, "header", "true"))
              .option("enforceSchema", "false")                 # confere o header com o schema
              .option("encoding", _p(meta, "encoding", "ISO-8859-15"))
              .option("quote", _p(meta, "quote"))                # vazio = sem aspas
              .option("nullValue", _p(meta, "null_value"))
              .option("timestampFormat", tsf).option("timestampNTZFormat", tsf)
              .option("dateFormat", _p(meta, "date_format", "yyyy-MM-dd"))
              .option("mode", "FAILFAST"))
    return leitor.load(arq["dat"])


def ler_dat_escape(arq, meta):
    """Arquivo sem aspas e com ESCAPECHAR (ex.: 2\\|3 = '2|3' e \\\\ = '\\'). Só funções nativas, sem UDF."""
    sep, esc = _p(meta, "delimitador", "|"), _p(meta, "escape")
    e, d, marca = re.escape(esc), re.escape(sep), "\u0002"
    # Lê cada linha inteira como uma coluna (separador improvável), respeitando o encoding.
    linhas = (CTX["spark"].read.format("csv").schema("linha STRING").option("sep", "\u0001").option("quote", "")
              .option("encoding", _p(meta, "encoding", "ISO-8859-15")).option("header", "false").load(arq["dat"]))
    if _p(meta, "header", "true") == "true":
        linhas = linhas.filter(F.col("linha") != ler_header(arq))
    campos = F.split(F.regexp_replace("linha", e + e, marca), f"(?<!{e}){d}", -1)   # | não escapado
    n = len(meta["schema"].fields)
    df = (linhas.select(campos.alias("c"))
          .filter(F.when(F.size("c") != n, F.raise_error(F.concat(
              F.lit(f"ERRO_DADOS: linha com "), F.size("c").cast("string"), F.lit(f" campos (esperado {n})"))))
              .otherwise(F.lit(True))))
    tsf, nulo = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]"), _p(meta, "null_value")
    cols = []
    for i, campo in enumerate(meta["schema"].fields):
        v = F.translate(F.regexp_replace(F.col("c")[i], e + "(.)", "$1"), marca, esc)   # tira o escape
        v = F.when(v == F.lit(nulo), F.lit(None)).otherwise(v)
        if isinstance(campo.dataType, T.TimestampNTZType):
            v = F.to_timestamp_ntz(v, F.lit(tsf))
        elif not isinstance(campo.dataType, T.StringType):
            v = v.cast(campo.dataType)                                                     # ANSI: inválido = erro
        cols.append(v.alias(campo.name))
    return df.select(cols)


def ler_header(arq):
    return CTX["dbutils"].fs.head(arq["dat"], 65536).split("\n", 1)[0].rstrip("\r")


# =============================================================================
# 6. Validações (CTL, sequência, pré-checagem) e detector de formato
# =============================================================================
def validar_ctl(arq, meta_header=True):
    """Conteúdo do CTL x nome do arquivo + DAT presente e não vazio. Devolve (instituições, tamanho do DAT)."""
    try:
        texto = CTX["dbutils"].fs.head(arq["ctl"]).strip()
    except Exception as e:
        raise ErroCarga("ERRO_CTL", f"não consegui ler o CTL: {e}")
    partes = texto.split("|")
    if len(partes) != 5:
        raise ErroCarga("ERRO_CTL", f"CTL com {len(partes)} campos (esperado 5): {texto[:200]}")
    cod, _tab, dt_mov, dt_ger, insts = (p.strip() for p in partes)
    if (cod, dt_mov, dt_ger) != (str(arq["cod"]), arq["dt_mov"], arq["dt_ger"]):
        raise ErroCarga("ERRO_CTL", f"conteúdo ({cod}, {dt_mov}, {dt_ger}) diferente do nome "
                                    f"({arq['cod']}, {arq['dt_mov']}, {arq['dt_ger']})")
    for valor, fmt in ((dt_mov, "%Y%m%d"), (dt_ger, "%Y%m%d%H%M%S")):
        try:
            time.strptime(valor, fmt)
        except ValueError:
            raise ErroCarga("ERRO_CTL", f"data inválida no CTL: {valor}")
    if insts != "0" and not re.fullmatch(r"\d+(,\d+)*", insts):
        raise ErroCarga("ERRO_CTL", f"lista de instituições inválida: {insts}")
    try:
        tamanho = CTX["dbutils"].fs.ls(arq["dat"])[0].size     # 1 arquivo: não lista a pasta inteira
    except Exception:
        raise ErroCarga("ERRO_PAR", f"CTL sem o DAT {arq['nome_dat']}")
    # DAT em branco = erro. Precisa ser ANTES da carga: um REPLACE WHERE com 0 linhas apagaria a data inteira.
    if tamanho == 0:
        raise ErroCarga("ERRO_DAT_VAZIO", f"DAT {arq['nome_dat']} com 0 bytes")
    inicio = CTX["dbutils"].fs.head(arq["dat"], 65536)
    linhas = [l for l in inicio.split("\n") if l.strip()]
    if len(linhas) < (2 if meta_header else 1):
        raise ErroCarga("ERRO_DAT_VAZIO", f"DAT {arq['nome_dat']} sem registros (só o header)" if linhas
                        else f"DAT {arq['nome_dat']} em branco")
    return insts, tamanho


def info_arquivo(arq):
    """UMA consulta ao log: último status do arquivo, se ele já foi CARREGADO (retomada), linhas carregadas
    e a última geração carregada do objeto (regra de sequência)."""
    r = sql(f"""SELECT max_by(qtd_inserida, evento_ts) FILTER (WHERE arquivo_ctl = :a AND status = 'CARREGADO') AS qtd,
                       count(*) FILTER (WHERE arquivo_ctl = :a AND status = 'CARREGADO')                    AS carregado,
                       max(date_format(data_geracao, 'yyyyMMddHHmmss')) FILTER (WHERE status = 'CARREGADO') AS ultima
                FROM {fq('log_carga_evento')} WHERE cod_objeto = :c""", a=arq["nome_ctl"], c=arq["cod"]).first()
    return (r.qtd, bool(r.carregado), r.ultima) if r else (None, False, None)


def validacoes_fato(df, meta, arq, insts):
    """Data de movimento = CTL e instituição na lista. Violação aborta a leitura (antes do commit)."""
    if meta["tipo"] != "FATO":
        return df
    col_d = F.col(meta["col_data"])
    tipo_d = meta["schema"][meta["col_data"]].dataType
    alvo = (F.to_date(F.lit(arq["dt_mov"]), "yyyyMMdd") if isinstance(tipo_d, T.DateType)
            else F.lit(arq["dt_mov"]).cast(tipo_d))
    regra = F.when(col_d != alvo, F.raise_error(F.concat(
        F.lit(f"ERRO_DADOS: {meta['col_data']} = "), col_d.cast("string"), F.lit(f" diferente do CTL ({arq['dt_mov']})"))))
    if insts != "0":
        lista = [int(x) for x in insts.split(",")]
        col_i = F.col(meta["col_inst"])
        regra = regra.when(~col_i.isin(lista), F.raise_error(F.concat(
            F.lit(f"ERRO_DADOS: {meta['col_inst']} = "), col_i.cast("string"), F.lit(f" fora da lista do CTL ({insts})"))))
    return df.filter(regra.otherwise(F.lit(True)))


_detector_feito = set()


def precheck(arq, meta, insts, tamanho, linhas=None):
    """Header x schema (sempre, é barato). A amostra (parâmetro precheck_linhas) + detector de formato custa uma
    leitura extra: roda em arquivos a partir de precheck_min_mb (erra cedo) e, nos menores, só no 1º arquivo de
    cada objeto por execução (nos pequenos a própria carga já valida tudo em FAILFAST, na mesma leitura)."""
    linhas = linhas or int(CTX["precheck_linhas"])
    if _p(meta, "header", "true") == "true":
        header = [c.strip().upper() for c in ler_header(arq).split(_p(meta, "delimitador", "|"))]
        esperado = [f.name.upper() for f in meta["schema"].fields]
        if header != esperado:
            novas = [c for c in header if c not in esperado]
            faltam = [c for c in esperado if c not in header]
            raise ErroCarga("ERRO_SCHEMA", f"header diferente da tabela {meta['tabela']}: "
                                           f"novas={novas[:10]} faltando={faltam[:10]} "
                                           f"({len(header)} colunas no arquivo, {len(esperado)} na tabela)")
    limite = float(CTX["precheck_min_mb"]) * 1024 * 1024
    with _trava_cache:
        primeiro = arq["cod"] not in _detector_feito
        _detector_feito.add(arq["cod"])
    if tamanho < limite and not primeiro:
        return
    amostra = validacoes_fato(ler_dat(arq, meta).limit(linhas), meta, arq, insts)
    textos = [f.name for f in meta["schema"].fields if isinstance(f.dataType, T.StringType)]
    try:
        aggs = [F.count(F.lit(1)).alias("_linhas")]
        for c in textos:
            aggs += [F.sum(F.when(F.col(c).rlike(r'\\[|"\\]'), 1).otherwise(0)).alias(f"esc__{c}"),
                     F.sum(F.when(F.upper(F.col(c)) == "NULL", 1).otherwise(0)).alias(f"null__{c}"),
                     F.sum(F.when(F.col(c).rlike(r'^".*"$'), 1).otherwise(0)).alias(f"aspas__{c}")]
        r = amostra.agg(*aggs).first().asDict()
    except ErroCarga:
        raise
    except Exception as e:
        raise ErroCarga(*explicar_erro(e, arq, meta))
    suspeitos = {k: v for k, v in r.items() if k != "_linhas" and v}
    if suspeitos:
        evento(arq, "ALERTA_FORMATO", tabela=meta["tabela"],
               mensagem=f"possível formato divergente nas primeiras {r['_linhas']} linhas: {suspeitos}")


def explicar_erro(e, arq, meta):
    """Como classificar(), mas quando o Spark só diz "erro ao ler o arquivo" (FAILED_READ_FILE), relê o arquivo
    em modo tolerante e mostra a 1ª linha inválida e o motivo. Custo extra só quando já houve erro."""
    tipo, msg = classificar(e)
    if tipo != "ERRO_DADOS" or "FAILED_READ_FILE" not in msg or (_p(meta, "escape") and not _p(meta, "quote")):
        return tipo, msg
    try:
        sep = _p(meta, "delimitador", "|")
        n = len(meta["schema"].fields)
        esquema = T.StructType(meta["schema"].fields + [T.StructField("_corrompida", T.StringType())])
        tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
        ruim = (CTX["spark"].read.format("csv").schema(esquema).option("sep", sep)
                .option("header", _p(meta, "header", "true")).option("encoding", _p(meta, "encoding", "ISO-8859-15"))
                .option("quote", _p(meta, "quote")).option("nullValue", _p(meta, "null_value"))
                .option("timestampFormat", tsf).option("timestampNTZFormat", tsf)
                .option("dateFormat", _p(meta, "date_format", "yyyy-MM-dd"))
                .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrompida")
                .load(arq["dat"]).filter("_corrompida IS NOT NULL")
                .select("*").limit(1).collect())   # todas as colunas: com poda, o Spark não analisa a coluna ruim
        if not ruim:
            print("diagnóstico: nenhuma linha inválida encontrada no modo tolerante")
            return tipo, msg
        linha = ruim[0]["_corrompida"]
        campos = linha.split(sep)
        if len(campos) != n:
            motivo = f"linha com {len(campos)} campos (esperado {n}); se há '{sep}' dentro de um texto, falta ESCAPECHAR"
        else:
            motivo = "valor incompatível com o tipo de alguma coluna (ex.: texto em campo numérico ou data inválida)"
        return tipo, f"ERRO_DADOS: {motivo}. 1ª linha inválida: {linha[:400]}"
    except Exception as diag:
        print(f"diagnóstico do erro não foi possível: {diag}")
        return tipo, msg


def classificar(e):
    """Traduz exceções do Spark/Delta em (tipo, mensagem). Erros de dado viram ERRO_DADOS."""
    msg = str(e)
    if "ERRO_DADOS" in msg:
        trecho = msg[msg.index("ERRO_DADOS"):].split("\n")[0]
        return "ERRO_DADOS", trecho
    sinais = ("MALFORMED", "Malformed", "CAST_INVALID_INPUT", "CANNOT_PARSE", "NumberFormat", "FAILFAST",
              "REPLACE_WHERE", "replaceWhere", "CSV header", "does not conform", "DATETIME", "ARITHMETIC_OVERFLOW",
              "NUMERIC_VALUE_OUT_OF_RANGE", "CANNOT_CAST")
    if any(s in msg for s in sinais):
        return "ERRO_DADOS", msg[:2000]
    return "ERRO_EXECUCAO", msg[:2000]


# =============================================================================
# 7. Carga atômica
# =============================================================================
def carregar(arq, meta, insts):
    """FATO: REPLACE WHERE data (+ instituições). DIM: substitui tudo. Um único commit Delta."""
    t = meta["tabela"]
    if meta["tipo"] == "FATO":
        tipo_d = meta["schema"][meta["col_data"]].dataType
        valor = f"DATE'{arq['dt_mov'][:4]}-{arq['dt_mov'][4:6]}-{arq['dt_mov'][6:]}'" \
            if isinstance(tipo_d, T.DateType) else (f"'{arq['dt_mov']}'" if isinstance(tipo_d, T.StringType) else arq["dt_mov"])
        predicado = f"{meta['col_data']} = {valor}"
        if insts != "0":
            predicado += f" AND {meta['col_inst']} IN ({insts})"
    else:
        predicado = "1 = 1"     # DIM: substitui a tabela inteira, mantendo tags, propriedades e grants
    df = validacoes_fato(ler_dat(arq, meta), meta, arq, insts)
    try:
        (df.write.format("delta").mode("overwrite").option("replaceWhere", predicado)
           .option("userMetadata", arq["nome_ctl"]).saveAsTable(t))
    except Exception as e:
        tipo, msg = explicar_erro(e, arq, meta)
        raise ErroCarga(tipo, msg, mover=(tipo != "ERRO_EXECUCAO"))
    h = (sql(f"DESCRIBE HISTORY {t} LIMIT 10").filter(F.col("userMetadata") == arq["nome_ctl"])
         .orderBy(F.col("version").desc()).first())
    m = h.operationMetrics or {}
    # numOutputRows inclui as linhas só regravadas (numCopiedRows) dos arquivos tocados pelo REPLACE WHERE
    inseridas = int(m.get("numOutputRows", 0) or 0) - int(m.get("numCopiedRows", 0) or 0)
    return h.version, inseridas, int(m.get("numDeletedRows", m.get("numRemovedRows", 0)) or 0)


# =============================================================================
# 8. Controle · síncrona, por arquivo (início, erro, conclusão)
#    db2_modo = DB2      : estado no Db2 do cliente (driver Python ibm_db; comando em sql/<db2_comando>)
#    db2_modo = SIMULADO : estado no Lakebase (só para testes, sem Db2)
#    Nos dois modos, cada mudança de status também vai para o histórico Delta (db2_controle_sim).
# =============================================================================
_lakebase = {}
_por_thread = threading.local()          # 1 conexão Lakebase por thread, reaproveitada entre arquivos
_falhas = {"quando": 0, "cods": set()}

# {t} = parâmetro lakebase_tabela (schema.tabela)
_UPSERT = """INSERT INTO {t}
               (cod_objeto, nome_tabela, data_movimento, data_geracao, instituicoes, status, qtd_registros, arquivo,
                job_run_id, lote_id, modo_execucao, dt_inicio, dt_fim, mensagem, confirmado_em)
             VALUES (%s, %s, %s, to_timestamp(%s, 'YYYYMMDDHH24MISS'), %s, %s, %s, %s, %s, %s, %s,
                     to_timestamp(%s), to_timestamp(%s), %s, now())
             ON CONFLICT (cod_objeto, data_movimento, data_geracao) DO UPDATE SET
               status = EXCLUDED.status, qtd_registros = EXCLUDED.qtd_registros, job_run_id = EXCLUDED.job_run_id,
               lote_id = EXCLUDED.lote_id, modo_execucao = EXCLUDED.modo_execucao,
               dt_inicio = COALESCE(EXCLUDED.dt_inicio, {t}.dt_inicio),
               dt_fim = EXCLUDED.dt_fim, mensagem = EXCLUDED.mensagem, confirmado_em = now()"""


def _conexao_lakebase(nova=False):
    """Conexão Postgres com token OAuth do usuário/service principal do job (token renovado a cada 30 min).
    Usa pg8000 (Python puro): o psycopg[binary] abortou o processo (SIGABRT) quando usado em threads no serverless."""
    import ssl
    import pg8000.dbapi
    from databricks.sdk import WorkspaceClient
    nome = CTX["lakebase_instance"]
    agora = time.time()
    with _trava_cache:
        if not _lakebase or agora - _lakebase["quando"] > 1800:
            w = WorkspaceClient()
            inst = w.database.get_database_instance(name=nome)
            cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[nome])
            _lakebase.update(host=inst.read_write_dns, token=cred.token, user=w.current_user.me().user_name, quando=agora)
    con = getattr(_por_thread, "con", None)
    if nova or con is None or getattr(_por_thread, "token", None) != _lakebase["token"]:
        try:
            con and con.close()
        except Exception:
            pass
        con = pg8000.dbapi.connect(host=_lakebase["host"], database=CTX["lakebase_database"], user=_lakebase["user"],
                                   password=_lakebase["token"], ssl_context=ssl.create_default_context(), timeout=30)
        _por_thread.con, _por_thread.token = con, _lakebase["token"]
    return con


_db2 = {}                                # comando lido de sql/<db2_comando>, montado uma vez


def _comando_db2():
    """Lê o comando que grava a controle (arquivo em sql/, editável pelo DBA) e troca {tabela}.
    Comentários (--) saem; precisa ter exatamente 14 parâmetros "?" (ordem descrita no próprio arquivo)."""
    import os
    with _trava_cache:
        if "sql" not in _db2:
            caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql", CTX["db2_comando"])
            linhas = [l for l in open(caminho, encoding="utf-8").read().splitlines() if not l.strip().startswith("--")]
            texto = "\n".join(linhas).strip().rstrip(";").replace("{tabela}", CTX["db2_tabela"])
            if texto.count("?") != 14:
                raise ValueError(f"{CTX['db2_comando']}: o comando precisa de 14 parâmetros '?', tem {texto.count('?')}")
            _db2["sql"] = texto
    return _db2["sql"]


def _conexao_db2():
    """Abre UMA conexão nova com o Db2 (driver Python oficial da IBM, ibm_db). Usuário e senha vêm do secret
    scope; host, porta, banco, SSL e certificado das variáveis do bundle. Quem usa é o pool abaixo."""
    try:
        import ibm_db_dbi
    except ImportError as e:
        # ibm_db só tem pacote para máquinas x86_64; no serverless a arquitetura acompanha a versão do ambiente
        import platform
        raise RuntimeError(f"driver ibm_db indisponível nesta máquina ({platform.machine()}): {e} · confira o "
                           "environment do job (dependência ibm_db e serverless_ambiente = versão em x86_64)") from e
    sec = CTX["dbutils"].secrets
    dsn = (f"DATABASE={CTX['db2_banco']};HOSTNAME={CTX['db2_host']};PORT={CTX['db2_porta']};PROTOCOL=TCPIP;"
           f"UID={sec.get(CTX['db2_secret_scope'], CTX['db2_chave_usuario'])};"
           f"PWD={sec.get(CTX['db2_secret_scope'], CTX['db2_chave_senha'])};")
    if str(CTX["db2_ssl"]).lower() == "true":
        dsn += "SECURITY=SSL;"
        if CTX["db2_certificado"]:
            dsn += f"SSLServerCertificate={CTX['db2_certificado']};"
    return ibm_db_dbi.connect(dsn + (CTX["db2_extra"] or ""), "", "")


# Pool de conexões com o Db2: as threads do despachante compartilham no máximo db2_max_conexoes conexões
# (o Db2 limita conexões por usuário/aplicação; cada gravação leva ~0,15 s, então poucas conexões bastam).
# Cada job independente é um processo à parte: total no Db2 = pool do despachante + jobs em execução.
_pool_db2 = {"livres": [], "abertas": 0}
_pool_cond = threading.Condition()


class conexao_db2:
    """with conexao_db2() as con: ...  Pega uma conexão do pool (abre se houver folga, senão espera uma voltar).
    Se o bloco der erro, a conexão é descartada (pode estar quebrada) e a vaga volta para o pool."""
    def __enter__(self):
        limite = max(1, int(CTX["db2_max_conexoes"]))
        with _pool_cond:
            while not _pool_db2["livres"] and _pool_db2["abertas"] >= limite:
                _pool_cond.wait(timeout=60)
            if _pool_db2["livres"]:
                self.con = _pool_db2["livres"].pop()
                return self.con
            _pool_db2["abertas"] += 1
        try:
            self.con = _conexao_db2()
        except Exception:
            with _pool_cond:
                _pool_db2["abertas"] -= 1
                _pool_cond.notify()
            raise
        return self.con

    def __exit__(self, tipo, erro, tb):
        with _pool_cond:
            if tipo is None:
                _pool_db2["livres"].append(self.con)
            else:
                try:
                    self.con.rollback()
                    self.con.close()
                except Exception:
                    pass
                _pool_db2["abertas"] -= 1
            _pool_cond.notify()
        return False


def _horario(epoch):
    """epoch (s) -> 'aaaa-mm-dd hh:mm:ss.ffffff' no fuso do parâmetro fuso_horario (o Db2 grava TIMESTAMP sem fuso;
    o comando faz CAST(? AS TIMESTAMP))."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, ZoneInfo(CTX["fuso_horario"])).strftime("%Y-%m-%d %H:%M:%S.%f")


def _gravar_db2(arq, status, insts, modo, qtd, inicio, fim, mensagem):
    """Executa o comando da controle no Db2 com os 14 parâmetros, na ordem documentada em sql/<db2_comando>."""
    g = arq["dt_ger"]                        # yyyyMMddHHmmss do nome do arquivo
    valores = (arq["cod"], arq["tabela_nome"][:128], int(arq["dt_mov"]),
               f"{g[:4]}-{g[4:6]}-{g[6:8]} {g[8:10]}:{g[10:12]}:{g[12:14]}",
               (insts or "")[:4000] or None, status, qtd, arq["nome_ctl"][:256], CTX["job_run_id"], CTX["lote_id"],
               modo, _horario(inicio), _horario(fim), (mensagem or "")[:2000] or None)
    for tentativa in (1, 2):                 # 2ª tentativa com conexão nova (ex.: conexão ociosa derrubada)
        try:
            with conexao_db2() as con:
                cur = con.cursor()
                cur.execute(_comando_db2(), valores)
                cur.close()
                con.commit()
            return
        except Exception:
            if tentativa == 2:
                raise


def _objetos_com_falha_simulada():
    """Teste: objetos em db2_sim_falhas simulam o Db2 fora do ar (lista relida a cada 30 s)."""
    with _trava_cache:
        if time.time() - _falhas["quando"] < 30:
            return _falhas["cods"]
    cods = {r.cod_objeto for r in CTX["spark"].table(fq("db2_sim_falhas")).collect()}
    with _trava_cache:
        _falhas.update(quando=time.time(), cods=cods)
    return cods


def _gravar_controle(arq, status, insts, modo, qtd, inicio, fim, mensagem):
    if arq["cod"] in _objetos_com_falha_simulada():     # teste de falha: vale para os dois modos
        raise RuntimeError("controle indisponível para este objeto (teste: db2_sim_falhas)")
    if CTX["db2_modo"] == "DB2":
        # (a) estado atual no Db2 do cliente
        _gravar_db2(arq, status, insts, modo, qtd, inicio, fim, mensagem)
    else:
        # (a) estado atual no Lakebase (simulação, sem Db2)
        valores = (arq["cod"], arq["tabela_nome"], int(arq["dt_mov"]), arq["dt_ger"], insts, status, qtd,
                   arq["nome_ctl"], CTX["job_run_id"], CTX["lote_id"], modo, inicio, fim, (mensagem or "")[:2000] or None)
        for tentativa in (1, 2):
            con = _conexao_lakebase(nova=(tentativa == 2))
            try:
                con.cursor().execute(_UPSERT.format(t=CTX["lakebase_tabela"]), valores)
                con.commit()
                break
            except Exception:
                if tentativa == 2:
                    raise
    # (b) histórico em Delta (1 linha por mudança de status)
    (CTX["spark"].createDataFrame([(arq["cod"], arq["tabela_nome"], int(arq["dt_mov"]), arq["dt_ger"], insts, status,
                                    qtd, arq["nome_ctl"], CTX["job_run_id"], CTX["lote_id"], modo, inicio, fim,
                                    (mensagem or "")[:2000] or None)],
                                  "cod_objeto INT, nome_tabela STRING, data_movimento INT, dg STRING, instituicoes STRING, "
                                  "status STRING, qtd_registros BIGINT, arquivo STRING, job_run_id BIGINT, lote_id BIGINT, "
                                  "modo_execucao STRING, ini DOUBLE, fim DOUBLE, mensagem STRING")
     .select("cod_objeto", "nome_tabela", "data_movimento", F.to_timestamp("dg", "yyyyMMddHHmmss").alias("data_geracao"),
             "instituicoes", "status", "qtd_registros", "arquivo", "job_run_id",
             F.current_timestamp().alias("confirmado_em"), "lote_id", "modo_execucao",
             F.timestamp_seconds("ini").alias("dt_inicio"), F.timestamp_seconds("fim").alias("dt_fim"), "mensagem")
     .write.mode("append").saveAsTable(fq("db2_controle_sim")))


def sensibilizar_controle(arq, status, insts, modo, qtd=None, inicio=None, fim=None, mensagem=None, tentativas=None):
    """Grava o status DESTE arquivo na controle: EM_PROCESSAMENTO, ERRO ou CONCLUIDO.
    Síncrono, com novas tentativas (espera crescente). Falhou todas: lança FalhaControle.
    tentativas: padrão = parâmetro db2_tentativas."""
    tentativas = tentativas or int(CTX["db2_tentativas"])
    base = float(CTX.get("db2_espera_base_s") or 30)
    erro = None
    for t in range(tentativas):
        try:
            _gravar_controle(arq, status, insts, modo, qtd, inicio, fim, mensagem)
            return
        except (ValueError, FileNotFoundError) as e:   # comando/configuração errados: não adianta tentar de novo
            raise FalhaControle(f"configuração da controle inválida: {e}")
        except Exception as e:
            erro = e
            print(f"controle ({status}): tentativa {t + 1} falhou: {e}")
            if t < tentativas - 1:
                time.sleep(base * 2 ** t)
    raise FalhaControle(str(erro))


class FalhaControle(Exception):
    pass


# =============================================================================
# 9. Processar um arquivo (threads do despachante e jobs independentes usam o MESMO código)
# =============================================================================
def ultimo_status(arq):
    return status_por_arquivo([arq["nome_ctl"]]).get(arq["nome_ctl"])


def arquivo_em_aberto(cod):
    """Arquivo do objeto que começou e não terminou (último status sem evento final). Usado quando um job
    independente cai: ele pode ter processado vários arquivos do objeto, e só o último ficou em aberto."""
    r = sql(f"""SELECT arquivo_ctl FROM {fq('v_status_arquivo')}
                WHERE cod_objeto = :c AND status IN ('ROTEADO', 'INICIADO', 'CARREGADO')
                ORDER BY ultimo_evento DESC LIMIT 1""", c=int(cod)).collect()
    return ler_nome(r[0].arquivo_ctl) if r else None


def processar_arquivo(arq, modo, tipo_job, roteado_ts=None):
    """Processa 1 par DAT/CTL do início ao fim, sensibilizando a controle com início, erro ou conclusão.
    Lança ErroCarga em caso de erro (o objeto fica travado).
    roteado_ts: no despachante o ROTEADO não é gravado na hora; vai junto com o INICIADO (menos gravações)."""
    inicio = time.time()
    tamanho, meta, insts = None, None, None
    pend = []                                           # eventos deste arquivo ainda não gravados
    comum = lambda: {"fila": modo, "tipo_job": tipo_job, "tabela": meta and meta["tabela"], "tamanho": tamanho}
    if roteado_ts:
        pend.append((arq, "ROTEADO", {"fila": modo, "tipo_job": tipo_job, "ts": roteado_ts}))
    carregou = False
    try:
        meta = metadados(arq["cod"])                    # ERRO_CADASTRO se o código não tem tabela
        insts, tamanho = validar_ctl(arq, _p(meta, "header", "true") == "true")
        qtd_anterior, ja_carregado, ultima = info_arquivo(arq)

        # 1) controle: este arquivo começou. Sem isso, não carrega (a controle é síncrona).
        try:
            sensibilizar_controle(arq, "EM_PROCESSAMENTO", insts, modo, inicio=inicio)
        except FalhaControle as e:
            raise ErroCarga("ERRO_DB2", f"controle não registrou o início: {e}", mover=False)
        eventos(pend + [(arq, "INICIADO", {**comum(), "ts": time.time(), "inicio": inicio})])
        pend = []

        if ja_carregado:
            # Retomada: os dados já estão no Delta (a controle tinha falhado na conclusão). Só conclui.
            print(f"retomada de {arq['nome_ctl']}: carga já feita, refazendo só a conclusão na controle")
            inseridas, carregou = qtd_anterior or 0, True
        else:
            if meta["tipo"] == "DIM" and insts != "0":
                raise ErroCarga("ERRO_CTL", f"DIM não aceita lista de instituições (CTL veio com {insts})")
            if ultima and arq["dt_ger"] <= ultima:
                raise ErroCarga("ERRO_SEQUENCIA", f"geração {arq['dt_ger']} não é posterior à última carregada {ultima}")
            precheck(arq, meta, insts, tamanho)
            versao, inseridas, removidas = carregar(arq, meta, insts)
            carregou = True
            pend.append((arq, "CARREGADO", {**comum(), "instituicoes": insts, "inicio": inicio, "fim": time.time(),
                                            "ts": time.time(), "qtd_inserida": inseridas, "qtd_removida": removidas,
                                            "versao": versao}))

        # 2) controle: este arquivo terminou (com a quantidade). Se a controle falhar: ERRO_DB2 e trava.
        fim = time.time()
        try:
            sensibilizar_controle(arq, "CONCLUIDO", insts, modo, qtd=inseridas, inicio=inicio, fim=fim)
        except FalhaControle as e:
            raise ErroCarga("ERRO_DB2", f"controle não registrou a conclusão: {e}", mover=False)
        confirmado = time.time()
        eventos(pend + [(arq, "CONFIRMADO_DB2", {**comum(), "ts": confirmado}),
                        (arq, "CONCLUIDO", {**comum(), "ts": max(time.time(), confirmado + 0.001),
                                            "inicio": inicio, "fim": fim, "qtd_inserida": inseridas})])
        apagar_par(arq)                                 # log antes de apagar: se cair no meio, só sobra arquivo
        return True

    except Exception as e:
        if isinstance(e, ErroCarga):
            tipo, msg, mover = e.tipo, e.mensagem, e.mover
        else:
            tipo, msg = classificar(e)
            mover = tipo == "ERRO_DADOS"
        if carregou:
            mover = False                               # dados já no Delta: arquivos ficam em raw/ para a retomada
        fim = time.time()
        eventos(pend + [(arq, tipo, {**comum(), "inicio": inicio, "fim": fim, "mensagem": msg})])
        # 3) controle: este arquivo deu erro (melhor esforço: se a própria controle estiver fora, só registra no log)
        try:
            sensibilizar_controle(arq, "ERRO", insts, modo, inicio=inicio, fim=fim, mensagem=f"{tipo}: {msg}",
                                  tentativas=1 if tipo == "ERRO_DB2" else 2)
        except Exception as e2:
            print(f"controle não recebeu o ERRO de {arq['nome_ctl']}: {e2}")
        if mover:
            mover_para_erro(arq)
        raise ErroCarga(tipo, msg, mover=False)


# =============================================================================
# 10. Execução: limites por modo (ctl_execucao)
# =============================================================================
def ler_execucao():
    """{modo: linha de ctl_execucao}. Os dois modos precisam existir; JOB precisa dos job_id."""
    linhas = {r.modo.upper(): r.asDict() for r in CTX["spark"].table(fq("ctl_execucao")).collect()}
    ok = all(m in linhas for m in MODOS) and linhas["JOB"]["job_id_perf"] and linhas["JOB"]["job_id_std"]
    return linhas if ok else None


def job_do_tipo(execucao, tipo_job):
    j = execucao["JOB"]
    return j["job_id_std"] if tipo_job == "STANDARD" else j["job_id_perf"]
