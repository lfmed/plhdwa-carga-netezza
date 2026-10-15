"""
PLHDWA · motor de carga Netezza -> Databricks (biblioteca única).

Fluxo de um arquivo (processar_arquivo), igual no despachante (threads) e nos jobs independentes:
  CTL válido? -> DAT existe e não está vazio? -> parâmetros do código na ctl_parametros (destino, carga, formato)
  -> [controle: EM_PROCESSAMENTO, só com db2_registrar_inicio = true] -> sequência
  -> [limpeza do DAT em pasta_tmp, com limpeza_dat] -> pré-checagem -> leitura + validação + carga atômica (1 commit)
  -> controle: CONCLUIDO (síncrono) -> apaga CTL e DAT -> log CONCLUIDO
  Em erro: controle: ERRO (com a mensagem) + log ERRO_* + arquivos para a pasta de erro + objeto travado.

Modelo v2 (01/10/2026):
  - sem filas por tamanho: cada objeto diz onde roda, na ctl_parametros
      execucao = DESPACHANTE (padrão) | JOB      tipo_job = PERFORMANCE_OPTIMIZED | STANDARD
  - ctl_execucao guarda só os limites de simultâneos de cada modo
  - cada execução do despachante é um LOTE (lote_id); cada arquivo sensibiliza a controle sozinho
  - parâmetros de cada objeto ficam na ctl_parametros (07/10/2026: sem tags nem TBLPROPERTIES); da tabela
    destino só vem o layout (colunas e tipos)
  - todo evento vai para log_carga_evento (só INSERT: vários workers ao mesmo tempo sem conflito)
"""
import re
import threading
import time
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Nome dos arquivos: <cod>_[<nomegrupo>_]<nome>_<data_mov yyyyMMdd>_<data_geracao yyyyMMddHHmmss>.<dat|CTL>
# (extensão em maiúscula ou minúscula: .CTL/.ctl, .DAT/.dat)
# (o grupo é o 1º segmento depois do código, só para visualização; a carga olha só o código)
RX_NOME = re.compile(r"^(\d+)_(.+)_(\d{8})_(\d{14})\.(dat|ctl)$", re.IGNORECASE)   # extensão em qualquer caixa
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
    "landing":            ("",                 "prefixo da landing (s3://... ou /Volumes/...); as pastas abaixo ficam dentro dele"),
    "pasta_dat":          ("dados",            "pasta dos arquivos .dat, dentro de landing"),
    "pasta_ctl":          ("ctl",              "pasta dos arquivos .CTL, dentro de landing (o gatilho do despachante olha esta)"),
    "pasta_erro":         ("erro",             "pasta para onde o motor move o par com erro de negócio, dentro de landing"),
    "pasta_tmp":          ("tmp",              "pasta de trabalho temporário do cliente, dentro de landing (o motor não lê "
                                               "nem grava nela; só confere que é diferente das outras)"),
    "landing_tipo":       ("S3",               "S3 = direto no bucket (s3://..., via external location do Unity Catalog); "
                                               "VOLUME = volume do Unity Catalog (/Volumes/...)"),
    "fuso_horario":       ("America/Sao_Paulo", "fuso das datas de geração dos nomes de arquivo e dos horários do log"),
    # --- controle (Db2) ---
    "usar_db2":           ("sim",              "sim = executa as etapas da controle no Db2 (CONCLUIDO/ERRO e, com "
                                               "db2_registrar_inicio, EM_PROCESSAMENTO); nao = não executa nenhuma "
                                               "etapa do Db2 (sem conexão; o log não tem CONFIRMADO_DB2)"),
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
    "db2_comando":        ("02_db2_controle_update.sql", "arquivo em sql/ com os UPDATE da controle (um por "
                                               "status: [CONCLUIDO], [ERRO] e, opcional, [EM_PROCESSAMENTO])"),
    "db2_timeout_s":      ("30",               "segundos de espera para conectar no Db2 e por lock na controle; "
                                               "passou disso, a tentativa falha (em vez de esperar sem fim)"),
    "db2_mensagem_max":   ("500",              "tamanho máximo da mensagem de erro enviada ao Db2 (o da coluna da controle)"),
    "lakebase_instance":  ("",                 "instância Lakebase que simula o Db2 Controle"),
    "lakebase_database":  ("plhdwa",           "banco (database) da controle simulada no Lakebase"),
    "lakebase_tabela":    ("controle.db2_controle", "schema.tabela da controle simulada no Lakebase"),
    "db2_max_conexoes":   ("8",                "conexões simultâneas com o Db2 por processo (as threads compartilham)"),
    "db2_tentativas":     ("3",                "tentativas de gravar na controle antes de ERRO_DB2"),
    "db2_espera_base_s":  ("30",               "espera antes da 2ª tentativa (dobra a cada tentativa)"),
    "db2_registrar_inicio": ("false",          "true = grava EM_PROCESSAMENTO no Db2 antes da carga (e só carrega se o Db2 "
                                               "aceitar); false = o Db2 só recebe o fim: CONCLUIDO ou ERRO"),
    # --- leitura ---
    "limpeza_pedaco_mb":  ("0",                "modo CLIENTE: tamanho (MB) dos pedaços do arquivo lidos por tarefa; 0 = padrão do "
                                               "Spark. 4 deixa os pedaços iguais com qualquer nº de núcleos, mas medido ~2x mais lento"),
    "limpeza_dat":        ("true",             "true = antes da carga, limpa o DAT com as regex do cliente e grava o arquivo "
                                               "limpo em pasta_tmp (apagado no fim); false = lê o DAT direto. Cada tabela "
                                               "pode sobrepor com a coluna limpeza da ctl_parametros"),
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
    "mover_invalido_min": ("5",                "arquivo da pasta de CTL com nome fora do padrão há mais de N minutos vai para "
                                               "pasta_erro (com o .dat de mesmo nome); a espera evita mexer em arquivo sendo gravado"),
    "listagem_espera_s":  ("5",                "espera (s) entre as tentativas de listar a pasta de CTL antes de falhar "
                                               "(vigia: variável vigia_retry_espera_s, padrão 30)"),
    "vigia_parado_min":   ("5",                "CTL pendente há mais de N minutos sem despachante rodando = disparar"),
    # --- referências dinâmicas do Jobs ---
    "job_id":             ("0",                "{{job.id}}"),
    "job_run_id":         ("0",                "{{job.run_id}}: liga o log ao custo (system.billing.usage)"),
}


# Parâmetros que todo notebook do motor recebe (os específicos de cada um vêm somados a esta lista)
COMUNS = ["catalogo", "schema", "landing", "landing_tipo", "pasta_dat", "pasta_ctl", "pasta_erro",
          "pasta_tmp", "fuso_horario", "limpeza_dat", "limpeza_pedaco_mb",
          "mover_invalido_min", "listagem_espera_s", "usar_db2", "db2_modo", "db2_host", "db2_porta", "db2_banco", "db2_ssl", "db2_certificado", "db2_extra",
          "db2_secret_scope",
          "db2_chave_usuario", "db2_chave_senha", "db2_tabela", "db2_comando", "db2_max_conexoes", "db2_tentativas",
          "db2_espera_base_s", "db2_registrar_inicio", "db2_timeout_s", "db2_mensagem_max",
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
    validar_landing()
    v = str(CTX["usar_db2"]).strip().lower()
    if v not in ("sim", "nao", "não"):
        raise ValueError(f"usar_db2 inválido: {CTX['usar_db2']!r} (use sim ou nao)")
    CTX["usar_db2"] = "sim" if v == "sim" else "nao"
    for c in [CTX["catalogo"], CTX["schema"], CTX["lakebase_tabela"], CTX["db2_tabela"] or "x"]:
        if not re.fullmatch(r"[A-Za-z0-9_.]+", c):   # vão em SQL montado por texto: só nomes simples
            raise ValueError(f"nome inválido em parâmetro: {c!r}")


def validar_landing():
    """landing_tipo e o caminho da landing precisam combinar. O motor só usa dbutils.fs e spark.read na landing,
    que funcionam igual nos dois casos; no S3, o acesso passa pela external location do Unity Catalog que cobre o
    bucket/prefixo (a identidade do job precisa de READ FILES e WRITE FILES nela)."""
    tipo, caminho = str(CTX["landing_tipo"]).upper(), str(CTX["landing"] or "")
    esperado = {"S3": "s3://", "VOLUME": "/Volumes/"}
    if tipo not in esperado:
        raise ValueError(f"landing_tipo inválido: {tipo!r} (use S3 ou VOLUME)")
    if caminho and not caminho.startswith(esperado[tipo]):
        raise ValueError(f"landing_tipo = {tipo} exige landing começando com {esperado[tipo]} (veio {caminho!r})")
    CTX["landing_tipo"] = tipo
    # Pastas dentro da landing: nomes simples (podem ter subpasta, ex.: entrada/dados) e todas diferentes,
    # para o gatilho (que olha pasta_ctl) não disparar com arquivo de outra pasta.
    pastas = {k: str(CTX[k]).strip("/") for k in ("pasta_dat", "pasta_ctl", "pasta_erro", "pasta_tmp")}
    for k, v in pastas.items():
        if not re.fullmatch(r"[A-Za-z0-9_.\-]+(/[A-Za-z0-9_.\-]+)*", v):
            raise ValueError(f"{k} inválida: {v!r}")
    if len(set(pastas.values())) != len(pastas):
        raise ValueError(f"pastas da landing precisam ser diferentes: {pastas}")
    CTX.update(pastas)


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
                  "data_referencia INT, dg STRING, instituicoes STRING, nr_cpr STRING, status STRING, inicio_s DOUBLE, "
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
        "data_referencia": int(arq["dt_mov"]) if arq and arq.get("dt_mov") else None,
        "dg": arq.get("dt_ger") if arq else None,
        "instituicoes": c.get("instituicoes"),
        "nr_cpr": c.get("nr_cpr"),
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
               "instituicoes", "nr_cpr", "status", F.timestamp_seconds("inicio_s").alias("inicio"),
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
    if not m or m.group(5).upper() != "CTL":
        return None
    base = nome_ctl[:-4]
    # <cod>_<nomegrupo>_<nome>_...: o grupo (1º segmento, sem '_') é só para visualização; a carga usa só o código
    grupo, _, nome = m.group(2).partition("_")
    return {"cod": int(m.group(1)), "tabela_nome": m.group(2), "grupo": grupo if nome else "", "nome": nome or grupo, "dt_mov": m.group(3), "dt_ger": m.group(4),
            "base": base, "nome_ctl": nome_ctl, "nome_dat": base + ".dat",
            "ctl": pasta(f"{CTX['pasta_ctl']}/{nome_ctl}"), "dat": pasta(f"{CTX['pasta_dat']}/{base}.dat")}


class FalhaLanding(Exception):
    """Não foi possível listar uma pasta da landing (permissão, external location com problema, rede...)."""


_SINAIS_INEXISTENTE = ("FileNotFound", "No such file", "does not exist", "PATH_NOT_FOUND")


def _ls(caminho, obrigatoria=False, tentativas=3, info=None):
    """Lista uma pasta.
    obrigatoria=False: qualquer erro = lista vazia.
    obrigatoria=True (pasta de CTL, no despachante e no vigia): erro de acesso (permissão, external location com
    problema) é tentado de novo e, persistindo, levanta FalhaLanding com o erro real: o job falha com o motivo (antes
    virava "pasta vazia" e o lote terminava sem processar nada e sem erro). Pasta inexistente: o motor cria a pasta
    (no S3 ela some quando o último CTL é apagado); se não conseguir criar (bucket ou prefixo errado, sem permissão
    de escrita), FalhaLanding. info (dict) recebe criada = True quando a pasta não existia."""
    for t in range(tentativas if obrigatoria else 1):
        try:
            return CTX["dbutils"].fs.ls(caminho)
        except Exception as e:
            msg = str(e)
            if not obrigatoria:
                return []
            if any(x in msg for x in _SINAIS_INEXISTENTE):
                try:
                    CTX["dbutils"].fs.mkdirs(caminho)
                except Exception as e2:
                    raise FalhaLanding(f"não consegui listar {caminho}: a pasta não existe e não foi possível criá-la "
                                       f"(confira landing e pasta_ctl, a external location e a permissão de escrita): "
                                       f"{str(e2)[:1200]}")
                if info is not None:
                    info["criada"] = True
                return []
            if t == tentativas - 1:
                raise FalhaLanding(f"não consegui listar {caminho} ({tentativas} tentativas): {msg[:1500]}")
            time.sleep(float(CTX.get("listagem_espera_s") or 5))


def tamanhos_dat():
    """Tamanho em bytes de cada DAT, direto da listagem (sem abrir o arquivo). A chave é o nome com extensão
    ".dat" minúscula (o nome_dat de ler_nome), qualquer que seja a caixa da extensão no arquivo."""
    return {(f.name[:-4] + ".dat" if f.name.lower().endswith(".dat") else f.name): f.size
            for f in _ls(pasta(CTX["pasta_dat"]))}


def resolver_dat(arq):
    """Acha o DAT do par com a extensão em qualquer caixa (.dat, .DAT, .Dat...) e acerta arq['dat'] e
    arq['nome_dat'] para o nome real. Devolve o tamanho em bytes, ou None se não existir."""
    for nome in (arq["base"] + ".dat", arq["base"] + ".DAT"):
        try:
            f = CTX["dbutils"].fs.ls(pasta(f"{CTX['pasta_dat']}/{nome}"))[0]
            arq.update(dat=f.path, nome_dat=nome)
            return f.size
        except Exception:
            pass
    alvo = (arq["base"] + ".dat").lower()          # outra combinação de caixa: procura na pasta (só neste caso)
    for f in _ls(pasta(CTX["pasta_dat"])):
        if f.name.lower() == alvo:
            arq.update(dat=f.path, nome_dat=f.name)
            return f.size
    return None


def status_por_arquivo(nomes):
    if not nomes:
        return {}
    df = CTX["spark"].table(fq("v_status_arquivo")).filter(F.col("arquivo_ctl").isin(list(nomes)))
    return {r.arquivo_ctl: r.status for r in df.select("arquivo_ctl", "status").collect()}


def objetos_travados():
    return {r.cod_objeto for r in CTX["spark"].table(fq("v_objeto_travado")).select("cod_objeto").collect()}


def pendentes(cod=None, diag=None):
    """CTLs da pasta de CTL ainda não concluídos, de objetos não travados nem inativos, agrupados por objeto e em ordem de geração.
    cod: só os desse objeto (usado pelo job independente para seguir com o próximo arquivo do mesmo objeto).
    diag (dict, opcional): recebe o que foi visto na pasta e o motivo de cada CTL não ter entrado (resumo_pasta).
    Falha ao listar a pasta = FalhaLanding (o job falha)."""
    caminho = pasta(CTX["pasta_ctl"])
    info = {}
    lidos = [f for f in _ls(caminho, obrigatoria=True, info=info) if not f.isDir()]
    nomes = [f.name for f in lidos]
    arqs, fora = [], []
    for f in lidos:
        a = ler_nome(f.name)
        if not a:
            fora.append(f)                  # fora do padrão <cod>_[<nomegrupo>_]<nome>_<yyyyMMdd>_<yyyyMMddHHmmss>.CTL
        elif cod is None or a["cod"] == cod:
            arqs.append(a)
    movidos = mover_invalidos(fora) if cod is None else []
    st = status_por_arquivo([a["nome_ctl"] for a in arqs])
    travados, inativos = objetos_travados(), objetos_inativos()
    ignorados = {"concluido": [], "travado": [], "inativo": []}
    grupos = {}
    for a in arqs:
        if st.get(a["nome_ctl"]) == "CONCLUIDO":
            ignorados["concluido"].append(a["nome_ctl"])
            try:
                apagar_par(a)          # sobra de uma execução que caiu entre o log e a remoção
            except Exception:
                pass
            continue
        if a["cod"] in travados:            # erro anterior sem DESBLOQUEIO
            ignorados["travado"].append(a["nome_ctl"])
            continue
        if a["cod"] in inativos:            # ativo = false na ctl_parametros
            ignorados["inativo"].append(a["nome_ctl"])
            continue
        grupos.setdefault(a["cod"], []).append(a)
    if diag is not None:
        diag.update(pasta=caminho, criada=info.get("criada", False), arquivos=len(nomes),
                    fora_do_padrao=[f.name for f in fora if f.name not in movidos], movidos=movidos,
                    pendentes=sum(len(v) for v in grupos.values()), **ignorados)
    return {cod: sorted(v, key=lambda a: a["dt_ger"]) for cod, v in grupos.items()}


def mover_invalidos(arquivos):
    """Arquivos da pasta de CTL com nome fora do padrão, parados há mais de mover_invalido_min minutos: vão para
    pasta_erro (o CTL e, se for um .ctl/.CTL, o .dat de mesmo nome em pasta_dat) e geram o evento NOME_INVALIDO
    (não trava objeto). Os mais novos esperam: podem estar sendo gravados."""
    limite_ms = (time.time() - float(CTX["mover_invalido_min"]) * 60) * 1000
    movidos, evs = [], []
    for f in arquivos:
        if f.modificationTime > limite_ms:
            continue
        try:
            CTX["dbutils"].fs.mv(f.path, pasta(f"{CTX['pasta_erro']}/{f.name}"))
        except Exception as e:
            print(f"não consegui mover {f.name} para {CTX['pasta_erro']}: {e}")
            continue
        base, _, ext = f.name.rpartition(".")
        dat = None
        if ext.lower() == "ctl":
            for nome_dat in (f"{base}.dat", f"{base}.DAT"):
                try:
                    CTX["dbutils"].fs.mv(pasta(f"{CTX['pasta_dat']}/{nome_dat}"), pasta(f"{CTX['pasta_erro']}/{nome_dat}"))
                    dat = nome_dat
                    break
                except Exception:
                    pass                    # sem DAT de mesmo nome
        c = re.match(r"^(\d+)_", f.name)
        evs.append(({"cod": int(c.group(1)) if c else None, "nome_ctl": f.name, "nome_dat": dat}, "NOME_INVALIDO",
                    {"mensagem": f"nome fora do padrão <cod>_[<nomegrupo>_]<nome>_<yyyyMMdd>_<yyyyMMddHHmmss>.CTL: "
                                 f"movido para {CTX['pasta_erro']}/" + (f" com {dat}" if dat else "")}))
        movidos.append(f.name)
    eventos(evs)
    return movidos


def resumo_pasta(diag, limite=5):
    """Texto do que foi visto na pasta de CTL (vai para log_lote.mensagem e para a saída do job)."""
    if not diag:
        return ""
    mostra = lambda xs: ", ".join(xs[:limite]) + (f" e mais {len(xs) - limite}" if len(xs) > limite else "")
    partes = [f"{diag['pasta']}: {diag['arquivos']} arquivo(s), {diag['pendentes']} CTL pendente(s)"]
    if diag.get("criada"):
        partes.append("a pasta não existia e foi criada agora (se o Netezza grava em outro lugar, confira landing e pasta_ctl)")
    elif not diag["arquivos"]:
        partes.append("pasta vazia (confira landing e pasta_ctl se o CTL foi gravado)")
    if diag.get("movidos"):
        partes.append(f"{len(diag['movidos'])} com nome fora do padrão movido(s) para {CTX['pasta_erro']}/: "
                      f"{mostra(diag['movidos'])}")
    if diag["fora_do_padrao"]:
        partes.append(f"{len(diag['fora_do_padrao'])} ignorado(s) por nome fora do padrão "
                      f"<cod>_[<nomegrupo>_]<nome>_<yyyyMMdd>_<yyyyMMddHHmmss>.CTL (vão para {CTX['pasta_erro']}/ "
                      f"depois de {CTX['mover_invalido_min']} min na pasta): {mostra(diag['fora_do_padrao'])}")
    for chave, texto in (("travado", "de objeto travado (ver v_objeto_travado; DESBLOQUEIO)"),
                         ("inativo", "de objeto com ativo = false na ctl_parametros"),
                         ("concluido", "já concluído(s) (sobra apagada)")):
        if diag[chave]:
            partes.append(f"{len(diag[chave])} {texto}: {mostra(diag[chave])}")
    return " · ".join(partes)


def apagar_par(arq):
    """Sucesso: apaga o CTL primeiro. Se cair no meio, sobra só um DAT órfão (inofensivo)."""
    CTX["dbutils"].fs.rm(arq["ctl"])
    resolver_dat(arq)
    try:
        CTX["dbutils"].fs.rm(arq["dat"])
    except Exception:
        pass


def mover_para_erro(arq):
    """Erro de negócio: move o CTL primeiro, depois o DAT (se existir), para a pasta de erro (pasta_erro)."""
    resolver_dat(arq)
    for origem, nome in ((arq["ctl"], arq["nome_ctl"]), (arq["dat"], arq["nome_dat"])):
        try:
            CTX["dbutils"].fs.mv(origem, pasta(f"{CTX['pasta_erro']}/{nome}"))
        except Exception:
            pass   # DAT ausente (ERRO_PAR)


# =============================================================================
# 4. Parâmetros dos objetos (tabela ctl_parametros: uma linha por código de objeto)
# =============================================================================
# O motor não usa tags nem TBLPROPERTIES: destino, tipo de carga, colunas do REPLACE WHERE, modo de execução,
# formato do arquivo e limpeza vêm da ctl_parametros. Da tabela destino só vem o layout (colunas e tipos).
_mapa = {"quando": 0, "objetos": {}}
_RX_NOME_SIMPLES = re.compile(r"[A-Za-z0-9_]+")


def mapa_objetos(validade_s=60):
    """UMA consulta traz a ctl_parametros inteira (cache de validade_s): {código: [linhas]}. Mais de uma linha
    para o mesmo código é erro de cadastro (o Delta não impede chave duplicada), tratado em metadados()."""
    with _trava_cache:
        if time.time() - _mapa["quando"] < validade_s:
            return _mapa["objetos"]
    objetos = {}
    for r in CTX["spark"].table(fq("ctl_parametros")).collect():
        objetos.setdefault(int(r.cod_objeto), []).append(r.asDict())
    with _trava_cache:
        _mapa.update(quando=time.time(), objetos=objetos)
    return objetos


def _linha_objeto(cod):
    """A linha do objeto na ctl_parametros (None se não houver exatamente uma)."""
    linhas = mapa_objetos().get(cod, [])
    return linhas[0] if len(linhas) == 1 else None


def _nome_tabela(o):
    return f"{o['catalogo_destino']}.{o['schema_destino']}.{o['tabela_destino']}"


def tabela_do_objeto(cod):
    """Tabela destino do objeto (None se não houver exatamente uma linha na ctl_parametros)."""
    o = _linha_objeto(cod)
    return _nome_tabela(o) if o else None


def execucao_do_objeto(cod):
    """(modo, tipo_job) do objeto. Objeto sem linha na ctl_parametros roda no despachante (e dá ERRO_CADASTRO)."""
    o = _linha_objeto(cod) or {}
    modo = (o.get("execucao") or "DESPACHANTE").upper()
    return (modo if modo in MODOS else "DESPACHANTE"), (o.get("tipo_job") or "PERFORMANCE_OPTIMIZED").upper()


def objetos_inativos():
    """Códigos com ativo = false: os arquivos deles ficam parados na pasta de CTL até o objeto voltar a ativo."""
    return {cod for cod, linhas in mapa_objetos().items() if len(linhas) == 1 and linhas[0].get("ativo") is False}


def metadados(cod, validade_s=60):
    """Linha do objeto na ctl_parametros + layout da tabela destino, conferidos antes da carga (ERRO_CADASTRO se
    não baterem). Cache de validade_s: um UPDATE na ctl_parametros vale sem reiniciar o despachante."""
    with _trava_cache:
        c = _cache_meta.get(cod)
        if c and time.time() - c[0] < validade_s:
            return c[1]
    linhas = mapa_objetos(validade_s).get(cod, [])
    if len(linhas) != 1:
        raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: {len(linhas)} linha(s) na {fq('ctl_parametros')} (esperado 1)")
    o = linhas[0]
    partes = (o.get("catalogo_destino"), o.get("schema_destino"), o.get("tabela_destino"))
    if not all(p and _RX_NOME_SIMPLES.fullmatch(p) for p in partes):
        raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: destino inválido na ctl_parametros {partes}")
    t = _nome_tabela(o)
    try:
        schema = CTX["spark"].table(t).schema
    except Exception as e:
        raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: tabela destino {t} não encontrada ({str(e)[:300]})")
    # filtrotabela SIM = REPLACE WHERE pela data (+ nr_cpr): no motor, "FATO"; NAO = tabela inteira: "DIM"
    filtro = o.get("filtrotabela") or ""
    if filtro not in ("SIM", "NAO"):
        raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: filtrotabela {o.get('filtrotabela')!r} (use SIM ou NAO)")
    tipo = "FATO" if filtro == "SIM" else "DIM"
    nomes = {f.name.lower(): f.name for f in schema.fields}
    cols = {}
    for chave, padrao in (("nomecolunatempo", ""), ("nomecolunainstituicao", "NR_CPR")):   # colunas do REPLACE WHERE
        v = o.get(chave) or padrao
        if tipo == "FATO" and v.lower() not in nomes:
            raise ErroCarga("ERRO_CADASTRO", f"objeto {cod}: {chave} = {v!r} não é coluna de {t}")
        cols[chave] = nomes.get(v.lower(), v)
    meta = {"tabela": t, "params": o, "schema": schema, "tipo": tipo,
            "col_data": cols["nomecolunatempo"], "col_cpr": cols["nomecolunainstituicao"]}
    with _trava_cache:
        _cache_meta[cod] = (time.time(), meta)
    return meta


# =============================================================================
# 5. Leitura do DAT
# =============================================================================
def _p(meta, chave, padrao=""):
    """Opção do arquivo na linha do objeto (ctl_parametros); vazia/nula = padrão. Booleanos viram "true"/"false"."""
    v = meta["params"].get(chave)
    if isinstance(v, bool):
        return "true" if v else "false"
    return padrao if v in (None, "") else str(v)


def _leitura(arq, meta):
    """(caminho, opções do leitor CSV) da carga: o DAT original ou, com a limpeza, o arquivo limpo em pasta_tmp.
    Arquivo limpo (pedido do cliente): header na 1ª linha, delimitador da tabela, multiLine, aspas simples ('),
    escape '\\' (padrão do Spark) e UTF-8 (o arquivo limpo é gravado em UTF-8). Sem multiLine, o header já saiu
    na limpeza (as partes são lidas em paralelo e o header de cada parte não existe)."""
    tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
    o = {"sep": _p(meta, "delimitador", "|"), "nullValue": _p(meta, "null_value"), "timestampFormat": tsf,
         "timestampNTZFormat": tsf, "dateFormat": _p(meta, "date_format", "yyyy-MM-dd")}
    if arq.get("dat_limpo"):
        modo = arq.get("limpeza_modo")
        multi = modo in ("MULTILINHA", "CLIENTE")       # CLIENTE: partes sem header, lidas com multiLine em paralelo
        o.update(header=str(_p(meta, "header", "true") == "true" and modo == "MULTILINHA").lower(),
                 multiLine=str(multi).lower(),
                 quote=_p(meta, "limpeza_quote", "'"), escape="\\", encoding="UTF-8",
                 lineSep="\n")                   # CR do dado (escapado no Netezza) não é fim de linha
        return arq["dat_limpo"], o
    o.update(header=_p(meta, "header", "true"), encoding=_p(meta, "encoding", "ISO-8859-15"), quote=_p(meta, "quote"))
    return arq["dat"], o


def ler_dat(arq, meta):
    """CSV nativo, com as opções do arquivo da ctl_parametros e o schema da tabela. FAILFAST: linha inválida = erro.
    Com a limpeza ativa, lê o arquivo limpo (pasta_tmp); sem ela, o DAT original (com ESCAPECHAR e sem aspas,
    usa ler_dat_escape)."""
    if arq.get("limpeza_modo") == "DIRETA":
        return ler_limpo_direto(arq, meta)
    if not arq.get("dat_limpo") and _p(meta, "escape") and not _p(meta, "quote"):
        return ler_dat_escape(arq, meta)
    caminho, o = _leitura(arq, meta)
    return (CTX["spark"].read.format("csv").schema(meta["schema"]).options(**o)
            .option("enforceSchema", "false")                 # confere o header com o schema
            .option("mode", "FAILFAST").load(caminho))


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
# 5b. Limpeza do DAT (pedido do cliente): as 3 regex do cliente antes da carga. Modo por tabela
#     (coluna limpeza_modo da ctl_parametros):
#       CLIENTE (padrão) abordagem do cliente: tmp em partes paralelas + multiLine, sem header nas partes e com os
#                     registros cortados na fronteira entre partes reunidos (preparar_cliente);
#       AUTO          conta as linhas com aspa aberta: nenhuma -> PARALELA; alguma -> CLIENTE;
#       DIRETA        regex + separação das colunas (from_csv) na mesma leitura: paralelo, sem arquivo temporário;
#       PARALELA      arquivo limpo em pasta_tmp em várias partes (sem header), lido em paralelo;
#       MULTILINHA    (não recomendado em arquivo grande) arquivo limpo em UM arquivo (coalesce(1)) lido com multiLine:
#                     tarefa única, e no teste de 80 GB a ordem das linhas não se manteve.
#     Em todos: leitura com o encoding da tabela (spark.read.text só lê UTF-8), schema da tabela e FAILFAST.
# =============================================================================
_RX_LIMPEZA = [(r'\\(?![\\\'"|nrt])', ''),   # barra que não escapa \ ' " | n r t: sai
               (r'\\\|', '|'),                 # \| vira | (o texto vem entre aspas simples)
               (r'\\null', '')]                # \null vira vazio (NULL)


def limpeza_ativa(meta):
    """Coluna limpeza da ctl_parametros; nula, o parâmetro limpeza_dat."""
    return _p(meta, "limpeza", str(CTX["limpeza_dat"])).lower() == "true"


MODOS_LIMPEZA = ("CLIENTE", "AUTO", "DIRETA", "PARALELA", "MULTILINHA")


def modo_limpeza(meta):
    """Coluna limpeza_modo da ctl_parametros (nula = CLIENTE)."""
    m = _p(meta, "limpeza_modo").upper()
    return m if m in MODOS_LIMPEZA else "CLIENTE"


def caminho_tmp(arq):
    return pasta(f"{CTX['pasta_tmp']}/{arq['base']}")


def _aplicar_regex(linhas):
    """As 3 regex do cliente, só nas linhas com barra invertida (nas outras não mudam nada e são a grande maioria;
    medido em 5 GB: regex em toda linha 61 s · só nas linhas com barra 16 s, mesmo resultado)."""
    limpo = F.col("value")
    for rx, novo in _RX_LIMPEZA:
        limpo = F.regexp_replace(limpo, rx, novo)
    return linhas.withColumn("value", F.when(F.col("value").contains("\\"), limpo).otherwise(F.col("value")))


def _linhas_limpas(arq, meta, tirar_header, regex=True, bloco=False):
    """Linhas do DAT com o encoding da tabela e as 3 regex do cliente aplicadas (coluna value)."""
    # Fim de linha = LF (lineSep): por padrão o Spark também quebra linha em CR solto, e o Netezza escapa CR do dado
    # como "\\<CR>": sem isso, o registro seria cortado e o CR do dado viraria LF. CR no fim da linha (arquivo CRLF)
    # e não escapado sai.
    linhas = (CTX["spark"].read.format("csv").schema("value STRING").option("sep", "\u0001").option("quote", "")
              .option("header", "false").option("lineSep", "\n").option("encoding", _p(meta, "encoding", "ISO-8859-15"))
              .load(arq["dat"]))
    if bloco:                                         # início do pedaço do arquivo que cada tarefa leu (ordem no arquivo)
        linhas = linhas.select("value", F.col("_metadata.file_block_start").alias("_bloco"))
    v = F.col("value")
    linhas = linhas.withColumn("value", F.when(v.endswith("\r"), F.regexp_replace(v, r"(?<!\\)\r$", "")).otherwise(v))
    if tirar_header and _p(meta, "header", "true") == "true":
        linhas = linhas.filter(F.col("value") != F.lit(ler_header(arq)))
    return _aplicar_regex(linhas) if regex else linhas


def linhas_com_aspa_aberta(arq, meta):
    """Detecção (modo AUTO): linhas com número ímpar de aspas não escapadas = um registro continua na linha de baixo.
    Uma contagem em paralelo no arquivo inteiro. Se errar, erra para o lado seguro (vai para MULTILINHA)."""
    q = re.escape(_p(meta, "limpeza_quote", "'"))
    aspas = F.size(F.split(F.col("value"), f"(?<!\\\\){q}")) - 1
    return (_linhas_limpas(arq, meta, tirar_header=False)
            .filter(F.col("value").contains(_p(meta, "limpeza_quote", "'")) & (aspas % 2 == 1)).count())


def _linhas_posicionadas(arq, meta):
    """Linhas do DAT (sem header) com a posição: _bloco = início do pedaço do arquivo lido por cada tarefa (ordem
    no arquivo) e _pos = ordem da linha DENTRO do pedaço; _impar = 1 se a linha tem número ímpar de aspas não
    escapadas (abre ou fecha um registro que ocupa mais de uma linha). A regex do cliente vem DEPOIS da posição, para
    um filtro por posição não precisar rodar a regex no arquivo inteiro.
    _pos NÃO é o monotonically_increasing_id puro: ele embute o número da tarefa, e o Spark pode dar outro número
    de tarefa ao mesmo pedaço em outra execução (visto no teste de 80 GB). Tirando a tarefa, sobra a ordem da linha
    dentro da tarefa, que é estável (num arquivo só, cada tarefa lê um pedaço; conferido em preparar_cliente)."""
    q = _p(meta, "limpeza_quote", "'")
    linhas = (_linhas_limpas(arq, meta, tirar_header=True, regex=False, bloco=True)
              .withColumn("_pos", F.monotonically_increasing_id()
                          - F.spark_partition_id().cast("long") * F.lit(8589934592)))   # 2^33: sai a tarefa
    linhas = _aplicar_regex(linhas)
    aspas = F.size(F.split(F.col("value"), f"(?<!\\\\){re.escape(q)}")) - 1
    return linhas.withColumn("_impar", (F.col("value").contains(q) & (aspas % 2 == 1)).cast("int"))


class _LayoutMudou(Exception):
    """Os pedaços do arquivo não saíram iguais entre as passadas (o motor refaz a limpeza)."""


def preparar_cliente(arq, meta, destino, tentativas=3):
    """Modo CLIENTE (padrão): a abordagem do cliente (arquivo limpo em pasta_tmp em VÁRIAS partes, gravadas em
    paralelo, lidas com multiLine em paralelo), com os ajustes que evitam perder ou corromper registros:
      - o header sai antes de gravar (o CSV com header=True descartaria a 1ª linha de cada parte);
      - um registro em mais de uma linha que caiu na fronteira entre duas partes seria cortado ao meio. Uma
        passada em paralelo resume cada pedaço (1ª e última linha com aspa aberta, quantidade); o driver encadeia
        os pedaços na ordem do arquivo e acha esses registros (no máximo 1 por fronteira); eles saem das partes e
        são gravados inteiros numa parte a mais. Nada é reordenado e nenhuma linha se perde.
    As passadas são jobs Spark separados: os pedaços do arquivo precisam ser os mesmos em todas. O motor confere:
    todas as linhas da fronteira achadas antes de gravar, e nº de partes gravadas = nº de pedaços (opcional:
    limpeza_pedaco_mb fixa o tamanho do pedaço, ao custo de ~2x no tempo).
    Se não conferir, apaga a tmp e refaz (até 'tentativas'); se ainda assim não conferir, para sem gravar nada."""
    if int(CTX["limpeza_pedaco_mb"] or 0) > 0:      # opcional: a conferência abaixo já cobre pedaços que mudem
        CTX["spark"].conf.set("spark.sql.files.maxPartitionBytes", f"{int(CTX['limpeza_pedaco_mb'])}m")
    for t in range(tentativas):
        try:
            return _preparar_cliente(arq, meta, destino) + (f" (tentativa {t + 1})" if t else "")
        except _LayoutMudou as e:
            print(f"modo CLIENTE: {e}; refazendo a limpeza")
            CTX["dbutils"].fs.rm(destino, True)
    raise ErroCarga("ERRO_EXECUCAO", f"modo CLIENTE: os pedaços do arquivo mudaram entre as passadas em {tentativas} "
                                     "tentativas", mover=False)


def _preparar_cliente(arq, meta, destino):
    df = _linhas_posicionadas(arq, meta)
    resumo = sorted((df.groupBy("_bloco").agg(
        F.min("_pos").alias("ini"), F.max("_pos").alias("fim"), F.sum("_impar").alias("impares"),
        F.min(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("pri_impar"),
        F.max(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("ult_impar")).collect()), key=lambda r: r["_bloco"])
    if any(int(r["ini"]) != 0 for r in resumo):       # uma tarefa leu mais de um pedaço: a posição não é confiável
        raise _LayoutMudou("uma tarefa leu mais de um pedaço do arquivo")
    faixas, aberto, total_impares = [], False, 0
    for i, r in enumerate(resumo):
        total_impares += int(r["impares"] or 0)
        if aberto:                                    # o pedaço começa no meio de um registro do pedaço anterior
            if r["pri_impar"] is None:
                raise ErroCarga("ERRO_DADOS", "registro com quebra de linha maior que um pedaço inteiro do arquivo")
            faixas.append((resumo[i - 1]["_bloco"], resumo[i - 1]["ult_impar"], resumo[i - 1]["fim"]))   # início do registro
            faixas.append((r["_bloco"], r["ini"], r["pri_impar"]))                                    # continuação
        aberto = (int(aberto) + int(r["impares"] or 0)) % 2 == 1
    fronteira = F.lit(False)
    for b, a, z in faixas:                            # (pedaço, posição dentro do pedaço): estável entre execuções
        fronteira = fronteira | ((F.col("_bloco") == F.lit(b)) & F.col("_pos").between(a, z))
    registros = []
    if faixas:                                        # poucos registros: montados inteiros no driver, ANTES de gravar
        linhas_f = {(r["_bloco"], r["_pos"]): r["value"]
                    for r in df.filter(fronteira).select("_bloco", "_pos", "value").collect()}
        esperadas = {(b, p) for b, a, z in faixas for p in range(int(a), int(z) + 1)}
        if set(linhas_f) != esperadas:
            raise _LayoutMudou(f"linhas da fronteira: {len(set(linhas_f) & esperadas)} de {len(esperadas)} achadas")
        for j in range(0, len(faixas), 2):            # cada par de faixas = 1 registro (início + continuação)
            partes = [linhas_f[(b, p)] for b, a, z in faixas[j:j + 2] for p in range(int(a), int(z) + 1)]
            registros.append(("\n".join(partes),))    # o registro inteiro num valor só: a ordem das linhas fica garantida
    df.filter(~fronteira).select("value").write.mode("overwrite").text(destino)
    gravadas = len([f for f in CTX["dbutils"].fs.ls(destino) if f.name.startswith("part-")])
    if gravadas != len(resumo):
        raise _LayoutMudou(f"{gravadas} parte(s) gravada(s) para {len(resumo)} pedaço(s) analisado(s)")
    if registros:
        CTX["spark"].createDataFrame(registros, "value STRING").write.mode("append").text(destino)
    return (f"CLIENTE: {len(resumo)} parte(s) em paralelo, {total_impares} linha(s) com aspa aberta, "
            f"{len(registros)} registro(s) na fronteira entre partes reunido(s)")


def _schema_ddl(meta, extra=None):
    campos = [f"`{f.name}` {f.dataType.simpleString()}" for f in meta["schema"].fields]
    return ", ".join(campos + ([f"`{extra}` string"] if extra else []))


def _opcoes_direta(meta):
    tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
    return {"sep": _p(meta, "delimitador", "|"), "quote": _p(meta, "limpeza_quote", "'"), "escape": "\\",
            "nullValue": _p(meta, "null_value"), "timestampFormat": tsf, "timestampNTZFormat": tsf,
            "dateFormat": _p(meta, "date_format", "yyyy-MM-dd")}


def ler_limpo_direto(arq, meta):
    """Modo DIRETA: as regex e a separação das colunas na MESMA leitura (from_csv com as opções do cliente), em
    paralelo e sem arquivo temporário. Só para arquivos sem registro em mais de uma linha. FAILFAST: linha inválida
    = erro (o motor mostra a 1ª linha inválida)."""
    linhas = _linhas_limpas(arq, meta, tirar_header=True)
    return (linhas.select(F.from_csv("value", _schema_ddl(meta), {**_opcoes_direta(meta), "mode": "FAILFAST"}).alias("r"))
            .select(*[F.col(f"r.`{f.name}`").alias(f.name) for f in meta["schema"].fields]))


def primeira_linha_invalida_direta(arq, meta):
    """Diagnóstico do modo DIRETA: a 1ª linha que não vira registro válido (PERMISSIVE + coluna de corrompida)."""
    linhas = _linhas_limpas(arq, meta, tirar_header=True)
    r = (linhas.select("value", F.from_csv("value", _schema_ddl(meta, "_corrompida"),
                                           {**_opcoes_direta(meta), "mode": "PERMISSIVE",
                                            "columnNameOfCorruptRecord": "_corrompida"}).alias("r"))
         .filter("r._corrompida IS NOT NULL").select("value").limit(1).collect())
    return r[0]["value"] if r else None


def preparar_dat(arq, meta):
    """Limpeza do DAT antes da carga. Decide o modo e:
       CLIENTE    (padrão) abordagem do cliente em partes paralelas + multiLine, com os ajustes (preparar_cliente);
       AUTO       detecta se há registro em mais de uma linha: nenhum = PARALELA, algum = CLIENTE;
       DIRETA     nada a gravar: a limpeza acontece na própria leitura da carga (ler_limpo_direto);
       PARALELA   regex + arquivo limpo em pasta_tmp em várias partes (sem header), lido em paralelo;
       MULTILINHA arquivo limpo em UM arquivo (coalesce(1)) + multiLine. NÃO recomendado em arquivo grande: uma
                  tarefa só, e no teste de 80 GB o coalesce(1) não manteve a ordem das linhas (o header foi parar no
                  meio do arquivo).
    Devolve (modo efetivo, segundos, detalhe)."""
    inicio = time.time()
    modo, detalhe = modo_limpeza(meta), ""
    if modo == "AUTO":
        abertas = linhas_com_aspa_aberta(arq, meta)
        modo = "CLIENTE" if abertas else "PARALELA"      # medido: PARALELA é a mais rápida sem multilinha
        detalhe = f"AUTO: {abertas} linha(s) com aspa aberta"
    arq["limpeza_modo"] = modo
    if modo == "CLIENTE":
        arq["dat_limpo"] = caminho_tmp(arq)
        detalhe = preparar_cliente(arq, meta, arq["dat_limpo"]) + (f"; {detalhe}" if detalhe else "")
    if modo in ("PARALELA", "MULTILINHA"):
        destino = caminho_tmp(arq)
        linhas = _linhas_limpas(arq, meta, tirar_header=(modo == "PARALELA"))
        if modo == "MULTILINHA":
            linhas = linhas.coalesce(1)               # 1 arquivo, na ordem original (repartition embaralharia)
        linhas.write.mode("overwrite").text(destino)
        arq["dat_limpo"] = destino
    return modo, time.time() - inicio, detalhe


def apagar_tmp(arq):
    arq.pop("limpeza_modo", None)
    caminho = arq.pop("dat_limpo", None)
    if caminho:
        try:
            CTX["dbutils"].fs.rm(caminho, True)
        except Exception as e:
            print(f"não consegui apagar {caminho}: {e}")


def limpar_tmp_orfaos():
    """Pastas de limpeza que sobraram de uma execução que caiu. Só apaga pastas com o nome de um arquivo do motor
    cujo CTL não está em andamento (outros arquivos do cliente em pasta_tmp não são tocados)."""
    pastas = [f for f in _ls(pasta(CTX["pasta_tmp"])) if f.isDir() and ler_nome(f.name.rstrip("/") + ".CTL")]
    if not pastas:
        return
    nomes = lambda f: [f.name.rstrip("/") + e for e in (".CTL", ".ctl", ".Ctl")]   # extensão em qualquer caixa
    st = status_por_arquivo([n for f in pastas for n in nomes(f)])
    for f in pastas:
        if not any(st.get(n) in ("ROTEADO", "INICIADO", "CARREGADO") for n in nomes(f)):
            CTX["dbutils"].fs.rm(f.path, True)
            print(f"pasta de limpeza órfã removida: {f.path}")


# =============================================================================
# 6. Validações (CTL, sequência, pré-checagem) e detector de formato
# =============================================================================
def _lista_ctl(texto, nome):
    """'1,2,3' -> '1,2,3' (sem zeros à esquerda e sem repetidos) · '0' -> '0' (todos). ERRO_CTL se vazio, não
    numérico ou 0 junto com outros."""
    valores = [v.strip() for v in texto.split(",")]
    if not all(v.isdigit() for v in valores):
        raise ErroCarga("ERRO_CTL", f"{nome} vazio ou não numérico no CTL: '{texto[:200]}'")
    lista = list(dict.fromkeys(str(int(v)) for v in valores))
    if "0" in lista and len(lista) > 1:
        raise ErroCarga("ERRO_CTL", f"{nome} 0 (todos) junto com outros: '{texto[:200]}'")
    return ",".join(lista)


def ler_conteudo_ctl(texto, arq):
    """Lê o conteúdo do CTL, confere com o nome do arquivo e devolve (instituições, nr_cpr), cada um "0" (todos)
    ou uma lista "1,2,3". Formato (4 campos separados por '|', listas separadas por vírgula, cabeçalho opcional):
        Cd_obj|cd_inst|nr_cpr|dt_mov
        1230|1,2,3|202609|2026-09-01
    cd_inst vai só para a controle (Db2); nr_cpr filtra o REPLACE WHERE da FATO. Data de movimento yyyy-MM-dd.
    ERRO_CTL: outro número de campos (os formatos anteriores não valem mais), mais de 1 linha de dados, lista vazia,
    não numérica ou com 0 junto com outros, código ou data diferentes do nome."""
    texto = texto.lstrip("\ufeff").replace("\r", "").strip()
    linhas = [l.strip() for l in texto.split("\n") if l.strip()]
    if linhas and not linhas[0].split("|")[0].strip().isdigit():
        linhas = linhas[1:]                             # cabeçalho (Cd_obj|cd_inst|nr_cpr|dt_mov)
    if len(linhas) != 1:
        raise ErroCarga("ERRO_CTL", f"CTL com {len(linhas)} linha(s) de dados (esperado 1): {texto[:200]}")
    partes = [p.strip() for p in linhas[0].split("|")]
    if len(partes) != 4:
        raise ErroCarga("ERRO_CTL", f"CTL com {len(partes)} campo(s) separados por '|' "
                                    f"(esperado 4: cd_obj|cd_inst|nr_cpr|dt_mov): {linhas[0][:200]}")
    cod, insts, cprs, dt = partes
    try:
        dt_mov = time.strftime("%Y%m%d", time.strptime(dt, "%Y-%m-%d"))
    except ValueError:
        raise ErroCarga("ERRO_CTL", f"data de movimento inválida no CTL: {dt!r} (esperado yyyy-MM-dd)")
    if not cod.isdigit() or int(cod) != int(arq["cod"]) or dt_mov != arq["dt_mov"]:
        raise ErroCarga("ERRO_CTL", f"conteúdo (objeto {cod}, data {dt}) diferente do nome "
                                    f"(objeto {arq['cod']}, data {arq['dt_mov']})")
    return _lista_ctl(insts, "cd_inst"), _lista_ctl(cprs, "nr_cpr")


def validar_ctl(arq, meta_header=True):
    """Conteúdo do CTL x nome do arquivo + DAT presente e não vazio. Devolve (instituições, nr_cpr, tamanho do DAT)."""
    try:
        texto = CTX["dbutils"].fs.head(arq["ctl"])
    except Exception as e:
        raise ErroCarga("ERRO_CTL", f"não consegui ler o CTL: {e}")
    insts, cprs = ler_conteudo_ctl(texto, arq)
    try:
        tamanho = resolver_dat(arq)                          # .dat ou .DAT; 1 arquivo: não lista a pasta inteira
    except Exception:
        tamanho = None
    if tamanho is None:
        raise ErroCarga("ERRO_PAR", f"CTL sem o DAT {arq['nome_dat']} (procurado com a extensão em qualquer caixa)")
    # DAT em branco = erro. Precisa ser ANTES da carga: um REPLACE WHERE com 0 linhas apagaria a data inteira.
    if tamanho == 0:
        raise ErroCarga("ERRO_DAT_VAZIO", f"DAT {arq['nome_dat']} com 0 bytes")
    inicio = CTX["dbutils"].fs.head(arq["dat"], 65536)
    linhas = [l for l in inicio.split("\n") if l.strip()]
    if len(linhas) < (2 if meta_header else 1):
        raise ErroCarga("ERRO_DAT_VAZIO", f"DAT {arq['nome_dat']} sem registros (só o header)" if linhas
                        else f"DAT {arq['nome_dat']} em branco")
    return insts, cprs, tamanho


def info_arquivo(arq):
    """UMA consulta ao log: último status do arquivo, se ele já foi CARREGADO (retomada), linhas carregadas
    e a última geração carregada do objeto (regra de sequência)."""
    r = sql(f"""SELECT max_by(qtd_inserida, evento_ts) FILTER (WHERE arquivo_ctl = :a AND status = 'CARREGADO') AS qtd,
                       count(*) FILTER (WHERE arquivo_ctl = :a AND status = 'CARREGADO')                    AS carregado,
                       max(date_format(data_geracao, 'yyyyMMddHHmmss')) FILTER (WHERE status = 'CARREGADO') AS ultima
                FROM {fq('log_carga_evento')} WHERE cod_objeto = :c""", a=arq["nome_ctl"], c=arq["cod"]).first()
    return (r.qtd, bool(r.carregado), r.ultima) if r else (None, False, None)


def validacoes_fato(df, meta, arq, cprs):
    """Data de movimento = CTL e nr_cpr na lista do CTL. Violação aborta a leitura (antes do commit)."""
    if meta["tipo"] != "FATO":
        return df
    col_d = F.col(meta["col_data"])
    tipo_d = meta["schema"][meta["col_data"]].dataType
    alvo = (F.to_date(F.lit(arq["dt_mov"]), "yyyyMMdd") if isinstance(tipo_d, T.DateType)
            else F.lit(arq["dt_mov"]).cast(tipo_d))
    regra = F.when(col_d != alvo, F.raise_error(F.concat(
        F.lit(f"ERRO_DADOS: {meta['col_data']} = "), col_d.cast("string"), F.lit(f" diferente do CTL ({arq['dt_mov']})"))))
    if cprs != "0":
        lista = [int(x) for x in cprs.split(",")]
        col_c = F.col(meta["col_cpr"])
        regra = regra.when(col_c.isNull() | ~col_c.cast("bigint").isin(lista), F.raise_error(F.concat(
            F.lit(f"ERRO_DADOS: {meta['col_cpr']} = "), F.coalesce(col_c.cast("string"), F.lit("NULL")),
            F.lit(f" fora da lista nr_cpr do CTL ({cprs})"))))
    return df.filter(regra.otherwise(F.lit(True)))


_detector_feito = set()


def precheck(arq, meta, cprs, tamanho, linhas=None):
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
    amostra = validacoes_fato(ler_dat(arq, meta).limit(linhas), meta, arq, cprs)
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
    if tipo == "ERRO_DADOS" and arq.get("limpeza_modo") == "DIRETA" and "ERRO_DADOS:" not in msg:
        try:
            ruim = primeira_linha_invalida_direta(arq, meta)
            if ruim is not None:
                return tipo, ("ERRO_DADOS: linha inválida depois da limpeza (campos a mais/menos, tipo incompatível ou "
                              f"registro em mais de uma linha: use limpeza_modo AUTO ou MULTILINHA). Linha: {ruim[:400]}")
        except Exception as diag:
            print(f"diagnóstico do erro não foi possível: {diag}")
        return tipo, msg
    sem_diagnostico = not arq.get("dat_limpo") and _p(meta, "escape") and not _p(meta, "quote")
    if tipo != "ERRO_DADOS" or "FAILED_READ_FILE" not in msg or sem_diagnostico:
        return tipo, msg
    try:
        sep = _p(meta, "delimitador", "|")
        n = len(meta["schema"].fields)
        esquema = T.StructType(meta["schema"].fields + [T.StructField("_corrompida", T.StringType())])
        caminho, o = _leitura(arq, meta)                     # mesmas opções da carga (DAT ou arquivo limpo)
        ruim = (CTX["spark"].read.format("csv").schema(esquema).options(**o)
                .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrompida")
                .load(caminho).filter("_corrompida IS NOT NULL")
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
def carregar(arq, meta, cprs):
    """FATO: REPLACE WHERE data (+ nr_cpr do CTL). DIM: substitui tudo. Um único commit Delta."""
    t = meta["tabela"]
    if meta["tipo"] == "FATO":
        tipo_d = meta["schema"][meta["col_data"]].dataType
        valor = f"DATE'{arq['dt_mov'][:4]}-{arq['dt_mov'][4:6]}-{arq['dt_mov'][6:]}'" \
            if isinstance(tipo_d, T.DateType) else (f"'{arq['dt_mov']}'" if isinstance(tipo_d, T.StringType) else arq["dt_mov"])
        predicado = f"{meta['col_data']} = {valor}"
        if cprs != "0":
            tipo_c = meta["schema"][meta["col_cpr"]].dataType
            col_c = meta["col_cpr"] if isinstance(tipo_c, T.NumericType) else f"CAST({meta['col_cpr']} AS BIGINT)"
            predicado += f" AND {col_c} IN ({cprs})"
    else:
        predicado = "1 = 1"     # DIM: substitui a tabela inteira, mantendo tags, propriedades e grants
    df = validacoes_fato(ler_dat(arq, meta), meta, arq, cprs)
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


# Dados de um arquivo que os comandos da controle podem usar, como marcadores :nome (o motor troca por "?" e
# manda os valores na ordem em que aparecem). :lista_cd_inst = cd_inst do CTL como lista para IN (...): vira um
# "?" por instituição (inteiros); "0" (todas) vira IN (0).
MARCADORES_DB2 = ("cod_objeto", "nome_tabela", "data_movimento", "data_geracao", "instituicoes", "lista_cd_inst",
                  "status", "qtd_registros", "arquivo", "job_run_id", "lote_id", "modo_execucao", "dt_inicio",
                  "dt_fim", "mensagem")
_LISTA_DB2 = "{lista_cd_inst}"           # lugar da lista no comando montado; vira "?, ?, ?" em montar_sql_db2
_RX_SECAO_DB2 = re.compile(r"^--\s*\[(CONCLUIDO|ERRO|EM_PROCESSAMENTO)\]\s*$", re.MULTILINE)


class SemLinhaControle(Exception):
    """O UPDATE da controle não encontrou nenhuma linha: não adianta tentar de novo."""


def ler_comandos_db2(texto, tabela, com_inicio=False, nome="db2_comando"):
    """Comandos da controle por status, a partir do arquivo (editável pelo DBA). Cada status tem uma seção
    "-- [CONCLUIDO]", "-- [ERRO]" (obrigatórias) e "-- [EM_PROCESSAMENTO]" (só com db2_registrar_inicio = true)
    com UM comando UPDATE. Qualquer outro comando (MERGE, INSERT, DELETE...) é recusado.
    Devolve {status: (sql com "?", [marcadores na ordem])}."""
    partes = _RX_SECAO_DB2.split(texto)
    comandos = {}
    for status, corpo in zip(partes[1::2], partes[2::2]):
        sql_ = re.sub(r"/\*.*?\*/", " ", corpo, flags=re.S)                     # comentários /* */
        sql_ = "\n".join(l for l in sql_.splitlines() if not l.strip().startswith("--")).strip().rstrip(";").strip()
        if not re.match(r"UPDATE\s", sql_, re.IGNORECASE):
            raise ValueError(f"{nome} [{status}]: só UPDATE é permitido na controle (veio: {sql_[:60]!r})")
        if ";" in sql_ or "?" in sql_:
            raise ValueError(f"{nome} [{status}]: um comando só, com marcadores :nome (sem ';' nem '?')")
        marcadores = re.findall(r":([a-z_]+)\b", sql_)
        desconhecidos = sorted(set(marcadores) - set(MARCADORES_DB2))
        if desconhecidos:
            raise ValueError(f"{nome} [{status}]: marcador desconhecido {desconhecidos} (use {', '.join(MARCADORES_DB2)})")
        sql_ = re.sub(r":([a-z_]+)\b", lambda m: _LISTA_DB2 if m.group(1) == "lista_cd_inst" else "?", sql_)
        comandos[status] = (sql_.replace("{tabela}", tabela), marcadores)
    faltam = [s for s in ("CONCLUIDO", "ERRO") + (("EM_PROCESSAMENTO",) if com_inicio else ()) if s not in comandos]
    if faltam:
        raise ValueError(f"{nome}: falta a seção {', '.join('-- [' + f + ']' for f in faltam)}")
    return comandos


def lista_cd_inst(insts):
    """cd_inst do CTL ("1,2,3" ou "0") -> [1, 2, 3] ou [0]. Desconhecido (erro antes de ler o CTL) = [0]."""
    return [int(v) for v in (insts or "0").split(",")]


def montar_sql_db2(sql_, marcadores, dados):
    """Comando com "?" e valores na ordem; cada :lista_cd_inst vira um "?" por instituição."""
    lista = dados.get("lista_cd_inst") or [0]
    valores = []
    for m in marcadores:
        valores += lista if m == "lista_cd_inst" else [dados[m]]
    return sql_.replace(_LISTA_DB2, ", ".join("?" * len(lista))), tuple(valores)


def _comandos_db2():
    """Comandos da controle (sql/<db2_comando>), lidos e conferidos uma vez por processo."""
    import os
    with _trava_cache:
        if "comandos" not in _db2:
            caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql", CTX["db2_comando"])
            _db2["comandos"] = ler_comandos_db2(open(caminho, encoding="utf-8").read(), CTX["db2_tabela"],
                                                str(CTX["db2_registrar_inicio"]).lower() == "true", CTX["db2_comando"])
    return _db2["comandos"]


def _conexao_db2():
    """Abre UMA conexão nova com o Db2 (driver Python oficial da IBM, ibm_db). Usuário e senha vêm do secret
    scope; host, porta, banco, SSL e certificado das variáveis do bundle. Quem usa é o pool abaixo."""
    try:
        import ibm_db_dbi
    except ImportError as e:
        # o driver vem do environment do job (dependência ibm-db em resources/jobs.yml)
        import platform
        raise RuntimeError(f"driver ibm_db indisponível nesta máquina ({platform.machine()}): {e} · confira a "
                           "dependência ibm-db no environment do job (resources/jobs.yml)") from e
    sec = CTX["dbutils"].secrets
    dsn = (f"DATABASE={CTX['db2_banco']};HOSTNAME={CTX['db2_host']};PORT={CTX['db2_porta']};PROTOCOL=TCPIP;"
           f"UID={sec.get(CTX['db2_secret_scope'], CTX['db2_chave_usuario'])};"
           f"PWD={sec.get(CTX['db2_secret_scope'], CTX['db2_chave_senha'])};")
    if str(CTX["db2_ssl"]).lower() == "true":
        dsn += "SECURITY=SSL;"
        if CTX["db2_certificado"]:
            dsn += f"SSLServerCertificate={CTX['db2_certificado']};"
    # Sem tempo-limite, uma rede que descarta os pacotes ou um lock na controle deixam o job esperando sem fim.
    limite = int(CTX["db2_timeout_s"] or 30)
    con = ibm_db_dbi.connect(dsn + f"ConnectTimeout={limite};ReceiveTimeout={limite};" + (CTX["db2_extra"] or ""),
                             "", "")
    cur = con.cursor()
    cur.execute(f"SET CURRENT LOCK TIMEOUT {limite}")
    cur.close()
    return con


def _com_limite(funcao, segundos, o_que):
    """Executa funcao() com um limite duro de tempo. O ConnectTimeout do driver só cobre a abertura TCP: uma
    negociação SSL pendurada (porta sem SSL, proxy) ficaria minutos esperando. Passou do limite: TimeoutError,
    e a conexão que porventura abrir depois é fechada."""
    res, pronto, abandonada = {}, threading.Event(), threading.Event()

    def alvo():
        try:
            res["ok"] = funcao()
            if abandonada.is_set():
                res["ok"].close()
        except Exception as e:
            res["erro"] = e
        finally:
            pronto.set()
    threading.Thread(target=alvo, daemon=True).start()
    if not pronto.wait(segundos):
        abandonada.set()
        raise TimeoutError(f"{o_que}: sem resposta em {segundos:.0f} s (rede, porta ou negociação SSL)")
    if "erro" in res:
        raise res["erro"]
    return res["ok"]


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
                self.reusada = True
                return self.con
            _pool_db2["abertas"] += 1
        self.reusada = False
        try:
            limite = int(CTX["db2_timeout_s"] or 30)
            self.con = _com_limite(_conexao_db2, 2 * limite + 5, f"conexão com o Db2 {CTX['db2_host']}:{CTX['db2_porta']}")
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
    """Executa o UPDATE da controle para este status (seção [status] de sql/<db2_comando>), com os marcadores
    :nome trocados pelos dados do arquivo. 0 linhas atualizadas = SemLinhaControle (nada é inserido)."""
    sql_, marcadores = _comandos_db2()[status]
    insts = insts or "0"                     # erro antes de ler o CTL: cd_inst desconhecido = 0 (todas)
    g = arq["dt_ger"]                        # yyyyMMddHHmmss do nome do arquivo
    dados = {"cod_objeto": arq["cod"], "nome_tabela": arq["tabela_nome"][:128], "data_movimento": int(arq["dt_mov"]),
             "data_geracao": f"{g[:4]}-{g[4:6]}-{g[6:8]} {g[8:10]}:{g[10:12]}:{g[12:14]}",
             "instituicoes": insts[:4000], "lista_cd_inst": lista_cd_inst(insts), "status": status,
             "qtd_registros": qtd, "arquivo": arq["nome_ctl"][:256], "job_run_id": CTX["job_run_id"],
             "lote_id": CTX["lote_id"], "modo_execucao": modo, "dt_inicio": _horario(inicio), "dt_fim": _horario(fim),
             "mensagem": (mensagem or "")[:int(CTX["db2_mensagem_max"] or 500)] or None}
    sql_, valores = montar_sql_db2(sql_, marcadores, dados)
    print(f"[{time.strftime('%H:%M:%S')}] controle {status} de {arq['nome_ctl']}: gravando no Db2", flush=True)
    t0 = time.time()
    for tentativa in (1, 2):                 # 2ª tentativa com conexão nova, só se a 1ª veio do pool (ociosa derrubada)
        pool = conexao_db2()
        try:
            with pool as con:
                cur = con.cursor()
                cur.execute(sql_, valores)
                linhas = cur.rowcount
                cur.close()
                if linhas == 0:
                    con.rollback()
                    raise SemLinhaControle(f"o UPDATE da controle ({status}) não encontrou a linha do objeto "
                                           f"{arq['cod']} (cd_inst {insts}) em {CTX['db2_tabela']} "
                                           f"(0 linhas; o motor não insere)")
                con.commit()
            print(f"[{time.strftime('%H:%M:%S')}] controle {status} de {arq['nome_ctl']}: ok, {linhas} linha(s) "
                  f"em {time.time() - t0:.1f} s", flush=True)
            return
        except SemLinhaControle:
            raise
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] controle {status} de {arq['nome_ctl']}: falhou em "
                  f"{time.time() - t0:.1f} s: {e}", flush=True)
            if tentativa == 2 or not getattr(pool, "reusada", False):
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


def usa_db2():
    """Parâmetro usar_db2: sim = executa as etapas da controle; nao = pula todas (nem conecta no Db2)."""
    return CTX.get("usar_db2", "sim") == "sim"


def sensibilizar_controle(arq, status, insts, modo, qtd=None, inicio=None, fim=None, mensagem=None, tentativas=None):
    """Grava o status DESTE arquivo na controle: CONCLUIDO ou ERRO (e EM_PROCESSAMENTO, com db2_registrar_inicio).
    Síncrono, com novas tentativas (espera crescente). Falhou todas: lança FalhaControle.
    tentativas: padrão = parâmetro db2_tentativas. Com usar_db2 = nao, não faz nada."""
    if not usa_db2():
        return
    tentativas = tentativas or int(CTX["db2_tentativas"])
    base = float(CTX.get("db2_espera_base_s") or 30)
    erro = None
    for t in range(tentativas):
        try:
            _gravar_controle(arq, status, insts, modo, qtd, inicio, fim, mensagem)
            return
        except (ValueError, FileNotFoundError) as e:   # comando/configuração errados: não adianta tentar de novo
            raise FalhaControle(f"configuração da controle inválida: {e}")
        except SemLinhaControle as e:                  # a linha não existe: tentar de novo não muda nada
            raise FalhaControle(str(e))
        except Exception as e:
            erro = e
            print(f"controle ({status}): tentativa {t + 1} de {tentativas} falhou: {e}")
            try:                                       # mensagem completa no log já na hora (não só no fim)
                evento(arq, "ALERTA_DB2", mensagem=f"controle {status}, tentativa {t + 1} de {tentativas}: {e}")
            except Exception as e2:
                print(f"não consegui gravar o ALERTA_DB2: {e2}")
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
    tamanho, meta, insts, cprs = None, None, None, None
    pend = []                                           # eventos deste arquivo ainda não gravados
    comum = lambda: {"fila": modo, "tipo_job": tipo_job, "tabela": meta and meta["tabela"], "tamanho": tamanho}
    if roteado_ts:
        pend.append((arq, "ROTEADO", {"fila": modo, "tipo_job": tipo_job, "ts": roteado_ts}))
    carregou = False
    try:
        meta = metadados(arq["cod"])                    # ERRO_CADASTRO se a ctl_parametros não bate com o destino
        insts, cprs, tamanho = validar_ctl(arq, _p(meta, "header", "true") == "true")   # insts: só para a controle
        qtd_anterior, ja_carregado, ultima = info_arquivo(arq)

        # 1) controle: este arquivo começou. Só com db2_registrar_inicio = true: aí, sem o início gravado, não
        #    carrega. Padrão (false): o Db2 só recebe o fim (CONCLUIDO ou ERRO); o início fica no log (INICIADO).
        #    Se o Db2 estiver fora, a carga acontece e só a conclusão falha (ERRO_DB2): os arquivos ficam em raw/
        #    e, depois do DESBLOQUEIO, a retomada refaz só a conclusão, sem carregar de novo.
        if usa_db2() and str(CTX["db2_registrar_inicio"]).lower() == "true":
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
            if ultima and arq["dt_ger"] <= ultima:
                raise ErroCarga("ERRO_SEQUENCIA", f"geração {arq['dt_ger']} não é posterior à última carregada {ultima}")
            limpeza = None
            if limpeza_ativa(meta):                     # pedido do cliente: limpa o DAT antes da carga
                modo_l, t_limpeza, detalhe = preparar_dat(arq, meta)
                limpeza = (f"limpeza do DAT em {t_limpeza:.1f} s (modo {modo_l}"
                           + (f"; {detalhe}" if detalhe else "") + ")")
            precheck(arq, meta, cprs, tamanho)
            versao, inseridas, removidas = carregar(arq, meta, cprs)
            carregou = True
            pend.append((arq, "CARREGADO", {**comum(), "instituicoes": insts, "nr_cpr": cprs, "inicio": inicio, "fim": time.time(),
                                            "ts": time.time(), "qtd_inserida": inseridas, "qtd_removida": removidas,
                                            "versao": versao, "mensagem": limpeza}))

        # 2) controle: este arquivo terminou (com a quantidade). Se a controle falhar: ERRO_DB2 e trava.
        #    Com usar_db2 = nao, pula a controle (e o CONFIRMADO_DB2) e conclui direto.
        fim = time.time()
        confirmacao = []
        if usa_db2():
            try:
                sensibilizar_controle(arq, "CONCLUIDO", insts, modo, qtd=inseridas, inicio=inicio, fim=fim)
            except FalhaControle as e:
                raise ErroCarga("ERRO_DB2", f"controle não registrou a conclusão: {e}", mover=False)
            confirmacao = [(arq, "CONFIRMADO_DB2", {**comum(), "ts": time.time()})]
        confirmado = time.time()
        eventos(pend + confirmacao + [(arq, "CONCLUIDO", {**comum(), "ts": max(time.time(), confirmado + 0.001),
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
            mover = False                               # dados já no Delta: arquivos ficam nas pastas de entrada para a retomada
        fim = time.time()
        eventos(pend + [(arq, tipo, {**comum(), "inicio": inicio, "fim": fim, "mensagem": msg})])
        # 3) controle: este arquivo deu erro (melhor esforço: se a própria controle estiver fora, só registra no log;
        #    com usar_db2 = nao, sensibilizar_controle não faz nada)
        try:
            sensibilizar_controle(arq, "ERRO", insts, modo, inicio=inicio, fim=fim, mensagem=f"{tipo}: {msg}",
                                  tentativas=1 if tipo == "ERRO_DB2" else 2)
        except Exception as e2:
            print(f"controle não recebeu o ERRO de {arq['nome_ctl']}: {e2}")
        if mover:
            mover_para_erro(arq)
        raise ErroCarga(tipo, msg, mover=False)
    finally:
        apagar_tmp(arq)                                 # o arquivo limpo é apagado sempre (sucesso ou erro)


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
