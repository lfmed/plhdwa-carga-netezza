-- =====================================================================================
-- PLHDWA · tabelas e visões operacionais do motor de carga Netezza -> Databricks
--
-- Executado pelo job "PLHDWA · pós-deploy" (src/pos_deploy.py), que troca os marcadores abaixo pelas
-- variáveis do bundle (databricks.yml):
--   ${catalogo}.${schema}          onde ficam as tabelas operacionais (o catálogo precisa existir)
--   ${owner_tag}                   valor da tag OWNER
--   ${max_simult_despachante}      vagas iniciais do modo DESPACHANTE (só na 1ª instalação)
--   ${max_simult_job}              vagas iniciais do modo JOB          (só na 1ª instalação)
-- Pode rodar quantas vezes quiser: tudo é CREATE ... IF NOT EXISTS / CREATE OR REPLACE VIEW, e os limites
-- iniciais só entram se ctl_execucao estiver vazia (depois disso, quem manda é o owner).
--
-- Os parâmetros de cada OBJETO (destino, tipo de carga, colunas do REPLACE WHERE, modo de execução, formato do
-- arquivo e limpeza) ficam na ctl_parametros (item 1b). Da tabela destino o motor só usa o layout (colunas e tipos).
-- Ver ferramentas/converter_ddl_netezza.py, que gera a linha de cada objeto a partir da DDL do Netezza.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS ${catalogo}.${schema};

ALTER SCHEMA ${catalogo}.${schema} SET TAGS ('OWNER' = '${owner_tag}');

-- 1) Vagas por modo de execução. O modo de cada objeto é a coluna execucao da ctl_parametros
--    (DESPACHANTE = thread dentro do despachante · JOB = job independente). Só o owner altera.
--    O pós-deploy grava os job_id dos jobs independentes (Performance e Standard).
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.ctl_execucao (
  modo          STRING     COMMENT 'DESPACHANTE (threads dentro do despachante) | JOB (job independente)',
  max_simult    INT        COMMENT 'objetos (= arquivos, 1 por objeto) processando ao mesmo tempo neste modo',
  job_id_perf   BIGINT     COMMENT 'modo JOB: job dos objetos com tipo_job = PERFORMANCE_OPTIMIZED (ctl_parametros)',
  job_id_std    BIGINT     COMMENT 'modo JOB: job dos objetos com tipo_job = STANDARD (ctl_parametros)',
  alterado_por  STRING,
  alterado_em   TIMESTAMP
) COMMENT 'PLHDWA · limites de execução por modo (relido pelo despachante a cada ciclo)';

ALTER TABLE ${catalogo}.${schema}.ctl_execucao SET TAGS ('OWNER' = '${owner_tag}');

INSERT INTO ${catalogo}.${schema}.ctl_execucao
SELECT * FROM VALUES
  ('DESPACHANTE', ${max_simult_despachante}, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), current_user(), current_timestamp()),
  ('JOB',         ${max_simult_job},         CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), current_user(), current_timestamp())
WHERE NOT EXISTS (SELECT 1 FROM ${catalogo}.${schema}.ctl_execucao);

-- 1b) Parâmetros de cada objeto (uma linha por código de objeto do CTL). Relida pelo motor a cada minuto: um
--     UPDATE vale sem deploy. Coluna vazia (NULL) = padrão do motor. Os CHECK recusam valor inválido no próprio
--     INSERT/UPDATE. O Delta não impede código duplicado: o motor confere e dá ERRO_CADASTRO.
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.ctl_parametros (
  cod_objeto        INT     NOT NULL COMMENT 'código do objeto (1º campo do nome e do conteúdo do CTL)',
  catalogo_destino  STRING  NOT NULL COMMENT 'catálogo da tabela destino',
  schema_destino    STRING  NOT NULL COMMENT 'schema da tabela destino',
  tabela_destino    STRING  NOT NULL COMMENT 'tabela destino (o layout, colunas e tipos, vem dela)',
  filtrotabela      STRING  NOT NULL COMMENT 'SIM = REPLACE WHERE pela data de movimento (+ nr_cpr do CTL) · NAO = substitui a tabela inteira',
  nomecolunatempo   STRING           COMMENT 'filtrotabela = SIM: coluna da data de movimento (REPLACE WHERE)',
  nomecolunainstituicao STRING       COMMENT 'filtrotabela = SIM: coluna comparada com o nr_cpr do CTL (REPLACE WHERE, se o CTL trouxer lista; NULL = NR_CPR)',
  execucao          STRING  NOT NULL DEFAULT 'DESPACHANTE' COMMENT 'DESPACHANTE (thread no despachante) | JOB (job independente)',
  tipo_job          STRING  NOT NULL DEFAULT 'PERFORMANCE_OPTIMIZED' COMMENT 'modo JOB: PERFORMANCE_OPTIMIZED | STANDARD',
  ativo             BOOLEAN NOT NULL DEFAULT true COMMENT 'false = os arquivos do objeto ficam parados na pasta de CTL',
  delimitador       STRING           COMMENT 'arquivo: separador de campos (NULL = |)',
  encoding          STRING           COMMENT 'arquivo: encoding (NULL = ISO-8859-15)',
  header            BOOLEAN          COMMENT 'arquivo: 1ª linha é o cabeçalho (NULL = true)',
  escape            STRING           COMMENT 'arquivo: caractere de escape (NULL = sem)',
  quote             STRING           COMMENT 'arquivo: aspas dos textos (NULL = sem)',
  null_value        STRING           COMMENT 'arquivo: texto que vale NULL (NULL = vazio)',
  date_format       STRING           COMMENT 'arquivo: formato das datas (NULL = yyyy-MM-dd)',
  timestamp_format  STRING           COMMENT 'arquivo: formato dos timestamps (NULL = yyyy-MM-dd HH:mm[:ss])',
  limpeza           BOOLEAN          COMMENT 'limpeza do DAT antes da carga (NULL = variável limpeza_dat)',
  limpeza_modo      STRING           COMMENT 'CLIENTE | AUTO | PARALELA | DIRETA | MULTILINHA (NULL = CLIENTE)',
  limpeza_quote     STRING           COMMENT 'aspas dos textos na leitura do arquivo limpo (NULL = aspa simples)',
  tabela_origem     STRING           COMMENT 'tabela externa de origem no Netezza (informativo)',
  observacao        STRING,
  alterado_por      STRING,
  alterado_em       TIMESTAMP
) COMMENT 'PLHDWA · parâmetros de cada objeto: destino, carga, execução, formato do arquivo e limpeza'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE ${catalogo}.${schema}.ctl_parametros SET TAGS ('OWNER' = '${owner_tag}');

ALTER TABLE ${catalogo}.${schema}.ctl_parametros DROP CONSTRAINT IF EXISTS ck_filtrotabela;
ALTER TABLE ${catalogo}.${schema}.ctl_parametros ADD CONSTRAINT ck_filtrotabela CHECK (filtrotabela IN ('SIM', 'NAO'));
ALTER TABLE ${catalogo}.${schema}.ctl_parametros DROP CONSTRAINT IF EXISTS ck_filtro_colunas;
ALTER TABLE ${catalogo}.${schema}.ctl_parametros ADD CONSTRAINT ck_filtro_colunas CHECK (filtrotabela = 'NAO' OR coalesce(nomecolunatempo, '') <> '');
ALTER TABLE ${catalogo}.${schema}.ctl_parametros DROP CONSTRAINT IF EXISTS ck_execucao;
ALTER TABLE ${catalogo}.${schema}.ctl_parametros ADD CONSTRAINT ck_execucao CHECK (execucao IN ('DESPACHANTE', 'JOB'));
ALTER TABLE ${catalogo}.${schema}.ctl_parametros DROP CONSTRAINT IF EXISTS ck_tipo_job;
ALTER TABLE ${catalogo}.${schema}.ctl_parametros ADD CONSTRAINT ck_tipo_job CHECK (tipo_job IN ('PERFORMANCE_OPTIMIZED', 'STANDARD'));
ALTER TABLE ${catalogo}.${schema}.ctl_parametros DROP CONSTRAINT IF EXISTS ck_limpeza_modo;
ALTER TABLE ${catalogo}.${schema}.ctl_parametros ADD CONSTRAINT ck_limpeza_modo CHECK (limpeza_modo IS NULL OR limpeza_modo IN ('CLIENTE', 'AUTO', 'PARALELA', 'DIRETA', 'MULTILINHA'));

-- 2) Log de eventos por arquivo: só INSERT (vários workers gravando ao mesmo tempo, sem conflito).
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.log_carga_evento (
  evento_ts        TIMESTAMP,
  job_id           BIGINT,
  job_run_id       BIGINT    COMMENT 'execução que gravou o evento: liga com system.billing.usage',
  fila             STRING    COMMENT 'modo de execução: DESPACHANTE | JOB',
  tipo_job         STRING    COMMENT 'PERFORMANCE_OPTIMIZED | STANDARD',
  cod_objeto       INT,
  tabela_destino   STRING,
  arquivo_ctl      STRING,
  arquivo_dat      STRING,
  tamanho_bytes    BIGINT,
  data_referencia  INT       COMMENT 'data de movimento como no arquivo (yyyyMMdd)',
  data_geracao     TIMESTAMP COMMENT 'do nome do arquivo, no fuso do parâmetro fuso_horario',
  instituicoes     STRING    COMMENT 'cd_inst do CTL (vai para a controle): "0" = todas, ou lista separada por vírgula',
  nr_cpr           STRING    COMMENT 'nr_cpr do CTL (REPLACE WHERE da FATO): "0" = todos, ou lista separada por vírgula',
  status           STRING    COMMENT 'ROTEADO | INICIADO | CARREGADO | CONFIRMADO_DB2 | CONCLUIDO | ERRO_* | ALERTA_FORMATO | DESBLOQUEIO',
  inicio           TIMESTAMP COMMENT 'início do processamento do arquivo',
  fim              TIMESTAMP COMMENT 'fim do processamento do arquivo',
  qtd_inserida     BIGINT,
  qtd_removida     BIGINT,
  versao_delta     BIGINT,
  mensagem         STRING,
  usuario          STRING,
  lote_id          BIGINT    COMMENT 'lote = execução do despachante que tratou o arquivo'
) CLUSTER BY (evento_ts, cod_objeto)
COMMENT 'PLHDWA · log append-only de eventos por arquivo';

ALTER TABLE ${catalogo}.${schema}.log_carga_evento SET TAGS ('OWNER' = '${owner_tag}');

-- 3) Lotes: início e fim de cada execução do despachante.
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.log_lote (
  lote_id      BIGINT    COMMENT 'job_run_id do despachante',
  evento_ts    TIMESTAMP,
  status       STRING    COMMENT 'INICIADO | FINALIZADO | INTERROMPIDO (registrado pelo lote seguinte)',
  pendentes    INT       COMMENT 'CTLs pendentes no início',
  mensagem     STRING
) COMMENT 'PLHDWA · lotes (execuções do despachante)';

ALTER TABLE ${catalogo}.${schema}.log_lote SET TAGS ('OWNER' = '${owner_tag}');

-- 4) Foto de cada ciclo do despachante, por modo (alimenta backlog, ocupação e projeção no dashboard).
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.log_ciclo_despachante (
  ciclo_ts         TIMESTAMP,
  job_run_id       BIGINT,
  fila             STRING  COMMENT 'modo de execução: DESPACHANTE | JOB',
  pendentes_qtd    INT,
  pendentes_bytes  BIGINT,
  em_andamento     INT,
  max_simult       INT
) COMMENT 'PLHDWA · backlog e ocupação por modo a cada ciclo';

ALTER TABLE ${catalogo}.${schema}.log_ciclo_despachante SET TAGS ('OWNER' = '${owner_tag}');

-- 4b) Jobs independentes ativos em cada ciclo (lidos da API de Jobs): mostra no dashboard QUAIS são, inclusive
--     os que ainda estão na fila do Jobs, arrancando ou entre um arquivo e outro do mesmo objeto.
--     Mesmo ciclo_ts da foto em log_ciclo_despachante.
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.log_job_ativo (
  ciclo_ts     TIMESTAMP,
  lote_id      BIGINT    COMMENT 'lote (execução do despachante) que viu o job ativo',
  cod_objeto   INT,
  job_run_id   BIGINT    COMMENT 'execução do job independente',
  tipo_job     STRING    COMMENT 'PERFORMANCE_OPTIMIZED | STANDARD',
  estado_run   STRING    COMMENT 'estado no Jobs: QUEUED | PENDING | RUNNING | BLOCKED',
  inicio_run   TIMESTAMP COMMENT 'início da execução no Jobs (vazio enquanto está na fila)'
) CLUSTER BY (ciclo_ts)
COMMENT 'PLHDWA · jobs independentes ativos a cada ciclo do despachante';

ALTER TABLE ${catalogo}.${schema}.log_job_ativo SET TAGS ('OWNER' = '${owner_tag}');

-- 5) Histórico da controle (Db2 simulado): 1 linha por mudança de status de cada arquivo.
--    O estado atual fica no Lakebase (parâmetros lakebase_*); no Db2 real, fica no Db2.
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.db2_controle_sim (
  cod_objeto       INT,
  nome_tabela      STRING,
  data_movimento   INT,
  data_geracao     TIMESTAMP,
  instituicoes     STRING,
  status           STRING    COMMENT 'EM_PROCESSAMENTO | ERRO | CONCLUIDO',
  qtd_registros    BIGINT,
  arquivo          STRING,
  job_run_id       BIGINT,
  confirmado_em    TIMESTAMP,
  lote_id          BIGINT,
  modo_execucao    STRING,
  dt_inicio        TIMESTAMP,
  dt_fim           TIMESTAMP,
  mensagem         STRING
) COMMENT 'PLHDWA · histórico da controle (início, erro e conclusão de cada arquivo)';

ALTER TABLE ${catalogo}.${schema}.db2_controle_sim SET TAGS ('OWNER' = '${owner_tag}');

-- 6) Só para testes: objetos listados aqui simulam a controle (Db2) fora do ar.
CREATE TABLE IF NOT EXISTS ${catalogo}.${schema}.db2_sim_falhas (cod_objeto INT)
COMMENT 'PLHDWA · TESTE: objetos listados aqui simulam o Db2 indisponível';

ALTER TABLE ${catalogo}.${schema}.db2_sim_falhas SET TAGS ('OWNER' = '${owner_tag}');

-- =====================================================================================
-- Visões operacionais (lidas pelo motor e pelos dashboards)
-- =====================================================================================

-- Último status de cada arquivo (ALERTA_FORMATO não muda o status).
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_status_arquivo AS
SELECT arquivo_ctl,
       max_by(cod_objeto, evento_ts)  AS cod_objeto,
       max_by(status, evento_ts)      AS status,
       max_by(fila, evento_ts)        AS fila,
       max_by(mensagem, evento_ts)    AS mensagem,
       max(evento_ts)                 AS ultimo_evento,
       nullif(regexp_extract(arquivo_ctl, '^[0-9]+_([^_]+)_.+_[0-9]{8}_[0-9]{14}[.][cC][tT][lL]$', 1), '') AS grupo   -- 1º segmento do nome depois do código (só visualização)
FROM ${catalogo}.${schema}.log_carga_evento
WHERE status <> 'ALERTA_FORMATO'
GROUP BY arquivo_ctl;

-- Objeto travado = o último evento relevante do objeto é um ERRO_* (sem DESBLOQUEIO depois).
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_objeto_travado AS
SELECT cod_objeto,
       max_by(status, evento_ts)      AS erro,
       max_by(arquivo_ctl, evento_ts) AS arquivo_ctl,
       max_by(mensagem, evento_ts)    AS mensagem,
       max(evento_ts)                 AS desde,
       nullif(regexp_extract(max_by(arquivo_ctl, evento_ts), '^[0-9]+_([^_]+)_.+_[0-9]{8}_[0-9]{14}[.][cC][tT][lL]$', 1), '') AS grupo
FROM ${catalogo}.${schema}.log_carga_evento
WHERE status LIKE 'ERRO%' OR status = 'DESBLOQUEIO'
GROUP BY cod_objeto
HAVING max_by(status, evento_ts) LIKE 'ERRO%';

-- Arquivos concluídos: tempos de cada etapa, tamanho e registros.
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_arquivo_concluido AS
SELECT arquivo_ctl,
       max(cod_objeto)                                             AS cod_objeto,
       max(fila)                                                   AS fila,
       max(tipo_job)                                               AS tipo_job,
       max(job_run_id)                                             AS job_run_id,
       max(lote_id)                                                AS lote_id,
       max(tamanho_bytes)                                          AS tamanho_bytes,
       min(CASE WHEN status = 'ROTEADO'   THEN evento_ts END)      AS roteado_em,
       min(CASE WHEN status = 'INICIADO'  THEN evento_ts END)      AS iniciado_em,
       min(CASE WHEN status = 'CARREGADO' THEN inicio END)         AS inicio_carga,
       min(CASE WHEN status = 'CARREGADO' THEN fim END)            AS fim_carga,
       min(CASE WHEN status = 'CONCLUIDO' THEN evento_ts END)      AS concluido_em,
       max(qtd_inserida)                                           AS qtd_inserida,
       nullif(regexp_extract(arquivo_ctl, '^[0-9]+_([^_]+)_.+_[0-9]{8}_[0-9]{14}[.][cC][tT][lL]$', 1), '') AS grupo
FROM ${catalogo}.${schema}.log_carga_evento
GROUP BY arquivo_ctl
HAVING concluido_em IS NOT NULL;

-- Projeção de término: backlog do último ciclo ÷ vazão da última hora, por modo.
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_projecao_termino AS
WITH ultimo AS (
  SELECT fila, pendentes_bytes, pendentes_qtd, em_andamento, max_simult, ciclo_ts
  FROM ${catalogo}.${schema}.log_ciclo_despachante
  QUALIFY ciclo_ts = max(ciclo_ts) OVER ()
), vazao AS (
  SELECT fila, sum(tamanho_bytes) AS bytes_ultima_hora
  FROM ${catalogo}.${schema}.v_arquivo_concluido
  WHERE concluido_em >= current_timestamp() - INTERVAL 1 HOUR
  GROUP BY fila
)
SELECT u.fila, u.pendentes_qtd, u.pendentes_bytes, u.em_andamento, u.max_simult,
       v.bytes_ultima_hora,
       CASE WHEN u.pendentes_bytes = 0 THEN u.ciclo_ts
            WHEN coalesce(v.bytes_ultima_hora, 0) = 0 THEN NULL
            ELSE u.ciclo_ts + make_interval(0, 0, 0, 0, 0, 0, u.pendentes_bytes / v.bytes_ultima_hora * 3600)
       END AS termino_previsto
FROM ultimo u LEFT JOIN vazao v USING (fila);

-- Arquivos em execução agora: último status sem evento final (últimas 12 h).
-- estado ROTEADO = job independente arrancando; INICIADO/CARREGADO = processando.
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_em_execucao AS
SELECT arquivo_ctl,
       max(cod_objeto)                                       AS cod_objeto,
       max_by(status, evento_ts)                             AS estado,
       max(tabela_destino)                                   AS tabela_destino,
       max_by(fila, evento_ts)                               AS modo,
       max_by(tipo_job, evento_ts)                           AS tipo_job,
       max(lote_id)                                          AS lote_id,
       max_by(job_run_id, evento_ts)                         AS job_run_id,
       max(tamanho_bytes)                                    AS tamanho_bytes,
       max(evento_ts)                                        AS desde,
       unix_timestamp(current_timestamp()) - unix_timestamp(max(evento_ts)) AS segundos,
       nullif(regexp_extract(arquivo_ctl, '^[0-9]+_([^_]+)_.+_[0-9]{8}_[0-9]{14}[.][cC][tT][lL]$', 1), '') AS grupo
FROM ${catalogo}.${schema}.log_carga_evento
WHERE evento_ts >= current_timestamp() - INTERVAL 12 HOURS AND status NOT IN ('ALERTA_FORMATO', 'DESBLOQUEIO')
GROUP BY arquivo_ctl
HAVING max_by(status, evento_ts) IN ('ROTEADO', 'INICIADO', 'CARREGADO');

-- Lotes: status (EM_ANDAMENTO, CONCLUIDO, CONCLUIDO_COM_ERROS, INTERROMPIDO) e resumo.
-- Só existe 1 despachante por vez: um lote sem fim que NÃO é o mais recente está INTERROMPIDO (caiu ou foi
-- cancelado), mesmo antes de o lote seguinte registrar isso em log_lote.
CREATE OR REPLACE VIEW ${catalogo}.${schema}.v_lote AS
WITH l AS (
  SELECT lote_id,
         min(evento_ts) FILTER (WHERE status = 'INICIADO')     AS inicio,
         max(evento_ts) FILTER (WHERE status IN ('FINALIZADO', 'INTERROMPIDO')) AS fim,
         count_if(status = 'INTERROMPIDO') > 0                 AS interrompido,
         max(pendentes) FILTER (WHERE status = 'INICIADO')     AS pendentes_no_inicio
  FROM ${catalogo}.${schema}.log_lote GROUP BY lote_id
), e AS (
  SELECT lote_id,
         count(DISTINCT arquivo_ctl) FILTER (WHERE status = 'INICIADO')      AS arquivos_iniciados,
         count(DISTINCT arquivo_ctl) FILTER (WHERE status = 'CONCLUIDO')     AS arquivos_concluidos,
         count(DISTINCT arquivo_ctl) FILTER (WHERE status LIKE 'ERRO%')      AS arquivos_com_erro,
         count(DISTINCT arquivo_ctl) FILTER (WHERE status = 'CONCLUIDO' AND fila = 'JOB') AS concluidos_em_job,
         sum(qtd_inserida) FILTER (WHERE status = 'CONCLUIDO')               AS registros
  FROM ${catalogo}.${schema}.log_carga_evento WHERE lote_id IS NOT NULL GROUP BY lote_id
)
SELECT l.lote_id, l.inicio, l.fim,
       CASE WHEN l.interrompido THEN 'INTERROMPIDO'
            WHEN l.fim IS NULL AND l.inicio < max(l.inicio) OVER () THEN 'INTERROMPIDO'
            WHEN l.fim IS NULL AND l.inicio < current_timestamp() - INTERVAL 12 HOURS THEN 'INTERROMPIDO'
            WHEN l.fim IS NULL THEN 'EM_ANDAMENTO'
            WHEN coalesce(e.arquivos_com_erro, 0) > 0 THEN 'CONCLUIDO_COM_ERROS'
            ELSE 'CONCLUIDO' END                                                  AS status,
       round((unix_timestamp(coalesce(l.fim, current_timestamp())) - unix_timestamp(l.inicio)) / 60, 1) AS minutos,
       l.pendentes_no_inicio, coalesce(e.arquivos_iniciados, 0) AS arquivos_iniciados,
       coalesce(e.arquivos_concluidos, 0) AS arquivos_concluidos, coalesce(e.arquivos_com_erro, 0) AS arquivos_com_erro,
       coalesce(e.concluidos_em_job, 0) AS concluidos_em_job, coalesce(e.registros, 0) AS registros
FROM l LEFT JOIN e USING (lote_id);
