-- =====================================================================================
-- PLHDWA · variante do comando da controle para uma tabela no layout "CONTROLE" genérico:
--   ID_CONTROLE (identity) · NOME_JOB VARCHAR(100) · STATUS CHAR(10) IN ('INICIADO','EXECUTANDO','CONCLUIDO','ERRO')
--   DT_INICIO TIMESTAMP NOT NULL · DT_FIM TIMESTAMP · MENSAGEM_ERRO VARCHAR(500)
-- Uso: variável db2_comando = 02_db2_controle_merge_tabela_controle.sql e db2_tabela = <schema>.CONTROLE.
--
-- Cada arquivo vira 1 linha, identificada pelo nome do CTL em NOME_JOB (o nome já traz objeto, data de
-- movimento e data de geração). Status do motor -> status da tabela:
--   EM_PROCESSAMENTO -> EXECUTANDO · CONCLUIDO -> CONCLUIDO · ERRO -> ERRO
-- Esta tabela não tem colunas para quantidade de registros, lote e modo: esses dados continuam no
-- histórico Delta (db2_controle_sim) e no log (log_carga_evento).
-- Os 14 parâmetros "?" chegam na mesma ordem descrita em 02_db2_controle_merge.sql.
-- =====================================================================================
MERGE INTO {tabela} AS d
USING (VALUES (CAST(? AS INTEGER), CAST(? AS VARCHAR(128)), CAST(? AS INTEGER), CAST(? AS TIMESTAMP),
               CAST(? AS VARCHAR(4000)), CAST(? AS VARCHAR(20)), CAST(? AS BIGINT), CAST(? AS VARCHAR(256)),
               CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR(20)), CAST(? AS TIMESTAMP),
               CAST(? AS TIMESTAMP), CAST(? AS VARCHAR(2000))))
      AS s (COD_OBJETO, NOME_TABELA, DATA_MOVIMENTO, DATA_GERACAO, INSTITUICOES, STATUS, QTD_REGISTROS, ARQUIVO,
            JOB_RUN_ID, LOTE_ID, MODO_EXECUCAO, DT_INICIO, DT_FIM, MENSAGEM)
ON d.NOME_JOB = CAST(s.ARQUIVO AS VARCHAR(100))
WHEN MATCHED THEN UPDATE SET
     STATUS = CASE s.STATUS WHEN 'EM_PROCESSAMENTO' THEN 'EXECUTANDO' ELSE s.STATUS END,
     DT_INICIO = COALESCE(s.DT_INICIO, d.DT_INICIO),
     DT_FIM = s.DT_FIM,
     MENSAGEM_ERRO = CAST(s.MENSAGEM AS VARCHAR(500))
WHEN NOT MATCHED THEN INSERT (NOME_JOB, STATUS, DT_INICIO, DT_FIM, MENSAGEM_ERRO)
     VALUES (CAST(s.ARQUIVO AS VARCHAR(100)), CASE s.STATUS WHEN 'EM_PROCESSAMENTO' THEN 'EXECUTANDO' ELSE s.STATUS END,
             COALESCE(s.DT_INICIO, CURRENT TIMESTAMP), s.DT_FIM, CAST(s.MENSAGEM AS VARCHAR(500)))
