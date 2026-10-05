-- =====================================================================================
-- PLHDWA · comando que o motor executa no Db2 a cada mudança de status de um arquivo
-- (lido por src/plhdwa_motor.py quando db2_modo = DB2; executado pelo driver Python oficial da IBM, ibm_db).
--
-- {tabela} = variável db2_tabela do bundle (schema.tabela).
-- Os 14 parâmetros "?" chegam SEMPRE nesta ordem (não mude a ordem; mude só os nomes de coluna à vontade):
--    1 cod_objeto       INTEGER        8 arquivo (nome do CTL)  VARCHAR
--    2 nome_tabela      VARCHAR        9 job_run_id             BIGINT
--    3 data_movimento   INTEGER       10 lote_id                BIGINT
--    4 data_geracao     TIMESTAMP (texto aaaa-mm-dd hh:mm:ss, convertido pelo CAST)     11 modo_execucao          VARCHAR  (DESPACHANTE | JOB)
--    5 instituicoes     VARCHAR       12 dt_inicio              TIMESTAMP
--    6 status           VARCHAR       13 dt_fim                 TIMESTAMP (vazio no EM_PROCESSAMENTO)
--      (CONCLUIDO | ERRO; EM_PROCESSAMENTO só com               14 mensagem   VARCHAR (só no ERRO)
--       db2_registrar_inicio = true)
--    7 qtd_registros    BIGINT (só no CONCLUIDO)
-- Um comando só (sem ";" no fim). Se a controle do cliente tiver outro layout, ajuste aqui.
-- =====================================================================================
MERGE INTO {tabela} AS d
USING (VALUES (CAST(? AS INTEGER), CAST(? AS VARCHAR(128)), CAST(? AS INTEGER), CAST(? AS TIMESTAMP),
               CAST(? AS VARCHAR(4000)), CAST(? AS VARCHAR(20)), CAST(? AS BIGINT), CAST(? AS VARCHAR(256)),
               CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR(20)), CAST(? AS TIMESTAMP),
               CAST(? AS TIMESTAMP), CAST(? AS VARCHAR(2000))))
      AS s (COD_OBJETO, NOME_TABELA, DATA_MOVIMENTO, DATA_GERACAO, INSTITUICOES, STATUS, QTD_REGISTROS, ARQUIVO,
            JOB_RUN_ID, LOTE_ID, MODO_EXECUCAO, DT_INICIO, DT_FIM, MENSAGEM)
ON d.COD_OBJETO = s.COD_OBJETO AND d.DATA_MOVIMENTO = s.DATA_MOVIMENTO AND d.DATA_GERACAO = s.DATA_GERACAO
WHEN MATCHED THEN UPDATE SET
     STATUS = s.STATUS, QTD_REGISTROS = s.QTD_REGISTROS, ARQUIVO = s.ARQUIVO, JOB_RUN_ID = s.JOB_RUN_ID,
     LOTE_ID = s.LOTE_ID, MODO_EXECUCAO = s.MODO_EXECUCAO, DT_INICIO = COALESCE(s.DT_INICIO, d.DT_INICIO),
     DT_FIM = s.DT_FIM, MENSAGEM = s.MENSAGEM, CONFIRMADO_EM = CURRENT TIMESTAMP
WHEN NOT MATCHED THEN INSERT
     (COD_OBJETO, NOME_TABELA, DATA_MOVIMENTO, DATA_GERACAO, INSTITUICOES, STATUS, QTD_REGISTROS, ARQUIVO,
      JOB_RUN_ID, LOTE_ID, MODO_EXECUCAO, DT_INICIO, DT_FIM, MENSAGEM, CONFIRMADO_EM)
     VALUES (s.COD_OBJETO, s.NOME_TABELA, s.DATA_MOVIMENTO, s.DATA_GERACAO, s.INSTITUICOES, s.STATUS,
             s.QTD_REGISTROS, s.ARQUIVO, s.JOB_RUN_ID, s.LOTE_ID, s.MODO_EXECUCAO, s.DT_INICIO, s.DT_FIM,
             s.MENSAGEM, CURRENT TIMESTAMP)
