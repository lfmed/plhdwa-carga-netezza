-- =====================================================================================
-- PLHDWA · variante dos comandos da controle para uma tabela no layout "CONTROLE" genérico (usada nos testes):
--   ID_CONTROLE · NOME_JOB VARCHAR(100) · STATUS CHAR(10) · DT_INICIO · DT_FIM · MENSAGEM_ERRO VARCHAR(500)
-- Mesmo comportamento dos comandos oficiais (02_db2_controle_update.sql): só UPDATE, a linha do objeto precisa já
-- existir. Uma linha por objeto e instituição: NOME_JOB = 'OBJ_<cod_objeto>_<cd_inst>.CTL' (cd_inst 0 = todas), para
-- simular o filtro CD_INS IN (:lista_cd_inst) dos comandos oficiais (a comparação é em texto, sem CAST, para não
-- falhar nas outras linhas da tabela).
-- =====================================================================================

-- [CONCLUIDO]
UPDATE {tabela}
   SET STATUS = 'CONCLUIDO', DT_FIM = CURRENT TIMESTAMP, MENSAGEM_ERRO = NULL
 WHERE UPPER(TRIM(NOME_JOB)) LIKE 'OBJ\_%.CTL' ESCAPE '\'
   AND REPLACE(REPLACE(UPPER(TRIM(NOME_JOB)), '.CTL', ''), 'OBJ_' || VARCHAR(CAST(:cod_objeto AS INTEGER)) || '_', '')
       IN (:lista_cd_inst)

-- [ERRO]
UPDATE {tabela}
   SET STATUS = 'ERRO', DT_FIM = CURRENT TIMESTAMP, MENSAGEM_ERRO = CAST(:mensagem AS VARCHAR(500))
 WHERE UPPER(TRIM(NOME_JOB)) LIKE 'OBJ\_%.CTL' ESCAPE '\'
   AND REPLACE(REPLACE(UPPER(TRIM(NOME_JOB)), '.CTL', ''), 'OBJ_' || VARCHAR(CAST(:cod_objeto AS INTEGER)) || '_', '')
       IN (:lista_cd_inst)
