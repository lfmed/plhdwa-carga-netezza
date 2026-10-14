-- =====================================================================================
-- PLHDWA · variante dos comandos da controle para uma tabela no layout "CONTROLE" genérico (usada nos testes):
--   ID_CONTROLE · NOME_JOB VARCHAR(100) · STATUS CHAR(10) · DT_INICIO · DT_FIM · MENSAGEM_ERRO VARCHAR(500)
-- Mesmo comportamento dos comandos oficiais (02_db2_controle_update.sql): só UPDATE, a linha do objeto precisa já
-- existir. A linha do objeto é a que tem NOME_JOB = 'OBJ_<cod_objeto>.CTL'.
-- =====================================================================================

-- [CONCLUIDO]
UPDATE {tabela}
   SET STATUS = 'CONCLUIDO', DT_FIM = CURRENT TIMESTAMP, MENSAGEM_ERRO = NULL
 WHERE NOME_JOB = 'OBJ_' || VARCHAR(CAST(:cod_objeto AS INTEGER)) || '.CTL'

-- [ERRO]
UPDATE {tabela}
   SET STATUS = 'ERRO', DT_FIM = CURRENT TIMESTAMP, MENSAGEM_ERRO = CAST(:mensagem AS VARCHAR(500))
 WHERE NOME_JOB = 'OBJ_' || VARCHAR(CAST(:cod_objeto AS INTEGER)) || '.CTL'
