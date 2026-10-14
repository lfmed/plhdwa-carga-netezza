-- =====================================================================================
-- PLHDWA · comandos da controle no Db2 (comandos oficiais do cliente, 14/10/2026)
-- Lido por src/plhdwa_motor.py (variável db2_comando) e executado com o driver Python oficial da IBM (ibm_db).
--
-- Regras:
--   * só UPDATE: o motor recusa qualquer outro comando (nunca insere nem apaga linha na controle);
--   * uma seção por status, com UM comando: -- [CONCLUIDO] (carga OK) e -- [ERRO] (obrigatórias);
--     -- [EM_PROCESSAMENTO] só é usada com db2_registrar_inicio = true;
--   * {tabela} = variável db2_tabela (ex.: ctl.OBJETO_DEPENDENCIA_EXECUCAO);
--   * dados do arquivo como marcadores :nome  ->  :cod_objeto :nome_tabela :data_movimento :data_geracao
--     :instituicoes :status :qtd_registros :arquivo :job_run_id :lote_id :modo_execucao :dt_inicio :dt_fim :mensagem
--   * UPDATE que não encontra linha (0 linhas) = ERRO_DB2 no arquivo, com a mensagem; mais de 1 linha é aceito.
-- :mensagem chega com até db2_mensagem_max caracteres (padrão 500): ajuste ao tamanho de DS_MNG_ERR_EXC.
-- =====================================================================================

-- [CONCLUIDO]
/* Sinaliza CTL DB2 em caso de carga OK */
UPDATE {tabela}
   SET IC_PRC_EXC = 0,              /* 0 indica que o processo não está mais em execução */
       IC_PRC_FNZ = 1,              /* 1 indica que o processo finalizou com sucesso */
       DH_FIM_PRC = CURRENT TIMESTAMP
 WHERE CD_OBJ IN (CAST(:cod_objeto AS INTEGER))

-- [ERRO]
/* Sinaliza CTL DB2 em caso de erro */
UPDATE {tabela}
   SET IC_PRC_EXC = 0,              /* 0 indica que o processo não está mais em execução */
       IC_PRC_FNZ = 0,              /* 0 indica que o processo finalizou com erro */
       DH_FIM_PRC = CURRENT TIMESTAMP,
       DS_MNG_ERR_EXC = CAST(:mensagem AS VARCHAR(2000))   /* mensagem de erro do motor (tipo: motivo) */
 WHERE CD_OBJ IN (CAST(:cod_objeto AS INTEGER))
