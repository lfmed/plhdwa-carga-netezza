-- =====================================================================================
-- PLHDWA · tabela da controle no Db2 (para o DBA do cliente criar, uma vez)
--
-- O motor grava 1 linha por arquivo, no fim: CONCLUIDO (com a quantidade de registros) | ERRO (com a mensagem).
-- Com a variável db2_registrar_inicio = true, a linha nasce antes da carga como EM_PROCESSAMENTO e é atualizada.
-- Chave: objeto + data de movimento + data de geração (é o que identifica um arquivo do Netezza).
--
-- {tabela} = variável db2_tabela do bundle (schema.tabela). O job "PLHDWA · testar controle Db2" pode criar
-- esta tabela (parâmetro criar_tabela = true) ou o DBA roda este arquivo trocando {tabela} à mão.
-- Se a controle do cliente já existe com outro layout, NÃO crie esta tabela: escolha/ajuste o comando
-- (variável db2_comando; ex.: 02_db2_controle_merge_tabela_controle.sql), mantendo os 14 parâmetros "?".
-- O usuário do motor (secret scope, variável db2_secret_scope) precisa de SELECT, INSERT e UPDATE nela:
--   GRANT SELECT, INSERT, UPDATE ON <schema.tabela> TO USER <usuario_do_motor>
-- =====================================================================================
CREATE TABLE {tabela} (
  COD_OBJETO      INTEGER        NOT NULL,      -- código do objeto (1º campo do nome do arquivo)
  NOME_TABELA     VARCHAR(128)   NOT NULL,      -- nome da tabela como vem no arquivo
  DATA_MOVIMENTO  INTEGER        NOT NULL,      -- yyyyMMdd, como no CTL
  DATA_GERACAO    TIMESTAMP      NOT NULL,      -- data de geração do nome do arquivo
  INSTITUICOES    VARCHAR(4000),                -- "0" = todas, ou lista separada por vírgula
  STATUS          VARCHAR(20)    NOT NULL,      -- EM_PROCESSAMENTO | ERRO | CONCLUIDO
  QTD_REGISTROS   BIGINT,                       -- registros carregados (no CONCLUIDO)
  ARQUIVO         VARCHAR(256),                 -- nome do CTL
  JOB_RUN_ID      BIGINT,                       -- execução do Databricks que gravou
  LOTE_ID         BIGINT,                       -- lote (execução do despachante)
  MODO_EXECUCAO   VARCHAR(20),                  -- DESPACHANTE | JOB
  DT_INICIO       TIMESTAMP,                    -- início do processamento do arquivo
  DT_FIM          TIMESTAMP,                    -- fim do processamento do arquivo
  MENSAGEM        VARCHAR(2000),                -- motivo do erro
  CONFIRMADO_EM   TIMESTAMP      NOT NULL DEFAULT CURRENT TIMESTAMP,
  PRIMARY KEY (COD_OBJETO, DATA_MOVIMENTO, DATA_GERACAO)
)
