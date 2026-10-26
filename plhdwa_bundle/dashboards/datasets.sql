-- Datasets dos dashboards PLHDWA. Um SELECT por dataset, separados por ";" + linha em branco.
-- O gerar_dashboards.py lê este arquivo (o nome do dataset vem no comentário "-- ds:").
-- __PLH__. some na geração: o catálogo/schema de cada ambiente vem do bundle (dataset_catalog/dataset_schema).
-- Tabelas de sistema (system.*) ficam qualificadas.

-- ds: janela
SELECT (SELECT max(termino_previsto) FROM __PLH__.v_projecao_termino)                                   AS termino_previsto,
       (SELECT coalesce(sum(pendentes_qtd), 0) FROM __PLH__.v_projecao_termino)                         AS pendentes_qtd,
       (SELECT round(coalesce(sum(pendentes_bytes), 0) / 1073741824, 2) FROM __PLH__.v_projecao_termino) AS pendentes_gb,
       (SELECT count(*) FROM __PLH__.v_em_execucao)                                                      AS em_execucao,
       -- objetos diferentes em processamento agora (no máximo 1 arquivo por objeto, em qualquer modo)
       (SELECT count(DISTINCT cod_objeto) FROM __PLH__.v_em_execucao)                                    AS objetos_em_execucao,
       (SELECT count(*) FROM __PLH__.v_lote WHERE status = 'EM_ANDAMENTO')                               AS lotes_em_andamento,
       -- foto do último ciclo do despachante (conta também os jobs ainda na fila do Jobs); 0 se não há lote rodando
       (SELECT coalesce(max(em_andamento) FILTER (WHERE fila = 'JOB'), 0) FROM __PLH__.log_ciclo_despachante
         WHERE ciclo_ts = (SELECT max(ciclo_ts) FROM __PLH__.log_ciclo_despachante)
           AND EXISTS (SELECT 1 FROM __PLH__.v_lote WHERE status = 'EM_ANDAMENTO'))                       AS jobs_em_execucao,
       (SELECT coalesce(max(em_andamento) FILTER (WHERE fila = 'DESPACHANTE'), 0) FROM __PLH__.log_ciclo_despachante
         WHERE ciclo_ts = (SELECT max(ciclo_ts) FROM __PLH__.log_ciclo_despachante)
           AND EXISTS (SELECT 1 FROM __PLH__.v_lote WHERE status = 'EM_ANDAMENTO'))                       AS threads_despachante,
       (SELECT count(*) FROM __PLH__.v_arquivo_concluido WHERE concluido_em >= current_date())          AS concluidos_hoje,
       (SELECT round(coalesce(sum(tamanho_bytes), 0) / 1073741824, 2) FROM __PLH__.v_arquivo_concluido
         WHERE concluido_em >= current_date())                                                           AS gb_hoje,
       (SELECT coalesce(sum(qtd_inserida), 0) FROM __PLH__.v_arquivo_concluido WHERE concluido_em >= current_date()) AS registros_hoje,
       (SELECT count(*) FROM __PLH__.v_objeto_travado)                                                   AS objetos_travados;

-- ds: backlog_fila
SELECT fila AS modo, pendentes_qtd, round(pendentes_bytes / 1048576, 1) AS pendentes_mb, em_andamento, max_simult,
       termino_previsto
FROM __PLH__.v_projecao_termino;

-- ds: em_execucao
SELECT arquivo_ctl, grupo, cod_objeto, estado, modo, tipo_job, lote_id, job_run_id, round(tamanho_bytes / 1048576, 1) AS mb,
       desde, segundos
FROM __PLH__.v_em_execucao
ORDER BY desde;

-- ds: objetos_execucao
-- Um objeto por linha: o arquivo que está rodando agora e quantos arquivos do objeto o lote já concluiu.
WITH concluidos AS (
  SELECT lote_id, cod_objeto, count(DISTINCT arquivo_ctl) AS concluidos_no_lote
  FROM __PLH__.log_carga_evento
  WHERE status = 'CONCLUIDO' AND evento_ts >= current_timestamp() - INTERVAL 12 HOURS
  GROUP BY 1, 2
)
SELECT e.cod_objeto, e.grupo, e.tabela_destino, e.modo, e.tipo_job, e.estado, e.arquivo_ctl AS arquivo_atual,
       round(e.tamanho_bytes / 1048576, 1) AS mb, e.desde, e.segundos, e.lote_id,
       coalesce(c.concluidos_no_lote, 0) AS concluidos_no_lote
FROM __PLH__.v_em_execucao e
LEFT JOIN concluidos c ON c.lote_id = e.lote_id AND c.cod_objeto = e.cod_objeto
ORDER BY e.modo, e.desde;

-- ds: jobs_execucao
-- Um job independente por linha, da foto do último ciclo do despachante (a mesma que o cartão conta):
-- aparece também o job na fila do Jobs, arrancando ou entre um arquivo e outro do mesmo objeto.
WITH ultimo AS (
  SELECT max(ciclo_ts) AS ciclo_ts FROM __PLH__.log_ciclo_despachante
), j AS (
  SELECT a.* FROM __PLH__.log_job_ativo a JOIN ultimo u ON a.ciclo_ts = u.ciclo_ts
  WHERE EXISTS (SELECT 1 FROM __PLH__.v_lote WHERE status = 'EM_ANDAMENTO')
), feitos AS (
  -- arquivos que esta execução já concluiu (o job segue com o mesmo objeto até max_arquivos / max_minutos)
  -- e o último evento da execução (sem arquivo em aberto: trocando de arquivo ou encerrando, ex.: após um ERRO_*)
  SELECT job_run_id, count(DISTINCT arquivo_ctl) FILTER (WHERE status = 'CONCLUIDO') AS arquivos_concluidos,
         max(tabela_destino) AS tabela_destino,
         max_by(status, evento_ts) AS ultimo_status, max_by(arquivo_ctl, evento_ts) AS ultimo_arquivo
  FROM __PLH__.log_carga_evento
  WHERE job_run_id IN (SELECT job_run_id FROM j) AND evento_ts >= current_timestamp() - INTERVAL 12 HOURS
    AND status NOT LIKE 'ALERTA%' AND status <> 'DESBLOQUEIO'
  GROUP BY 1
)
SELECT j.job_run_id, j.cod_objeto, coalesce(e.tabela_destino, f.tabela_destino) AS tabela_destino, j.tipo_job,
       j.estado_run, j.inicio_run,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(j.inicio_run)) / 60, 1) AS minutos_rodando,
       CASE WHEN e.estado IS NOT NULL THEN e.estado
            WHEN f.ultimo_status = 'CONCLUIDO' THEN 'ENTRE_ARQUIVOS'
            WHEN f.ultimo_status IS NOT NULL THEN concat(f.ultimo_status, ' · encerrando')
            ELSE 'AGUARDANDO_INICIO' END                                  AS estado_arquivo,
       coalesce(e.arquivo_ctl, f.ultimo_arquivo)                        AS arquivo_atual,
       round(e.tamanho_bytes / 1048576, 1) AS mb, e.segundos AS segundos_no_arquivo,
       coalesce(f.arquivos_concluidos, 0) AS arquivos_concluidos, j.lote_id
FROM j
LEFT JOIN __PLH__.v_em_execucao e ON e.cod_objeto = j.cod_objeto
LEFT JOIN feitos f ON f.job_run_id = j.job_run_id
ORDER BY j.inicio_run NULLS LAST, j.cod_objeto;

-- ds: lotes
SELECT lote_id, status, inicio, fim, minutos, pendentes_no_inicio, arquivos_iniciados, arquivos_concluidos,
       arquivos_com_erro, concluidos_em_job, registros
FROM __PLH__.v_lote
WHERE inicio >= date_sub(current_date(), 7)
ORDER BY inicio DESC
LIMIT 50;

-- ds: controle
SELECT confirmado_em, cod_objeto, arquivo, status, modo_execucao, lote_id, qtd_registros, dt_inicio, dt_fim,
       left(mensagem, 200) AS mensagem
FROM __PLH__.db2_controle_sim
WHERE confirmado_em >= date_sub(current_date(), 1)
ORDER BY confirmado_em DESC
LIMIT 1000;

-- ds: ciclos
SELECT ciclo_ts, fila AS modo, pendentes_qtd, round(pendentes_bytes / 1048576, 1) AS pendentes_mb, em_andamento, max_simult
FROM __PLH__.log_ciclo_despachante
WHERE ciclo_ts >= date_sub(current_date(), 1);

-- ds: travados
SELECT cod_objeto, grupo, erro, arquivo_ctl, left(mensagem, 400) AS mensagem, desde
FROM __PLH__.v_objeto_travado;

-- ds: eventos
SELECT evento_ts, cod_objeto, nullif(regexp_extract(arquivo_ctl, '^[0-9]+_([^_]+)_.+_[0-9]{8}_[0-9]{14}[.][cC][tT][lL]$', 1), '') AS grupo, arquivo_ctl, status, fila,
       tipo_job, tabela_destino, left(mensagem, 300) AS mensagem
FROM __PLH__.log_carga_evento
WHERE evento_ts >= date_sub(current_date(), 1)
ORDER BY evento_ts DESC
LIMIT 2000;

-- ds: desempenho
SELECT arquivo_ctl, grupo, cod_objeto, fila AS modo, coalesce(tipo_job, 'PERFORMANCE_OPTIMIZED') AS tipo_job, lote_id,
       round(tamanho_bytes / 1048576, 1)                                         AS tamanho_mb,
       unix_timestamp(concluido_em) - unix_timestamp(roteado_em)                 AS duracao_s,
       unix_timestamp(fim_carga) - unix_timestamp(inicio_carga)                  AS carga_s,
       round(tamanho_bytes / 1073741824 /
             greatest(unix_timestamp(concluido_em) - unix_timestamp(roteado_em), 1) * 60, 3) AS gb_por_min,
       concluido_em, qtd_inserida
FROM __PLH__.v_arquivo_concluido
WHERE concluido_em >= date_sub(current_date(), 30);

-- ds: diario
SELECT date(concluido_em) AS dia, count(*) AS arquivos, round(sum(tamanho_bytes) / 1073741824, 3) AS gb,
       max(concluido_em) AS ultimo_concluido, sum(qtd_inserida) AS linhas
FROM __PLH__.v_arquivo_concluido
WHERE concluido_em >= date_sub(current_date(), 30)
GROUP BY 1;

-- ds: vazao
-- Vazão: 1 linha por arquivo concluído (30 dias). espera = roteado -> iniciado (fila do Jobs / vaga); carga = leitura +
-- gravação na tabela ·
-- execução = job_run_id (o despachante ou o job independente).
SELECT concluido_em, fila AS modo, coalesce(tipo_job, 'PERFORMANCE_OPTIMIZED') AS tipo_job, grupo, cod_objeto, lote_id,
       job_run_id, tamanho_bytes / 1073741824                                     AS gb,
       qtd_inserida                                                               AS registros,
       unix_timestamp(iniciado_em) - unix_timestamp(roteado_em)                   AS espera_s,
       unix_timestamp(fim_carga) - unix_timestamp(inicio_carga)                   AS carga_s,
       unix_timestamp(concluido_em) - unix_timestamp(roteado_em)                  AS duracao_s,
       tamanho_bytes / 1073741824 /
             greatest(unix_timestamp(concluido_em) - unix_timestamp(roteado_em), 1) * 60 AS gb_por_min
FROM __PLH__.v_arquivo_concluido
WHERE concluido_em >= date_sub(current_date(), 30);

-- ds: vazao_hora
SELECT date_trunc('HOUR', concluido_em) AS hora, fila AS modo, grupo,
       count(*)                                                                   AS arquivos,
       round(sum(tamanho_bytes) / 1073741824, 3)                                  AS gb,
       sum(qtd_inserida)                                                          AS registros,
       count(DISTINCT job_run_id)                                                 AS execucoes,
       count(DISTINCT cod_objeto)                                                 AS objetos,
       round(sum(tamanho_bytes) / 1073741824 /
             greatest(unix_timestamp(max(concluido_em)) - unix_timestamp(min(roteado_em)), 1) * 60, 3) AS gb_por_min,
       round(avg(unix_timestamp(iniciado_em) - unix_timestamp(roteado_em)), 1)    AS espera_media_s,
       round(avg(unix_timestamp(fim_carga) - unix_timestamp(inicio_carga)), 1)    AS carga_media_s
FROM __PLH__.v_arquivo_concluido
WHERE concluido_em >= date_sub(current_date(), 30)
GROUP BY ALL;

-- ds: erros_dia
SELECT date(evento_ts) AS dia, status, count(*) AS qtd
FROM __PLH__.log_carga_evento
WHERE (status LIKE 'ERRO%' OR status LIKE 'ALERTA%') AND status <> 'ALERTA_DB2_COMANDO'   -- comando executado: não é alerta
  AND evento_ts >= date_sub(current_date(), 30)
GROUP BY 1, 2;

-- ds: filas
SELECT modo, max_simult, job_id_perf, job_id_std, alterado_por, alterado_em
FROM __PLH__.ctl_execucao
ORDER BY modo;

-- ds: custo
WITH jobs AS (
  SELECT job_id, max_by(name, change_time) AS job
  FROM system.lakeflow.jobs
  WHERE tags['OWNER'] = '__OWNER__'
  GROUP BY job_id
), uso AS (
  SELECT u.usage_date, u.usage_metadata.job_id AS job_id, u.usage_metadata.job_run_id AS job_run_id, u.sku_name,
         coalesce(u.product_features.performance_target, 'n/d') AS tipo_job,
         u.usage_quantity AS dbus,
         u.usage_quantity * p.pricing.default AS custo_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_date >= date_sub(current_date(), 30)
    AND (u.custom_tags['OWNER'] = '__OWNER__' OR u.usage_metadata.job_id IN (SELECT job_id FROM jobs))
)
SELECT uso.usage_date, coalesce(j.job, concat('job ', uso.job_id), 'outros (tag OWNER)') AS job, uso.job_run_id,
       uso.sku_name, uso.tipo_job, uso.dbus, round(uso.custo_usd, 4) AS custo_usd
FROM uso LEFT JOIN jobs j ON uso.job_id = j.job_id;

-- ds: custo_fila
WITH arquivo_run AS (
  SELECT CAST(job_run_id AS STRING) AS job_run_id, max(fila) AS fila
  FROM __PLH__.log_carga_evento WHERE status = 'CARREGADO' GROUP BY 1
), uso AS (
  SELECT u.usage_date, u.usage_metadata.job_run_id AS job_run_id,
         u.usage_quantity * p.pricing.default AS custo_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_date >= date_sub(current_date(), 30)
    AND u.usage_metadata.job_run_id IN (SELECT job_run_id FROM arquivo_run)
)
SELECT uso.usage_date, coalesce(a.fila, 'despacho / outros') AS modo, round(sum(uso.custo_usd), 4) AS custo_usd
FROM uso LEFT JOIN arquivo_run a USING (job_run_id)
GROUP BY 1, 2;

-- ds: custo_tb
WITH custo AS (
  SELECT u.usage_date AS dia, sum(u.usage_quantity * p.pricing.default) AS custo_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_date >= date_sub(current_date(), 30)
    AND (u.custom_tags['OWNER'] = '__OWNER__' OR u.usage_metadata.job_id IN
         (SELECT job_id FROM system.lakeflow.jobs WHERE tags['OWNER'] = '__OWNER__'))
  GROUP BY 1
), volume AS (
  SELECT date(concluido_em) AS dia, sum(tamanho_bytes) / 1099511627776 AS tb
  FROM __PLH__.v_arquivo_concluido GROUP BY 1
)
SELECT c.dia, round(c.custo_usd, 2) AS custo_usd, round(v.tb, 6) AS tb_carregado,
       round(c.custo_usd / nullif(v.tb, 0), 2) AS custo_por_tb_usd
FROM custo c LEFT JOIN volume v USING (dia);

-- ds: custo_objeto
WITH run_obj AS (
  SELECT CAST(job_run_id AS STRING) AS job_run_id, cod_objeto, fila, sum(tamanho_bytes) AS bytes
  FROM __PLH__.log_carga_evento WHERE status = 'CARREGADO' GROUP BY 1, 2, 3
), peso AS (
  SELECT *, bytes / sum(bytes) OVER (PARTITION BY job_run_id) AS fracao FROM run_obj
), uso AS (
  SELECT u.usage_metadata.job_run_id AS job_run_id, sum(u.usage_quantity * p.pricing.default) AS custo_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_date >= date_sub(current_date(), 30)
    AND u.usage_metadata.job_run_id IN (SELECT job_run_id FROM run_obj)
  GROUP BY 1
)
SELECT p.cod_objeto, max(p.fila) AS modo, round(sum(u.custo_usd * p.fracao), 4) AS custo_usd,
       round(sum(p.bytes) / 1073741824, 3) AS gb
FROM peso p JOIN uso u USING (job_run_id)
GROUP BY 1
ORDER BY custo_usd DESC
LIMIT 20
