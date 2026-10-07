"""
Gera os dois dashboards AI/BI do PLHDWA a partir de datasets.sql (consultas já testadas no warehouse).
  python3 gerar_dashboards.py
Saída: plhdwa_execucao.lvdash.json e plhdwa_custo.lvdash.json (referenciados em resources/dashboards.yml).

As consultas ficam SEM catálogo/schema (ex.: FROM v_lote): o bundle define o catálogo e o schema padrão de
cada dashboard por ambiente (dataset_catalog / dataset_schema em resources/dashboards.yml). Assim o mesmo
JSON serve para dev, hml e prd. Só é preciso rodar este gerador quando mudar datasets.sql ou o layout.
(--plh catalogo.schema grava os nomes completos, para testar uma consulta fora do bundle.)
O valor da tag OWNER usado no painel de custo vem de --owner-tag (padrão PLHDWA): se mudar a variável
owner_tag do bundle, gere de novo com o mesmo valor.
"""
import argparse
import json
import os
import re

AQUI = os.path.dirname(os.path.abspath(__file__))


def datasets(plh, owner_tag):
    texto = open(os.path.join(AQUI, "datasets.sql"), encoding="utf-8").read()
    texto = texto.replace("__PLH__.", f"{plh}." if plh else "").replace("__OWNER__", owner_tag)
    saida = {}
    for bloco in re.split(r"\n(?=-- ds: )", texto):
        m = re.match(r"-- ds: (\w+)\n(.*)", bloco, re.S)
        if m:
            corpo = m.group(2).strip().rstrip(";")
            saida[m.group(1)] = {"name": m.group(1), "displayName": m.group(1),
                                 "queryLines": [l + "\n" for l in corpo.splitlines()]}
    return saida


# ---------- widgets (formato AI/BI) ----------
def texto(nome, linha, x, y, w=6, h=1):
    return {"widget": {"name": nome, "multilineTextboxSpec": {"lines": [linha]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def _campos(lista):
    return [{"name": n, "expression": e} for n, e in lista]


def contador(nome, ds, campo, titulo, x, y, agregado=None):
    nome_campo = agregado[0] if agregado else campo
    expr = agregado[1] if agregado else f"`{campo}`"
    return {"widget": {"name": nome, "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": _campos([(nome_campo, expr)]), "disaggregated": agregado is None}}],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": nome_campo, "displayName": titulo}},
                     "frame": {"showTitle": True, "title": titulo}}},
            "position": {"x": x, "y": y, "width": 2, "height": 3}}


def grafico(nome, tipo, ds, x_enc, y_enc, titulo, pos, cor=None, agrupado=False, agregado=True):
    """x_enc/y_enc/cor = (nome_campo, expressão, tipo_escala, rótulo). y_enc pode ser uma lista (vários y)."""
    ys = y_enc if isinstance(y_enc, list) else [y_enc]
    encs = [x_enc] + ys + ([cor] if cor else [])
    enc = {"x": {"fieldName": x_enc[0], "scale": {"type": x_enc[2]}, "displayName": x_enc[3]}}
    if len(ys) == 1:
        enc["y"] = {"fieldName": ys[0][0], "scale": {"type": "quantitative"}, "displayName": ys[0][3]}
    else:
        enc["y"] = {"scale": {"type": "quantitative"}, "fields": [{"fieldName": e[0], "displayName": e[3]} for e in ys]}
    if cor:
        enc["color"] = {"fieldName": cor[0], "scale": {"type": "categorical"}, "displayName": cor[3]}
    spec = {"version": 3, "widgetType": tipo, "encodings": enc, "frame": {"showTitle": True, "title": titulo}}
    if agrupado:
        spec["mark"] = {"layout": "group"}
    return {"widget": {"name": nome, "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": _campos([(e[0], e[1]) for e in encs]), "disaggregated": not agregado}}],
            "spec": spec},
            "position": dict(zip(("x", "y", "width", "height"), pos))}


def tabela(nome, ds, colunas, titulo, y, h=6):
    return {"widget": {"name": nome, "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": _campos([(c, f"`{c}`") for c, _ in colunas]), "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": c, "displayName": r} for c, r in colunas]},
                     "frame": {"showTitle": True, "title": titulo}}},
            "position": {"x": 0, "y": y, "width": 6, "height": h}}


def filtro(nome, ds, campo, titulo, x, y, w=3):
    q = f"{ds}_{campo}"
    return {"widget": {"name": nome, "queries": [{"name": q, "query": {
                "datasetName": ds, "fields": _campos([(campo, f"`{campo}`")]), "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "filter-multi-select",
                     "encodings": {"fields": [{"fieldName": campo, "displayName": titulo, "queryName": q}]},
                     "frame": {"showTitle": True, "title": titulo}}},
            "position": {"x": x, "y": y, "width": w, "height": 2}}


def pagina(nome, titulo, layout):
    return {"name": nome, "displayName": titulo, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}


C, Q, T = "categorical", "quantitative", "temporal"


def dash_execucao(ds):
    agora = pagina("agora", "Em execução", [
        texto("t_agora", "## PLHDWA · Em execução agora", 0, 0),
        texto("s_agora", "Lotes (execuções do despachante) e arquivos em processamento · modo DESPACHANTE (threads) ou JOB (job independente)", 0, 1),
        # 3 linhas de 3 cartões: o que está rodando · fila · resultado do dia
        contador("k_objetos", "janela", "objetos_em_execucao", "Objetos em execução", 0, 2),
        contador("k_threads", "janela", "threads_despachante", "Arquivos rodando no despachante", 2, 2),
        contador("k_jobs", "janela", "jobs_em_execucao", "Jobs independentes em execução", 4, 2),
        contador("k_lotes", "janela", "lotes_em_andamento", "Despachante: lotes em andamento", 0, 5),
        contador("k_pend", "janela", "pendentes_qtd", "Arquivos aguardando", 2, 5),
        contador("k_trav", "janela", "objetos_travados", "Objetos travados", 4, 5),
        contador("k_concl", "janela", "concluidos_hoje", "Arquivos concluídos hoje", 0, 8),
        contador("k_reg", "janela", "registros_hoje", "Registros carregados hoje", 2, 8),
        contador("k_gb", "janela", "gb_hoje", "GB carregados hoje", 4, 8),
        tabela("t_objexec", "objetos_execucao", [("cod_objeto", "Objeto"), ("grupo", "Grupo"), ("tabela_destino", "Tabela destino"),
                                                 ("modo", "Modo"), ("tipo_job", "Tipo do job"), ("estado", "Estado"),
                                                 ("arquivo_atual", "Arquivo atual"), ("mb", "MB"),
                                                 ("segundos", "Segundos no arquivo"),
                                                 ("concluidos_no_lote", "Concluídos no lote"), ("lote_id", "Lote")],
               "Objetos em execução (1 linha por objeto)", 11, 7),
        tabela("t_jobs", "jobs_execucao", [("job_run_id", "Execução do job"), ("cod_objeto", "Objeto"),
                                           ("tabela_destino", "Tabela destino"), ("tipo_job", "Tipo do job"),
                                           ("estado_run", "Estado no Jobs"), ("minutos_rodando", "Min rodando"),
                                           ("estado_arquivo", "Estado do arquivo"), ("arquivo_atual", "Arquivo atual"),
                                           ("mb", "MB"), ("segundos_no_arquivo", "Segundos no arquivo"),
                                           ("arquivos_concluidos", "Arquivos concluídos no job"), ("lote_id", "Lote")],
               "Jobs independentes em execução (1 linha por job · inclui fila do Jobs e troca de arquivo)", 18, 6),
        tabela("t_exec", "em_execucao", [("desde", "Desde"), ("segundos", "Segundos"), ("cod_objeto", "Objeto"), ("grupo", "Grupo"),
                                         ("estado", "Estado"), ("modo", "Modo"), ("tipo_job", "Tipo do job"),
                                         ("mb", "MB"), ("lote_id", "Lote"), ("job_run_id", "Execução"),
                                         ("arquivo_ctl", "Arquivo")],
               "Arquivos em execução (ROTEADO = job independente arrancando)", 24, 7),
        tabela("t_lotes", "lotes", [("lote_id", "Lote"), ("status", "Status"), ("inicio", "Início"), ("fim", "Fim"),
                                    ("minutos", "Min"), ("pendentes_no_inicio", "Pendentes no início"),
                                    ("arquivos_concluidos", "Concluídos"), ("concluidos_em_job", "Em job"),
                                    ("arquivos_com_erro", "Com erro"), ("registros", "Registros")],
               "Lotes (últimos 7 dias)", 31, 6),
        grafico("g_ocupacao", "bar", "backlog_fila", ("modo", "`modo`", C, "Modo"),
                [("em_andamento", "`em_andamento`", Q, "Em andamento"), ("max_simult", "`max_simult`", Q, "Limite (max_simult)")],
                "Em andamento × limite por modo", (0, 37, 3, 5), agrupado=True, agregado=False),
        grafico("g_backlog", "bar", "backlog_fila", ("modo", "`modo`", C, "Modo"),
                ("pendentes_qtd", "`pendentes_qtd`", Q, "Aguardando"), "Arquivos aguardando por modo",
                (3, 37, 3, 5), agregado=False),
        grafico("g_ciclos", "line", "ciclos", ("ciclo_ts", "`ciclo_ts`", T, "Ciclo"),
                ("sum(em_andamento)", "SUM(`em_andamento`)", Q, "Em andamento"), "Em andamento ao longo do tempo",
                (0, 42, 6, 6), cor=("modo", "`modo`", C, "Modo")),
        tabela("t_filas", "filas", [("modo", "Modo"), ("max_simult", "Simultâneos"), ("job_id_perf", "Job Performance"),
                                    ("job_id_std", "Job Standard"), ("alterado_por", "Alterado por"),
                                    ("alterado_em", "Alterado em")], "Limites por modo (ctl_execucao)", 48, 4),
    ])
    objetos = pagina("objetos", "Objetos e controle", [
        texto("t_obj", "## Objetos travados, eventos e controle", 0, 0),
        filtro("f_status", "eventos", "status", "Status", 0, 1, w=2),
        filtro("f_cod", "eventos", "cod_objeto", "Objeto", 2, 1, w=2),
        filtro("f_grupo", "eventos", "grupo", "Grupo", 4, 1, w=2),
        tabela("t_trav", "travados", [("cod_objeto", "Objeto"), ("grupo", "Grupo"), ("erro", "Erro"), ("desde", "Desde"),
                                       ("arquivo_ctl", "Arquivo"), ("mensagem", "Mensagem")],
               "Objetos travados (destravar com evento DESBLOQUEIO)", 3, 6),
        tabela("t_ev", "eventos", [("evento_ts", "Quando"), ("cod_objeto", "Objeto"), ("grupo", "Grupo"), ("status", "Status"),
                                   ("fila", "Modo"), ("tipo_job", "Tipo"), ("arquivo_ctl", "Arquivo"),
                                   ("mensagem", "Mensagem")], "Eventos (últimas 24 h)", 9, 9),
        tabela("t_ctl", "controle", [("confirmado_em", "Quando"), ("cod_objeto", "Objeto"), ("status", "Status na controle"),
                                     ("modo_execucao", "Modo"), ("qtd_registros", "Registros"), ("dt_inicio", "Início"),
                                     ("dt_fim", "Fim"), ("lote_id", "Lote"), ("arquivo", "Arquivo"),
                                     ("mensagem", "Mensagem")],
               "Controle (histórico): CONCLUIDO · ERRO por arquivo (EM_PROCESSAMENTO se db2_registrar_inicio)", 18, 8),
    ])
    desemp = pagina("desempenho", "Desempenho", [
        texto("h_des", "## Desempenho por modo e tipo de job", 0, 0),
        grafico("g_dur", "bar", "desempenho", ("modo", "`modo`", C, "Modo"),
                ("avg(duracao_s)", "AVG(`duracao_s`)", Q, "Segundos"), "Duração média por arquivo (s)", (0, 1, 3, 5),
                cor=("tipo_job", "`tipo_job`", C, "Tipo"), agrupado=True),
        grafico("g_gbmin", "bar", "desempenho", ("modo", "`modo`", C, "Modo"),
                ("avg(gb_por_min)", "AVG(`gb_por_min`)", Q, "GB/min"), "Vazão média (GB/min)", (3, 1, 3, 5),
                cor=("tipo_job", "`tipo_job`", C, "Tipo"), agrupado=True),
        tabela("t_des", "desempenho", [("concluido_em", "Concluído"), ("cod_objeto", "Objeto"), ("grupo", "Grupo"), ("modo", "Modo"),
                                       ("tipo_job", "Tipo"), ("lote_id", "Lote"), ("tamanho_mb", "MB"),
                                       ("duracao_s", "Duração (s)"), ("carga_s", "Carga (s)"), ("qtd_inserida", "Registros")],
               "Arquivos concluídos (30 dias)", 6, 7),
    ])
    hist = pagina("historico", "Histórico", [
        texto("t_hist", "## Histórico (30 dias)", 0, 0),
        grafico("g_gbdia", "bar", "diario", ("dia", "`dia`", T, "Dia"), ("arquivos", "`arquivos`", Q, "Arquivos"),
                "Arquivos concluídos por dia", (0, 1, 3, 5), agregado=False),
        grafico("g_errdia", "bar", "erros_dia", ("dia", "`dia`", T, "Dia"), ("sum(qtd)", "SUM(`qtd`)", Q, "Eventos"),
                "Erros e alertas por dia", (3, 1, 3, 5), cor=("status", "`status`", C, "Tipo")),
        tabela("t_dia", "diario", [("dia", "Dia"), ("arquivos", "Arquivos"), ("gb", "GB"), ("linhas", "Registros"),
                                   ("ultimo_concluido", "Último concluído")], "Resumo diário", 6, 6),
    ])
    usados = ["janela", "backlog_fila", "em_execucao", "objetos_execucao", "jobs_execucao", "lotes", "ciclos", "travados", "eventos", "controle",
              "desempenho", "diario", "erros_dia", "filas"]
    return {"datasets": [ds[n] for n in usados], "pages": [agora, objetos, desemp, hist]}


def dash_custo(ds):
    p = pagina("custo", "Custo", [
        texto("t_custo", "## PLHDWA · Custo", 0, 0),
        texto("s_custo", "Consumo dos jobs com a tag OWNER = PLHDWA · valores a preço de lista (sem descontos) · "
                         "o billing pode levar algumas horas para atualizar", 0, 1),
        contador("k_usd", "custo", None, "Custo 30 dias (US$)", 0, 2, agregado=("sum(custo_usd)", "SUM(`custo_usd`)")),
        contador("k_dbu", "custo", None, "DBUs 30 dias", 2, 2, agregado=("sum(dbus)", "SUM(`dbus`)")),
        contador("k_runs", "custo", None, "Execuções", 4, 2,
                 agregado=("countdistinct(job_run_id)", "COUNT(DISTINCT `job_run_id`)")),
        grafico("g_dia", "bar", "custo", ("daily(usage_date)", 'DATE_TRUNC("DAY", `usage_date`)', T, "Dia"),
                ("sum(custo_usd)", "SUM(`custo_usd`)", Q, "US$"), "Custo diário por job", (0, 5, 6, 6),
                cor=("job", "`job`", C, "Job")),
        grafico("g_tipo", "bar", "custo", ("tipo_job", "`tipo_job`", C, "Tipo do job"),
                ("sum(custo_usd)", "SUM(`custo_usd`)", Q, "US$"), "Custo por tipo de job", (0, 11, 3, 5)),
        grafico("g_fila", "bar", "custo_fila", ("modo", "`modo`", C, "Modo"),
                ("sum(custo_usd)", "SUM(`custo_usd`)", Q, "US$"),
                "Custo por modo (JOB exato; DESPACHANTE inclui o despacho)", (3, 11, 3, 5)),
        grafico("g_tb", "line", "custo_tb", ("dia", "`dia`", T, "Dia"),
                ("custo_por_tb_usd", "`custo_por_tb_usd`", Q, "US$/TB"), "Custo por TB carregado", (0, 16, 6, 5),
                agregado=False),
        tabela("t_obj", "custo_objeto", [("cod_objeto", "Objeto"), ("modo", "Modo"), ("custo_usd", "US$"),
                                         ("gb", "GB")], "Top 20 objetos por custo (DESPACHANTE por rateio de GB)", 21, 6),
    ])
    return {"datasets": [ds[n] for n in ["custo", "custo_fila", "custo_tb", "custo_objeto"]], "pages": [p]}


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--plh", default="", help="catalogo.schema (opcional; padrão: sem qualificar, o bundle define)")
    a.add_argument("--owner-tag", default="PLHDWA", help="valor da tag OWNER (filtro do painel de custo)")
    args = a.parse_args()
    ds = datasets(args.plh, args.owner_tag)
    for nome, d in [("plhdwa_execucao", dash_execucao(ds)), ("plhdwa_custo", dash_custo(ds))]:
        with open(os.path.join(AQUI, f"{nome}.lvdash.json"), "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
        print(nome, len(d["datasets"]), "datasets,", sum(len(p["layout"]) for p in d["pages"]), "widgets")


if __name__ == "__main__":
    main()
