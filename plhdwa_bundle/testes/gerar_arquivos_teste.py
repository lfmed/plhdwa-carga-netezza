"""
Gera pares .dat/.CTL de teste para o motor PLHDWA, a partir do fatparcela.dat real.

Roda em qualquer lugar com Python 3 (sem dependências):
  - local:      python3 gerar_arquivos_teste.py --cenario todos --destino ./saida
  - Databricks: num notebook serverless, destino = /Volumes/<cat>/<schema>/landing/netezza
                (arquivos grandes, para medir tempo, devem ser gerados direto no volume)

Para cada arquivo, grava o .dat primeiro e o .CTL por último, igual ao Netezza.
Também grava manifesto_<cenario>.csv com a fila e o resultado ESPERADOS de cada arquivo:
o notebook de validação compara esse manifesto com o log do motor.
"""
import argparse
import csv
import os
import random
import time
from datetime import datetime, timedelta, timezone

GMT3 = timezone(timedelta(hours=-3))
INSTITUICOES = [501172, 501174, 501204, 501207, 501208]      # valores de SK_INS do arquivo real
DATA_MOV = 20250110                                         # SK_TMO_MVM do arquivo real
MB = 1024 * 1024

# Limites de fila usados nos testes (ver ctl_filas_teste.sql): P < 1 MB, M de 1 a 20 MB, G >= 20 MB.
TAMANHO = {"P": 200 * 1024, "M": 3 * MB, "G": 25 * MB}


class Gerador:
    def __init__(self, amostra, destino):
        linhas = open(amostra, encoding="latin-1").read().splitlines()
        self.header = linhas[0]
        self.colunas = self.header.split("|")
        self.modelos = [l.split("|") for l in linhas[1:] if l]
        self.idx = {c: i for i, c in enumerate(self.colunas)}
        self.destino = destino
        self.relogio = datetime.now(GMT3).replace(microsecond=0)   # data de geração, sempre crescente
        self.manifesto = []
        for pasta in ("raw/data", "raw/ctl"):
            os.makedirs(os.path.join(destino, pasta), exist_ok=True)

    # ---------- linhas ----------
    def linha(self, data_mov=DATA_MOV, inst=None, mexer=None):
        """Copia uma linha real e troca data de movimento, instituição e chaves (para não ficar tudo igual)."""
        v = list(random.choice(self.modelos))
        v[self.idx["SK_TMO_MVM"]] = str(data_mov)
        v[self.idx["SK_INS"]] = str(inst or random.choice(INSTITUICOES))
        v[self.idx["SK_CLI"]] = str(random.randint(500_000_000, 599_999_999))
        v[self.idx["NR_CON_CRD"]] = str(random.randint(1_000_000, 9_999_999))
        if mexer:
            mexer(v, self.idx)
        return "|".join(v)

    def proxima_geracao(self):
        self.relogio += timedelta(seconds=7)
        return self.relogio.strftime("%Y%m%d%H%M%S")

    # ---------- arquivos ----------
    def par(self, cod, cenario, fila_esperada, resultado_esperado, tamanho=TAMANHO["P"], data_mov=DATA_MOV,
            insts_ctl="0", insts_dados=None, header=None, mexer=None, gerar_dat=True, ctl_conteudo=None,
            geracao=None, tabela="FATPARCELA"):
        geracao = geracao or self.proxima_geracao()
        base = f"{cod}_{tabela}_{data_mov}_{geracao}"
        dat = os.path.join(self.destino, "raw/data", base + ".dat")
        ctl = os.path.join(self.destino, "raw/ctl", base + ".CTL")
        if gerar_dat:
            self._escrever_dat(dat, tamanho, data_mov, insts_dados, header or self.header, mexer)
        # CTL por último: é ele que dispara o File Arrival.
        with open(ctl, "w", encoding="latin-1", newline="\n") as f:
            f.write(ctl_conteudo or f"{cod}|{tabela}|{data_mov}|{geracao}|{insts_ctl}\n")
        self.manifesto.append({"cenario": cenario, "cod_objeto": cod, "arquivo_ctl": base + ".CTL",
                               "tamanho_bytes": os.path.getsize(dat) if gerar_dat else 0,
                               "fila_esperada": fila_esperada, "resultado_esperado": resultado_esperado})
        return base

    def _escrever_dat(self, caminho, tamanho, data_mov, insts, header, mexer):
        """Escreve até atingir o tamanho. Para arquivos grandes, repete um bloco pronto (rápido)."""
        with open(caminho, "w", encoding="latin-1", newline="\n") as f:
            f.write(header + "\n")
            escrito = len(header) + 1
            nova = lambda: self.linha(data_mov, random.choice(insts) if insts else None, mexer) + "\n"
            if tamanho < 8 * MB:                       # pequeno: linha a linha, para respeitar o tamanho
                while escrito < tamanho:
                    l = nova()
                    f.write(l)
                    escrito += len(l)
                return
            bloco = "".join(nova() for _ in range(2000))   # grande: repete um bloco pronto (~1 MB)
            while escrito < tamanho:
                f.write(bloco)
                escrito += len(bloco)

    def salvar_manifesto(self, nome):
        caminho = os.path.join(self.destino, f"manifesto_{nome}.csv")
        with open(caminho, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(self.manifesto[0].keys()))
            w.writeheader()
            w.writerows(self.manifesto)
        print(f"{len(self.manifesto)} arquivos · manifesto: {caminho}")


# ---------- cenários ----------
def roteamento(g):
    """Um objeto por fila + um objeto que troca de fila entre arquivos (P -> G -> P)."""
    g.par(9001, "roteamento", "P", "CONCLUIDO", TAMANHO["P"])
    g.par(9002, "roteamento", "M", "CONCLUIDO", TAMANHO["M"])
    g.par(9003, "roteamento", "G", "CONCLUIDO", TAMANHO["G"])
    for fila in ("P", "G", "P"):
        g.par(9004, "roteamento", fila, "CONCLUIDO", TAMANHO[fila])


def concorrencia(g, objetos=20, arquivos=3):
    """20 objetos x 3 arquivos pequenos: testa paralelismo entre objetos e série dentro do objeto."""
    for n in range(objetos):
        for _ in range(arquivos):
            g.par(9011 + n, "concorrencia", "P", "CONCLUIDO", TAMANHO["P"])


def ordem(g):
    """Grava os arquivos do mais NOVO para o mais antigo: o motor deve processar pela data de geração."""
    geracoes = [g.proxima_geracao() for _ in range(3)]
    for ger in reversed(geracoes):
        g.par(9040, "ordem", "P", "CONCLUIDO", TAMANHO["P"], geracao=ger)


def reprocessamento(g):
    """Mesmo movimento duas vezes: a 2ª só com 2 instituições -> REPLACE WHERE troca só essas duas."""
    g.par(9041, "reprocessamento", "P", "CONCLUIDO", TAMANHO["P"], insts_ctl="0")
    g.par(9041, "reprocessamento", "P", "CONCLUIDO", TAMANHO["P"], insts_ctl="501204,501207",
          insts_dados=[501204, 501207])


def tempo(g, tamanho_mb, cod=9050):
    """Um arquivo grande para medir tempo por GB (gere direto no volume quando for > 1 GB)."""
    fila = "P" if tamanho_mb * MB < TAMANHO["M"] else ("M" if tamanho_mb * MB < 20 * MB else "G")
    g.par(cod, "tempo", fila, "CONCLUIDO", tamanho_mb * MB)


def erros(g):
    """Um objeto por tipo de erro. Todos devem ir para error/ e travar só o próprio objeto."""
    def letra_em_inteiro(v, i): v[i["NR_PRL"]] = "ABC"
    def campo_a_menos(v, i): v.pop()
    def pipe_no_texto(v, i): v[i["DS_CON_MAE"]] = "CONTRATO 2|3 VIAS"      # sem ESCAPECHAR, quebra a linha
    g.par(9101, "erros", "P", "ERRO_SCHEMA", header=g.header + "|COLUNA_NOVA",
          mexer=lambda v, i: v.append("1"))
    g.par(9102, "erros", "P", "ERRO_DADOS", mexer=letra_em_inteiro)
    g.par(9103, "erros", "P", "ERRO_DADOS", ctl_conteudo=None, data_mov=20250111,
          mexer=lambda v, i: v.__setitem__(i["SK_TMO_MVM"], "20250110"))      # dados de 10/01, CTL de 11/01
    g.par(9104, "erros", "P", "ERRO_DADOS", insts_ctl="501204", insts_dados=[501204, 501208])
    g.par(9105, "erros", "P", "ERRO_PAR", gerar_dat=False)
    g.par(9106, "erros", "P", "ERRO_CTL", ctl_conteudo="9999|FATPARCELA|20250110|20200101000000|0\n")
    g.par(9107, "erros", "P", "ERRO_DADOS", mexer=campo_a_menos)
    g.par(9108, "erros", "P", "ERRO_DADOS", mexer=pipe_no_texto)
    g.par(9110, "erros", "P", "ERRO_CTL", insts_ctl="501204", tabela="DIMTESTE")   # DIM com lista
    g.par(9199, "erros", "P", "ERRO_CADASTRO")                                     # código sem tabela


def escape(g):
    """Tabela 9109 com plhdwa.arquivo.escape = '\\': '2\\|3' vira '2|3' e '\\\\' vira uma barra (recomendação V1)."""
    def com_escape(v, i): v[i["DS_CON_MAE"]] = "CONTRATO 2\\|3 VIAS C:\\\\TEMP"
    g.par(9109, "escape", "P", "CONCLUIDO", mexer=com_escape)


def db2(g):
    """Objeto 9130 listado em db2_sim_falhas: carga OK, Db2 falha -> ERRO_DB2 e arquivos ficam em raw/."""
    g.par(9130, "db2", "P", "ERRO_DB2")


def sequencia(g, parte):
    """Parte 1 carrega um arquivo; parte 2 manda um arquivo com data de geração MAIS ANTIGA -> ERRO_SEQUENCIA."""
    if parte == 1:
        g.par(9120, "sequencia", "P", "CONCLUIDO", geracao="20300101000000")
    else:
        g.par(9120, "sequencia", "P", "ERRO_SEQUENCIA", geracao="20290101000000")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--amostra", default=os.path.join(os.path.dirname(__file__), "..", "..", "fatparcela.dat"))
    p.add_argument("--destino", default=os.path.join(os.path.dirname(__file__), "saida"))
    p.add_argument("--cenario", required=True,
                   choices=["roteamento", "concorrencia", "ordem", "reprocessamento", "tempo", "erros",
                            "escape", "db2", "sequencia1", "sequencia2", "todos"])
    p.add_argument("--tamanho-mb", type=int, default=512, help="cenário tempo")
    p.add_argument("--cod-tempo", type=int, default=9050, help="cenário tempo")
    a = p.parse_args()

    random.seed(42)
    g = Gerador(a.amostra, a.destino)
    inicio = time.time()
    if a.cenario in ("roteamento", "todos"): roteamento(g)
    if a.cenario in ("concorrencia", "todos"): concorrencia(g)
    if a.cenario in ("ordem", "todos"): ordem(g)
    if a.cenario in ("reprocessamento", "todos"): reprocessamento(g)
    if a.cenario in ("erros", "todos"): erros(g)
    if a.cenario in ("escape", "todos"): escape(g)
    if a.cenario in ("sequencia1", "todos"): sequencia(g, 1)
    if a.cenario == "db2": db2(g)
    if a.cenario == "sequencia2": sequencia(g, 2)
    if a.cenario == "tempo": tempo(g, a.tamanho_mb, a.cod_tempo)
    g.salvar_manifesto(a.cenario)
    print(f"gerado em {time.time() - inicio:.1f}s em {a.destino}")


if __name__ == "__main__":
    main()
