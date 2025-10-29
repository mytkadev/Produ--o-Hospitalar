# ===============================================================
# 📄 DESCRIÇÃO GERAL DO SCRIPT
# ===============================================================
#
# Este script foi desenvolvido para automatizar o processamento 
# de planilhas mensais de produção ambulatorial hospitalar.
# Ele realiza a leitura, padronização, validação, registro de erros,
# e inserção estruturada dos dados em uma base central em Excel,
# além de gerar logs e controle de envios por hospital e mês.
#
# Esse script funciona ESPECIFICAMENTE para a seção de "Ambulatório",
# para demais seções, ver documentação oficial ou script main.py
#
# ✅ O código está completamente documentado e dividido por seções,
# com explicações detalhadas para facilitar a compreensão,
# manutenção futura e colaboração em equipe.
#
# Caso haja dúvidas, sugestões ou necessidade de ajustes,
# entre em contato diretamente comigo:
# 👩‍💻 Autora: Ana Vitória
# 🔗 LinkedIn: https://www.linkedin.com/in/anavitoriabaetas/
#
# Fique à vontade para adaptar o código às suas necessidades,
# mas lembre-se de revisar as regras de negócio específicas que
# foram implementadas, principalmente as de padronização e validação.
#
# ===============================================================

# ===============================================================
# IMPORTAÇÃO DE BIBLIOTECAS NECESSÁRIAS
# ===============================================================
 
import os                                           # Lidar com caminhos e arquivos do sistema
import unicodedata                                  # Normalizar strings (ex: remover acentos)
import pandas as pd                                 # Manipulação de planilhas e dados em tabelas
import shutil                                       # Mover arquivos entre pastas
import re                                           # Expressões regulares
import json
import argparse
from openpyxl import load_workbook                  # Leitura e escrita em arquivos Excel (.xlsx)
from fuzzywuzzy import process                      # Matching aproximado de texto (fuzzy matching)
from datetime import datetime
from dateutil.relativedelta import relativedelta    # Manipular datas relativas (ex: mês anterior)
from openpyxl import Workbook

# =========================
# UI helpers (prompt/menus)
# =========================

def _line():
    return "─" * 63

def _hdr(t):
    return f"┌{_line()}┐\n│ {t.ljust(61)}│\n└{_line()}┘"

def _ask(msg):
    return input(f"{msg.strip()} ").strip()

def _pause():
    input("\n(Pressione Enter para continuar...)")

def _lista_paginada(opcoes, titulo="Lista", por_pagina=50):
    """
    Mostra itens numerados com paginação. Retorna o índice (0-based) escolhido,
    ou -1 se o usuário digitar 0 para “digitar manualmente”.
    """
    if not opcoes:
        print("Lista vazia.")
        return -1
    total = len(opcoes)
    pagina = 0
    while True:
        ini = pagina * por_pagina
        fim = min(ini + por_pagina, total)
        print("\n" + _hdr(titulo))
        print(" 0) Digitar manualmente")
        for i, item in enumerate(opcoes[ini:fim], start=1):
            print(f"{i}) {item}")
        print(f"\nPágina {pagina+1}/{(total-1)//por_pagina+1}  (N=próx, P=ant, Q=sair)")
        resp = _ask("Escolha um número, ou N/P/Q:").lower()
        if resp == "q":
            return -1
        if resp == "n" and fim < total:
            pagina += 1
            continue
        if resp == "p" and pagina > 0:
            pagina -= 1
            continue
        if resp.isdigit():
            n = int(resp)
            if n == 0:
                return -1
            if 1 <= n <= (fim - ini):
                return ini + (n - 1)
        print("Opção inválida.")

def _confirmar_mapeamento(orig: str, destino: str) -> str:
    """
    Mostra uma confirmação visual do mapeamento.
    Retorna: 'c' (confirmar), 'v' (voltar e escolher de novo),
             'l' (enviar pro LOG), 'q' (cancelar).
    """
    print()
    print(_hdr("Confirme a modificação"))
    print(f"Especialidade original: {_ansi(orig, '1;37')}")
    print(f"→ Proposta de destino:  {_ansi(destino, '1;36')}\n")
    print("  [C] Confirmar e continuar")
    print("  [V] Voltar e escolher novamente")
    print("  [L] Não mapear agora e enviar para o LOG")
    print("  [Q] Cancelar (sem ação)")

    while True:
        esc = _ask("Escolha (C/V/L/Q): ").lower()
        if esc in ("c", "v", "l", "q"):
            return esc
        print("Opção inválida.")

# =========================
# Helpers de retificação
# =========================

def _input_motivo_padrao() -> str:
    """Pergunta e retorna um motivo padronizado para LOG."""
    print("\nMotivo:")
    print("  [a] Perguntar para Sara")
    print("  [b] Rever nos registros anteriores")
    print("  [c] Solicitar retificação ao hospital")
    print("  [d] Outros")
    while True:
        m = _ask("Escolha (a/b/c/d): ").lower()
        if m in ("a", "b", "c", "d"):
            break
        print("Opção inválida.")
    if m == "a": return "Perguntar para Sara"
    if m == "b": return "Rever nos registros anteriores"
    if m == "c": return "Solicitar retificação ao hospital"
    outro = _ask("Digite o motivo: ").strip()
    return outro or "Outros (sem detalhamento)"


def _listar_especialidades_log_unicas() -> list[str]:
    """Lê o log de erros (ambulatorio_log) e retorna a lista única (ordenada) de especialidade_original."""
    try:
        df_log = pd.read_excel(CAMINHO_LOGS, sheet_name="ambulatorio_log", engine="openpyxl")
        if df_log.empty:
            return []
    except Exception:
        return []
    col = "especialidade_original"
    if col not in df_log.columns:
        return []
    esp = (
        df_log[col]
        .astype(str)
        .fillna("")
        .map(lambda s: s.strip())
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    esp.sort(key=lambda s: unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8').lower())
    return esp

def _atualizar_qualificacao_por_retificacao(ajustes: list[dict]):
    """
    Aplica deltas na aba 'Ambulatorio' (Qualificação de Dados.xlsx) por (arquivo, competencia):
      delta_linhas_logs, delta_linhas_base, delta_soma_logs, delta_soma_base.
    Se a linha não existir, cria com os deltas (demais campos 0). Recalcula status.
    """
    garantir_quali_dados()  # <-- nome novo

    try:
        dfq = pd.read_excel(CAMINHO_QUALI_DADOS, sheet_name=ABA_QUALI_AMB, engine="openpyxl")
    except Exception:
        # estrutura mínima
        dfq = pd.DataFrame(columns=[
            "Data_Registro",
            "arquivo","cnes","nome_hospital","competencia",
            "linhas_raw","linhas_base","linhas_logs","status_linhas",
            "soma_raw","soma_base","soma_logs","status_soma"
        ])

    def _recalcular_status(l_raw, l_base, l_logs, s_raw, s_base, s_logs):
        if l_logs > 0:
            status_linhas = "Pendente"
        elif l_raw == (l_base + l_logs):
            status_linhas = "OK"
        elif l_raw > (l_base + l_logs):
            status_linhas = "Falta linha"
        else:
            status_linhas = "Sobrando linha"
        status_soma = "OK" if s_raw == (s_base + s_logs) else "Divergente"
        return status_linhas, status_soma

    for a in ajustes:
        arquivo      = a["arquivo"]
        competencia  = a["competencia"]
        cnes         = a.get("cnes", "")
        nome_hosp    = a.get("nome_hospital", "")

        d_ll = int(a.get("delta_linhas_logs", 0) or 0)
        d_lb = int(a.get("delta_linhas_base", 0) or 0)
        d_sl = int(a.get("delta_soma_logs", 0) or 0)
        d_sb = int(a.get("delta_soma_base", 0) or 0)

        mask = (dfq.get("arquivo")==arquivo) & (dfq.get("competencia")==competencia)
        if mask.any():
            idx = dfq[mask].index[0]

            # preenche cnes/nome_hospital se estiverem vazios
            if not pd.notna(dfq.at[idx, "cnes"]) or str(dfq.at[idx, "cnes"]).strip()=="":
                dfq.at[idx, "cnes"] = cnes
            if not pd.notna(dfq.at[idx, "nome_hospital"]) or str(dfq.at[idx, "nome_hospital"]).strip()=="":
                dfq.at[idx, "nome_hospital"] = nome_hosp

            # aplica deltas
            for col, delta in (("linhas_logs", d_ll), ("linhas_base", d_lb),
                               ("soma_logs", d_sl), ("soma_base", d_sb)):
                try:
                    dfq.at[idx, col] = int(dfq.at[idx, col]) + delta
                except Exception:
                    dfq.at[idx, col] = delta

            # recalcula status
            l_raw = int(dfq.at[idx, "linhas_raw"] or 0)
            l_base= int(dfq.at[idx, "linhas_base"] or 0)
            l_logs= int(dfq.at[idx, "linhas_logs"] or 0)
            s_raw = int(dfq.at[idx, "soma_raw"] or 0)
            s_base= int(dfq.at[idx, "soma_base"] or 0)
            s_logs= int(dfq.at[idx, "soma_logs"] or 0)
            st_l, st_s = _recalcular_status(l_raw,l_base,l_logs,s_raw,s_base,s_logs)
            dfq.at[idx,"status_linhas"] = st_l
            dfq.at[idx,"status_soma"]   = st_s

        else:
            # cria nova linha com Data_Registro agora
            novo = {
                "Data_Registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "arquivo": arquivo,
                "cnes": cnes,
                "nome_hospital": nome_hosp,
                "competencia": competencia,
                "linhas_raw": 0,
                "linhas_base": d_lb,
                "linhas_logs": d_ll,
                "soma_raw": 0,
                "soma_base": d_sb,
                "soma_logs": d_sl,
            }
            st_l, st_s = _recalcular_status(novo["linhas_raw"],novo["linhas_base"],novo["linhas_logs"],
                                            novo["soma_raw"],novo["soma_base"],novo["soma_logs"])
            novo["status_linhas"] = st_l
            novo["status_soma"]   = st_s
            dfq = pd.concat([dfq, pd.DataFrame([novo])], ignore_index=True)

    with pd.ExcelWriter(CAMINHO_QUALI_DADOS, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        dfq.to_excel(w, sheet_name=ABA_QUALI_AMB, index=False)


# ===============================================================
# CONFIGURAÇÕES GERAIS E PARÂMETROS
# ===============================================================
# Pasta do arquivo atual (…/Produção Hospitalar/Códigos)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Pasta "Produção Hospitalar" (subir um nível a partir de /Códigos)
PRODUCAO_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))

# Pasta "Controle" (irmã de "Códigos" dentro de "Produção Hospitalar")
CONTROLE_DIR = os.path.join(PRODUCAO_DIR, "Controle")
os.makedirs(CONTROLE_DIR, exist_ok=True)

# Decisões por hospital+termo (persistidas em JSON)
# Formato: { cnes_str: { TERMO_UP: {"acao": "M"|"L", "destino": str|None, "motivo": str|None} } }
decisoes_especialidades = {}

# Arquivo de configuração (mantido ao lado do script)
CONFIG_PATH = os.path.join(SCRIPT_DIR, "ambulatorio_config.json")

# === ARQUIVOS DE CONTROLE (consolidados/reorganizados) ===

# (NOVO) Só Qualificação (com Data_Registro) — arquivo e aba renomeados
CAMINHO_QUALI_DADOS = os.path.join(CONTROLE_DIR, "Qualificação de Dados.xlsx")
ABA_QUALI_AMB = "Ambulatorio"  # antes: "Ambulatorial – Qualificação"

# 2) Grade (formato da tabela por competência, igual ao print)
CAMINHO_CONTROLE_ATUALIZACAO_GRADE = os.path.join(CONTROLE_DIR, "Controle de Atualização.xlsx")
ABA_GRADE = "Ambulatorial – Grade"

# 3) Mudanças + Decisões manuais de registro → MESMO ARQUIVO
CAMINHO_CONTROLE_MUD_REG = os.path.join(CONTROLE_DIR, "Controle de Mudanças e Registros.xlsx")
ABA_MUD_REG = "Ambulatório"

# 4) Log de erros (especialidades/consultórios não reconhecidos)
CAMINHO_LOGS = os.path.join(CONTROLE_DIR, "Log de Erros.xlsx")

# === PASTAS DE PLANILHAS ===
PLANILHAS_DIR = os.path.join(PRODUCAO_DIR, "Planilhas")
CAMINHO_PLANILHAS = os.path.join(PLANILHAS_DIR, "A serem processadas")
CAMINHO_ARQUIVADAS = os.path.join(PLANILHAS_DIR, "Processadas")

# === BASE DE DADOS PRINCIPAL ===
# (arquivo de fato utilizado para receber os dados válidos)
BASES_DIR = os.path.abspath(os.path.join(PRODUCAO_DIR, os.pardir, "Bases de Dados"))
NOME_ARQUIVO_BASE = "dbProducao.xlsx"
CAMINHO_BASE = os.path.join(BASES_DIR, NOME_ARQUIVO_BASE)

# Dicionário hospitalar (referência para CNES)
CAMINHO_DHOSPITAIS = os.path.join(BASES_DIR, "dHospitais.xlsx")
# --- Helpers para normalização e nome oficial ---
def _normalizar_cnes(cnes_raw: str) -> str:
    """Mantém apenas dígitos e preenche à esquerda para 7 dígitos (padrão CNES)."""
    dig = re.sub(r"\D+", "", str(cnes_raw or "").strip())
    return dig.zfill(7) if dig else ""

_HOSP_CACHE = None  # cache em memória

def _nome_oficial_por_cnes(cnes_norm: str) -> str | None:
    """Retorna nome oficial a partir do dHospitais.xlsx (col A=CNES, col D=Nome)."""
    global _HOSP_CACHE
    cnes_norm = _normalizar_cnes(cnes_norm)
    if not cnes_norm:
        return None
    try:
        if _HOSP_CACHE is None:
            if not os.path.exists(CAMINHO_DHOSPITAIS):
                return None
            xls = pd.ExcelFile(CAMINHO_DHOSPITAIS, engine="openpyxl")
            aba = "hospitais" if "hospitais" in [s.lower() for s in xls.sheet_names] else xls.sheet_names[0]
            df = pd.read_excel(xls, sheet_name=aba, engine="openpyxl").iloc[:, [0, 3]].copy()
            df.columns = ["cnes", "nome_hospital"]
            df["cnes"] = df["cnes"].map(_normalizar_cnes)
            df["nome_hospital"] = df["nome_hospital"].astype(str).str.strip().str.upper()
            _HOSP_CACHE = df.set_index("cnes")["nome_hospital"].to_dict()
        return _HOSP_CACHE.get(cnes_norm)
    except Exception:
        return None

# Garante que as pastas existem
os.makedirs(CAMINHO_PLANILHAS, exist_ok=True)
os.makedirs(CAMINHO_ARQUIVADAS, exist_ok=True)
os.makedirs(BASES_DIR, exist_ok=True)

NOME_ABA = "ambulatorioAtendimentos"
NOME_ABA_2 = "ambulatorioConsultorios"


# Lista padrão de especialidades esperadas para matching
lista_especialidades_ambulatorio = [
    "Anestesiologia", 
    "Assistente Social", 
    "Buco Maxilo Facial", 
    "Cardiologia", 
    "Cirurgia de Cabeça e Pescoço", 
    "Cirurgia Geral", 
    "Cirurgia Ginecológica", 
    "Cirurgia Homem Trans", 
    "Cirurgia Ortopédica Pediátrica", 
    "Cirurgia Pediátrica", 
    "Cirurgia Plástica", 
    "Cirurgia Proctológica", 
    "Cirurgia Torácica", 
    "Cirurgia Urológica", 
    "Cirurgia Vascular", 
    "Clínica Médica", 
    "Colproctologia", 
    "Dermatologia", 
    "Endocrinologia", 
    "Endoscopia", 
    "Enfermeiro", 
    "Fisioterapia", 
    "Fisioterapia Queimados", 
    "Fonoaudiologia", 
    "Gastroenterologia", 
    "Geriatria", 
    "Hebiatria", 
    "Hebiatria Adolescentes", 
    "Hematologia", 
    "Hepatologia", 
    "Homeopatia", 
    "Imunologia", 
    "Infectologia", 
    "Mastologia", 
    "Neonatologia", 
    "Nefrologia Pediátrica", 
    "Neurocirurgia", 
    "Neurologia", 
    "Nutrição", 
    "Oftalmologia", 
    "Ortopedia", 
    "Ortodontia", 
    "Otorrinolaringologia", 
    "Pediatria", 
    "Pneumologia", 
    "Pneumologia Pediátrica", 
    "Proctologia", 
    "Psicologia", 
    "Psiquiatria", 
    "Queimados", 
    "Reumatologia", 
    "Serviço Social", 
    "Terapia Ocupacional", 
    "Terapia Ocupacional Queimados", 
    "Uroginecologia", 
    "Urologia",
]

# Hospitais que não possuem ambulatório (exceções conhecidas)
cnes_sem_ambulatorio = [
    "161438",  
    "7638698",
]

# Termos que devem ser ignorados no processamento (ex: funções administrativas)
termos_proibidos = [
    "AUXILIAR DE ENFERMAGEM",
    "TECNICO DE ENFERMAGEM",
    "TÉCNICO DE ENFERMAGEM"
]

substituicoes_especialidades = {}

# Flags auxiliares de interação
ULTIMA_RESOLUCAO_TEXTO = None   # preenchida quando usuário escolhe correção manual (M)
ULTIMO_MOTIVO_ERRO = None       # preenchida quando usuário escolhe mandar pro log (L)

def garantir_quali_dados():
    """Garante o arquivo 'Qualificação de Dados.xlsx' com a aba 'Ambulatorio'."""
    os.makedirs(CONTROLE_DIR, exist_ok=True)
    precisa_criar = not os.path.exists(CAMINHO_QUALI_DADOS)
    if precisa_criar:
        with pd.ExcelWriter(CAMINHO_QUALI_DADOS, engine="openpyxl", mode="w") as w:
            pd.DataFrame(columns=[
                "Data_Registro",
                "arquivo",
                "cnes",
                "nome_hospital",
                "competencia",
                "linhas_raw",
                "linhas_base",
                "linhas_logs",
                "status_linhas",
                "soma_raw",
                "soma_base",
                "soma_logs",
                "status_soma"
            ]).to_excel(w, sheet_name=ABA_QUALI_AMB, index=False)
        print(f"📄 Criado '{CAMINHO_QUALI_DADOS}' com a aba '{ABA_QUALI_AMB}'.")


def _mm_aaaa(comp_yyyy_mm: str) -> str:
    # "2025-01" -> "01-2025"
    try:
        y, m = comp_yyyy_mm.split("-")
        return f"{m}-{y}"
    except Exception:
        return comp_yyyy_mm

def _garantir_grade_vazia():
    cols = ["CNES", "Hospital"]  # meses serão adicionados on-demand
    df = pd.DataFrame(columns=cols)
    with pd.ExcelWriter(CAMINHO_CONTROLE_ATUALIZACAO_GRADE, engine="openpyxl", mode="w") as w:
        df.to_excel(w, sheet_name=ABA_GRADE, index=False)
    print(f"📄 Criado '{CAMINHO_CONTROLE_ATUALIZACAO_GRADE}' com a aba '{ABA_GRADE}'.")

def garantir_controle_atualizacao_grade():
    os.makedirs(CONTROLE_DIR, exist_ok=True)
    if not os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE):
        _garantir_grade_vazia()
    else:
        # garante que a aba exista
        try:
            pd.read_excel(CAMINHO_CONTROLE_ATUALIZACAO_GRADE, sheet_name=ABA_GRADE, engine="openpyxl")
        except Exception:
            _garantir_grade_vazia()

def atualizar_controle_atualizacao_grade(*_args, **_kwargs):
    """
    Reconstrói a 'Ambulatorial – Grade' com TODOS os hospitais (dHospitais.xlsx)
    e TODAS as competências encontradas na base db_ambulatorio, marcando:
      ✅ se há qualquer registro para (CNES, competência)
      ❌ caso contrário.
    Ignora parâmetros antigos; agora é uma visão global, no estilo da 'Envio'.
    """
    try:
        # Hospitais (CNES na col A, Nome na col D)
        if not os.path.exists(CAMINHO_DHOSPITAIS):
            print(f"⚠️ dHospitais.xlsx não encontrado em: {CAMINHO_DHOSPITAIS}")
            return
        xls = pd.ExcelFile(CAMINHO_DHOSPITAIS, engine="openpyxl")
        aba = "hospitais" if "hospitais" in [s.lower() for s in xls.sheet_names] else xls.sheet_names[0]
        df_hosp = pd.read_excel(xls, sheet_name=aba, engine="openpyxl").iloc[:, [0, 3]].copy()
        df_hosp.columns = ["CNES", "Hospital"]
        df_hosp["CNES"] = df_hosp["CNES"].astype(str).str.strip()
        df_hosp["Hospital"] = df_hosp["Hospital"].astype(str).str.strip().str.upper()

        # Base de produção (ambulatorio)
        if not os.path.exists(CAMINHO_BASE):
            print(f"ℹ️ Base '{CAMINHO_BASE}' não existe ainda; criando grade vazia.")
            df_out = df_hosp.copy()
            with pd.ExcelWriter(CAMINHO_CONTROLE_ATUALIZACAO_GRADE, engine="openpyxl",
                                mode=("a" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else "w"),
                                if_sheet_exists=("replace" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else None)) as w:
                df_out.to_excel(w, sheet_name=ABA_GRADE, index=False)
            return

        df_base = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA, engine="openpyxl")
        if df_base.empty:
            df_out = df_hosp.copy()
            with pd.ExcelWriter(CAMINHO_CONTROLE_ATUALIZACAO_GRADE, engine="openpyxl",
                                mode=("a" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else "w"),
                                if_sheet_exists=("replace" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else None)) as w:
                df_out.to_excel(w, sheet_name=ABA_GRADE, index=False)
            return

        df_base["cnes"] = df_base["cnes"].astype(str).str.strip()
        df_base["competencia"] = df_base["competencia"].astype(str).str.strip()

        # Competências únicas, ordenadas (YYYY-MM)
        comps = sorted(set(df_base["competencia"]), key=lambda s: s if re.match(r"^\d{4}-\d{2}$", s) else f"9999-99")
        # Colunas MM-YYYY
        cols_mes = [f"{c.split('-')[1]}-{c.split('-')[0]}" if re.match(r"^\d{4}-\d{2}$", c) else c for c in comps]

        # Monta grade ✅/❌
        df_out = df_hosp.copy()
        for comp_iso, col in zip(comps, cols_mes):
            mask_comp = df_base["competencia"] == comp_iso
            cnes_ok = set(df_base.loc[mask_comp, "cnes"].astype(str))
            df_out[col] = ["✅" if str(c) in cnes_ok else "❌" for c in df_out["CNES"].astype(str)]

        # Ordena colunas: CNES, Hospital, depois meses por ano+mês
        def _key(colname: str):
            if colname in ("CNES", "Hospital"):
                return (0, colname)
            try:
                m, y = colname.split("-")  # "MM-YYYY"
                return (1, f"{y}{m}")
            except Exception:
                return (2, colname)

        df_out = df_out[sorted(df_out.columns, key=_key)]

        with pd.ExcelWriter(CAMINHO_CONTROLE_ATUALIZACAO_GRADE, engine="openpyxl",
                            mode=("a" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else "w"),
                            if_sheet_exists=("replace" if os.path.exists(CAMINHO_CONTROLE_ATUALIZACAO_GRADE) else None)) as w:
            df_out.to_excel(w, sheet_name=ABA_GRADE, index=False)

        print(f"✅ Grade atualizada (estilo ✅/❌) com {len(df_out)} hospitais e {len(cols_mes)} competências.")

    except Exception as e:
        print(f"❌ Erro ao atualizar grade: {e}")


# ===============================================================
# FUNÇÕES AUXILIARES GERAIS
# ===============================================================


def _get_decisao(cnes: str, termo_up: str):
    cnes = str(cnes).strip()
    termo_up = str(termo_up).strip().upper()
    return (decisoes_especialidades.get(cnes) or {}).get(termo_up)

def _set_decisao(cnes: str, termo_up: str, acao: str, destino: str | None, motivo: str | None):
    cnes = str(cnes).strip()
    termo_up = str(termo_up).strip().upper()
    decisoes_especialidades.setdefault(cnes, {})
    decisoes_especialidades[cnes][termo_up] = {"acao": acao, "destino": destino, "motivo": motivo}
    salvar_config()  # persiste imediatamente

def carregar_config():
    """
    Lê overrides do arquivo JSON e aplica em memória (mutação, sem 'global').
    """
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)

            if isinstance(cfg.get("lista_especialidades_ambulatorio"), list):
                lista_especialidades_ambulatorio.clear()
                lista_especialidades_ambulatorio.extend(cfg["lista_especialidades_ambulatorio"])

            if isinstance(cfg.get("termos_proibidos"), list):
                termos_proibidos.clear()
                termos_proibidos.extend(cfg["termos_proibidos"])

            if isinstance(cfg.get("substituicoes_especialidades"), dict):
                substituicoes_especialidades.clear()
                substituicoes_especialidades.update(cfg["substituicoes_especialidades"])

            if isinstance(cfg.get("decisoes_especialidades"), dict):
                decisoes_especialidades.clear()
                decisoes_especialidades.update(cfg["decisoes_especialidades"])

            print(f"⚙️ Config carregada de {CONFIG_PATH}.")
        else:
            print("ℹ️ Nenhum config JSON encontrado; usando valores padrão.")
    except Exception as e:
        print(f"❌ Erro ao carregar config: {e}")

def wizard_editar_config_interativo():
    """
    Wizard simples no terminal para permitir adicionar/remover
    especialidades, termos proibidos e mapeamentos de substituição.
    """
    global lista_especialidades_ambulatorio, termos_proibidos, substituicoes_especialidades

    print("\n=== MODO EDIÇÃO DE CONFIG (INTERATIVO) ===")
    print("Você pode adicionar/remover itens. Deixe em branco para pular.\n")

    # 1) Especialidades
    print("Especialidades atuais (amostra até 10):", lista_especialidades_ambulatorio[:10], "...")
    while True:
        acao = input("Especialidades — [A]dicionar, [R]emover, [Enter] para continuar: ").strip().lower()
        if acao == "a":
            novo = input("Digite a especialidade a adicionar: ").strip()
            if novo and novo not in lista_especialidades_ambulatorio:
                lista_especialidades_ambulatorio.append(novo)
                print("✔️ Adicionada.")
        elif acao == "r":
            rem = input("Digite a especialidade a remover: ").strip()
            if rem in lista_especialidades_ambulatorio:
                lista_especialidades_ambulatorio.remove(rem)
                print("🗑️ Removida.")
        else:
            break

    # 2) Termos proibidos
    print("\nTermos proibidos atuais:", termos_proibidos)
    while True:
        acao = input("Termos proibidos — [A]dicionar, [R]emover, [Enter] para continuar: ").strip().lower()
        if acao == "a":
            novo = input("Digite o termo a adicionar (use a grafia exata que vem na planilha): ").strip().upper()
            if novo and novo not in termos_proibidos:
                termos_proibidos.append(novo)
                print("✔️ Adicionado.")
        elif acao == "r":
            rem = input("Digite o termo a remover: ").strip().upper()
            if rem in termos_proibidos:
                termos_proibidos.remove(rem)
                print("🗑️ Removido.")
        else:
            break

    # 3) Substituições
    print("\nSubstituições atuais (amostra até 5):", list(substituicoes_especialidades.items())[:5], "...")
    while True:
        acao = input("Substituições — [A]dicionar/atualizar, [R]emover, [Enter] para finalizar: ").strip().lower()
        if acao == "a":
            origem = input("Origem (como vem na planilha): ").strip()
            destino = input("Destino padronizado: ").strip()
            if origem and destino:
                substituicoes_especialidades[origem] = destino
                # se destino for novo, adiciona à lista de especialidades
                if destino not in lista_especialidades_ambulatorio:
                    lista_especialidades_ambulatorio.append(destino)
                print("✔️ Substituição registrada.")
        elif acao == "r":
            origem = input("Qual 'origem' deseja remover do mapa? ").strip()
            if origem in substituicoes_especialidades:
                del substituicoes_especialidades[origem]
                print("🗑️ Removida.")
        else:
            break

    # Salva tudo que foi feito
    salvar_config()
    print("✅ Edição concluída.\n")

def salvar_config():
    """
    Grava o estado atual das estruturas em JSON (apenas 1 vez).
    """
    try:
        cfg = {
            "lista_especialidades_ambulatorio": lista_especialidades_ambulatorio,
            "termos_proibidos": termos_proibidos,
            "substituicoes_especialidades": substituicoes_especialidades,
            "decisoes_especialidades": decisoes_especialidades,
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        print(f"💾 Config salva em {CONFIG_PATH}.")
    except Exception as e:
        print(f"❌ Erro ao salvar config: {e}")

def normalizar(texto):
    """
    Remove acentos e transforma em minúsculas para facilitar comparações.
    """
    texto_sem_acentos = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto_sem_acentos.lower().strip()

def mes_para_numero(mes_extenso):
    """
    Converte o nome do mês (ex: "maio") para número ("05")
    """
    meses = {
        "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04", "maio": "05", "junho": "06",
        "julho": "07", "agosto": "08", "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
    }
    return meses.get(mes_extenso.strip().lower())

def _prompt_menu(titulo: str, opcoes: list[tuple[str, str]], allow_empty: bool=False) -> str:
    """
    Exibe um menu simples:
      - opcoes: lista de pares (tecla, rótulo)
      - retorna a tecla escolhida, já normalizada (minúscula)
    """
    print("\n" + titulo.strip())
    for k, label in opcoes:
        print(f"  [{k}] {label}")
    while True:
        esc = input("→ Sua escolha: ").strip().lower()
        if allow_empty and esc == "":
            return ""
        if any(esc == k.lower() for k, _ in opcoes):
            return esc
        print("❌ Opção inválida. Tente novamente.")

def _escolher_da_lista_numerada(titulo: str, itens: list[str]) -> str | None:
    """
    Mostra itens numerados (1..N). Retorna o item escolhido.
    Retorna None se o usuário escolher '0' para informar manualmente.
    """
    print("\n" + titulo.strip())
    for i, it in enumerate(itens, start=1):
        print(f"  {i:>2}. {it}")
    print("  0. Digitar manualmente")

    while True:
        raw = input("→ Número (ou 0 para digitar): ").strip()
        if raw.isdigit():
            n = int(raw)
            if n == 0:
                return None
            if 1 <= n <= len(itens):
                return itens[n-1]
        print("❌ Entrada inválida. Informe um número listado.")

# ===============================================================
# UI: cores ANSI simples (funcionam no PowerShell moderno)
# ===============================================================

def _ansi(s, code):
    try:
        return f"\033[{code}m{s}\033[0m"
    except:
        return s

def _title(s):   return _ansi(s, "1;36")   # bold + ciano
def _ok(s):      return _ansi(s, "1;32")   # bold + verde
def _warn(s):    return _ansi(s, "1;33")   # bold + amarelo
def _err(s):     return _ansi(s, "1;31")   # bold + vermelho
def _muted(s):   return _ansi(s, "2;37")   # cinza

# ===============================================================
# Fluxos de ação do menu
# ===============================================================
def executar_retificacao():
    """
    Retificação de dados pendentes:
      - [1] Retificar uma especialidade
      (novas opções poderão ser adicionadas aqui)
    """
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        print(_title("┌──────────────────────────────────────────────┐"))
        print(_title("│ Retificação de dados pendentes              │"))
        print(_title("└──────────────────────────────────────────────┘"))
        print("Escolha uma ação:\n")
        print("  [1] Retificar uma especialidade")
        print("  [0] Voltar\n")
        esc = _ask("→ Sua escolha: ").strip()
        if esc == "0":
            return
        if esc == "1":
            retificar_uma_especialidade()
        else:
            print(_warn("Opção inválida."))
            _pause()

def executar_processamento():
    """Opção 2: Processa planilhas novas com todo o pipeline já existente."""
    print(_title("\n▶ Processamento de planilhas novas"))
    df_dados, arquivos_lidos, linhas_invalidas, consultorios_extraidos, erros_consultorios = ler_planilhas_ambulatorio()

    if df_dados.empty:
        print(_warn("Nenhum dado válido encontrado nas planilhas."))
    else:
        df_base = carregar_base_existente()
        df_para_inserir = remover_duplicatas(df_dados, df_base)
        inserir_novos_dados(df_para_inserir)

    registrar_erros_ambulatorio(linhas_invalidas)
    processar_log_de_erros()
    atualizar_aba_controle()

    if arquivos_lidos:
        mover_arquivos_processados(arquivos_lidos)

    # Inserção de consultórios (aba db_ambulatorio2)
    if consultorios_extraidos:
        df_cons = pd.DataFrame(consultorios_extraidos)
        if not df_cons.empty:
            inserir_consultorios(df_cons)
            # Erros de consultórios
            if erros_consultorios:
                registrar_erros_consultorios(erros_consultorios)

    print(_ok("✔ Processamento concluído.\n"))
    input(_muted("Pressione Enter para voltar ao menu... "))

def executar_edicao_parametrizacoes():
    """Opção 3: Abre o wizard para editar listas/configurações persistidas no JSON."""
    print(_title("\n▶ Edição de parametrizações (listas)"))
    wizard_editar_config_interativo()
    print(_ok("✔ Parametrizações atualizadas.\n"))
    input(_muted("Pressione Enter para voltar ao menu... "))

def wizard_renomear_na_base():
    """
    Opção 4: Renomeia valores/nomenclaturas diretamente na base (aba db_ambulatorio).
    Permite escolher a coluna e fazer find→replace com confirmação.
    """
    print(_title("\n▶ Alterar valores/nomenclaturas na base (db_ambulatorio)"))

    # Carrega base
    try:
        df = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA, engine="openpyxl")
    except FileNotFoundError:
        print(_err("Base não encontrada. Execute um processamento primeiro para criar a base."))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return
    except Exception as e:
        print(_err(f"Erro ao ler a base: {e}"))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return

    cols = list(df.columns)
    print("Colunas disponíveis:")
    for i, c in enumerate(cols, start=1):
        print(f"  [{i}] {c}")

    # Escolhe coluna
    while True:
        try:
            idx = int(input("\nInforme o número da coluna onde deseja renomear (ex.: 4): ").strip())
            if 1 <= idx <= len(cols):
                coluna = cols[idx-1]
                break
        except:
            pass
        print(_warn("Entrada inválida."))

    termo_de = input(f"Valor atual a localizar em '{coluna}': ").strip()
    if not termo_de:
        print(_warn("Operação cancelada (valor de origem vazio)."))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return

    termo_para = input(f"Novo valor que substituirá '{termo_de}': ").strip()
    if not termo_para:
        print(_warn("Operação cancelada (valor de destino vazio)."))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return

    # Prévia
    mask = df[coluna].astype(str) == termo_de
    qtd = int(mask.sum())
    if qtd == 0:
        print(_warn(f"Nenhuma ocorrência de '{termo_de}' encontrada na coluna '{coluna}'."))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return

    print(_muted(f"\nPrévia: {qtd} linha(s) serão alteradas em '{coluna}'."))
    confirma = input("Confirmar renomeação? [S/N]: ").strip().lower()
    if confirma != "s":
        print(_warn("Operação cancelada pelo usuário."))
        input(_muted("Pressione Enter para voltar ao menu... "))
        return

    # Aplica e salva
    df.loc[mask, coluna] = termo_para
    try:
        with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            df.to_excel(w, sheet_name=NOME_ABA, index=False)
        print(_ok(f"✔ Renomeadas {qtd} ocorrência(s) em '{coluna}'."))
    except Exception as e:
        print(_err(f"Erro ao salvar alterações: {e}"))

    input(_muted("Pressione Enter para voltar ao menu... "))

def menu_principal():
    """
    Mostra o menu inicial com experiência visual aprimorada.
    Apenas a opção 2 está ativa no momento.
    """
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        print(
            _title("┌───────────────────────────────────────────────────────────────┐\n") +
            _title("│  Processador de Produção Hospitalar – Menu Principal          │\n") +
            _title("└───────────────────────────────────────────────────────────────┘")
        )
        print("👋 Seja bem-vinda(o) ao programa de processamento de dados de Produção Hospitalar.")
        print("Escolha uma ação:\n")
        print("  [1] Retificar dados pendentes (em breve)")
        print("  [2] Processar planilhas de 'A serem processadas'")
        print("  [3] Alterar parametrizações (em breve)")
        print("  [4] Alterar valores/nomenclaturas na base de dados (em breve)")
        print("  [0] Sair\n")

        escolha = input("👉  Digite o número da opção e pressione Enter: ").strip()

        # Entrada inválida (vazio, letra, símbolo, etc.)
        if not escolha.isdigit():
            print(_err("\n❌ Funcionalidade não encontrada."))
            input(_muted("Pressione Enter para voltar ao menu... "))
            continue

        # Retificação
        if escolha == "1":
            executar_retificacao()
            input(_muted("\nPressione Enter para voltar ao menu... "))
            continue

        # Processamento
        if escolha == "2":
            executar_processamento()
            input(_muted("\nPressione Enter para voltar ao menu... "))
            continue

        # Sair
        if escolha == "0":
            print(_muted("\nEncerrando. Até breve!"))
            break

        # Demais opções ainda não implementadas
        if escolha in {"1", "3", "4"}:
            print(_warn("\n🚧 Essa funcionalidade ainda não está disponível."))
            input(_muted("Pressione Enter para voltar ao menu... "))
            continue

        # Qualquer número fora das opções
        print(_err("\n❌ Funcionalidade não encontrada."))
        input(_muted("Pressione Enter para voltar ao menu... "))

# ===============================================================
# FUNÇÕES DE EXTRAÇÃO E REGISTRO DE CONSULTÓRIOS (aba db_ambulatorio2)
# ===============================================================

def registrar_erros_consultorios(erros_consultorios):
    """
    Registra erros de consultórios **apenas** no arquivo de logs central:
      Controle/Log de Erros.xlsx → aba: 'consultorios_log'
    """
    if not erros_consultorios:
        return

    print(f"📝 Registrando {len(erros_consultorios)} erros em 'consultorios_log' (arquivo de LOG).")

    os.makedirs(CONTROLE_DIR, exist_ok=True)

    sheet = "consultorios_log"
    try:
        df_exist = pd.read_excel(CAMINHO_LOGS, sheet_name=sheet, engine="openpyxl")
    except Exception:
        df_exist = pd.DataFrame()

    df_new = pd.DataFrame(erros_consultorios)
    df_out = pd.concat([df_exist, df_new], ignore_index=True)

    with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl",
                        mode=("a" if os.path.exists(CAMINHO_LOGS) else "w"),
                        if_sheet_exists=("replace" if os.path.exists(CAMINHO_LOGS) else None)) as w:
        df_out.to_excel(w, sheet_name=sheet, index=False)

    print("✅ Log de consultórios atualizado em 'Log de Erros.xlsx'.")

def inserir_consultorios(df_consultorios):
    """
    Insere dados válidos de consultórios na aba 'db_ambulatorio2'
    """
    if df_consultorios.empty:
        print("✅ Nenhum dado de consultórios para inserir.")
        return

    print(f"🟢 Inserindo {len(df_consultorios)} registros em '{NOME_ABA_2}'.")

    try:
        df_antigo = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA_2, engine="openpyxl")
        df_antigo.columns = [col.strip().lower() for col in df_antigo.columns]
    except:
        df_antigo = pd.DataFrame(columns=["cnes", "competencia", "qtd_consultorios_disponiveis"])

    df_total = pd.concat([df_antigo, df_consultorios], ignore_index=True)

    # Remover duplicatas
    df_total.drop_duplicates(subset=["cnes", "competencia"], inplace=True)

    with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_total.to_excel(writer, sheet_name=NOME_ABA_2, index=False)

    print("✅ 'db_ambulatorio2' atualizado com sucesso.")

def extrair_consultorios(df):
    """
    Procura na coluna F (índice 5) a string 'quantitativo de consultorios'
    e retorna o valor numérico que estiver na célula abaixo.
    """
    from fuzzywuzzy import fuzz

    col_f = df.iloc[:, 5].astype(str).str.strip().str.lower()

    for i, valor in enumerate(col_f):
        if fuzz.partial_ratio(valor, "quantitativo de consultorios") >= 85:
            try:
                valor_baixo = df.iloc[i + 1, 5]
                if pd.notna(valor_baixo):
                    valor_baixo = str(valor_baixo).strip()
                    numero = int(re.findall(r"\d+", valor_baixo)[0])
                    return numero
            except:
                continue
    return None

# ===============================================================
# PROCESSAMENTO DE LOG DE ERROS (especialidades não reconhecidas)
# ===============================================================

def processar_log_de_erros():
    """
    Reanalisa o log central de erros (Controle/Log de Erros.xlsx → 'ambulatorio_log'),
    tenta corrigir linhas com substituições manuais e insere na base principal.
    """
    try:
        df_log = pd.read_excel(CAMINHO_LOGS, sheet_name="ambulatorio_log", engine="openpyxl")
        if df_log.empty:
            print("✅ Log de erros está vazio.")
            return
    except Exception:
        print("ℹ️ Nenhum log encontrado em Controle/Log de Erros.xlsx (aba 'ambulatorio_log').")
        return

    df_log["especialidade_original"] = df_log["especialidade_original"].astype(str).str.strip().str.upper()

    # Normaliza o dicionário para uppercase
    substituicoes_upper = {k.upper(): v for k, v in substituicoes_especialidades.items()}

    corrigidos = []
    ainda_invalidos = []

    for _, row in df_log.iterrows():
        especialidade = row["especialidade_original"]
        substituida = substituicoes_upper.get(especialidade)

        if substituida:
            print(f"🛠 Corrigindo linha do log: {especialidade} → {substituida}")
            corrigidos.append({
                "cnes": str(row["cnes"]).strip(),
                "competencia": str(row["competencia"]).strip(),
                "especialidade_original": str(row["especialidade_original"]).strip(),
                "especialidade": substituida.strip(),
                "quantitativo de atendimentos": int(row["quantitativo"])
            })
        else:
            ainda_invalidos.append(row)

    df_para_inserir = pd.DataFrame()

    if corrigidos:
        df_corrigido = pd.DataFrame(corrigidos)
        df_corrigido.columns = [col.strip().lower() for col in df_corrigido.columns]

        df_base = carregar_base_existente()
        df_para_inserir = remover_duplicatas(df_corrigido, df_base)
        inserir_novos_dados(df_para_inserir)

        print(f"✅ Log atualizado. {len(corrigidos)} linhas corrigidas, {len(df_para_inserir)} inseridas na base.")
    else:
        print("ℹ️ Nenhuma linha foi corrigida a partir do log.")

    # ✅ Atualiza o log (ainda inválidos continuam no LOG CENTRAL)
    df_novo_log = pd.DataFrame(ainda_invalidos)
    with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_novo_log.to_excel(writer, sheet_name="ambulatorio_log", index=False)

    print(f"✅ Log atualizado. {len(corrigidos)} linhas corrigidas, {len(df_para_inserir)} inseridas na base.")

def _retificar_alterar_motivo(esp_escolhida: str):
    """
    Atualiza o 'motivo' de TODAS as ocorrências dessa especialidade no LOG.
    """
    try:
        df_log = pd.read_excel(CAMINHO_LOGS, sheet_name="ambulatorio_log", engine="openpyxl")
    except Exception:
        print(_warn("Log não encontrado ou sem aba 'ambulatorio_log'."))
        return

    if df_log.empty:
        print(_warn("Log vazio."))
        return

    motivo_novo = _input_motivo_padrao()

    mask = df_log["especialidade_original"].astype(str).str.strip() == esp_escolhida
    n = int(mask.sum())
    if n == 0:
        print(_warn("Nenhuma ocorrência encontrada para essa especialidade no log."))
        return

    print(_muted(f"{n} ocorrência(s) serão atualizadas com o novo motivo: {motivo_novo}"))
    conf = _ask("Confirmar? [S/N]: ").lower()
    if conf != "s":
        print(_warn("Operação cancelada."))
        return

    df_log.loc[mask, "motivo"] = motivo_novo

    with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df_log.to_excel(w, sheet_name="ambulatorio_log", index=False)

    print(_ok(f"✔ Motivo atualizado em {n} ocorrência(s)."))


def _retificar_alterar_especialidade_e_inserir(esp_escolhida: str):
    """
    Move as ocorrências dessa especialidade do LOG para a base,
    alterando a especialidade (padronizada), deletando somente essas
    linhas do LOG, registrando em 'Mudanças e Registros' e
    atualizando 'Ambulatorial – Qualificação'.
    """
    try:
        df_log = pd.read_excel(CAMINHO_LOGS, sheet_name="ambulatorio_log", engine="openpyxl")
    except Exception:
        print(_warn("Log não encontrado ou sem aba 'ambulatorio_log'."))
        return
    if df_log.empty:
        print(_warn("Log vazio."))
        return

    mask = df_log["especialidade_original"].astype(str).str.strip() == esp_escolhida
    sub = df_log[mask].copy()
    if sub.empty:
        print(_warn("Nenhuma ocorrência encontrada para essa especialidade no log."))
        return

    # Escolher destino (com confirmação visual)
    print(_title("\n▶ Retificar especialidade"))
    print(f"Especialidade original: {esp_escolhida}")
    print("\nComo deseja modificar?")
    print("  [a] Digitar a especialidade padronizada")
    print("  [b] Buscar na lista de especialidades")
    while True:
        subop = _ask("Escolha (a/b): ").lower()
        if subop in ("a", "b"):
            break
        print("Opção inválida.")

    if subop == "a":
        destino = _ask("Digite a especialidade padronizada: ").strip()
        while not destino:
            destino = _ask("Valor vazio. Digite a especialidade padronizada: ").strip()
    else:
        idx = _lista_paginada(lista_especialidades_ambulatorio, "Especialidades – escolha um destino", por_pagina=50)
        if idx == -1:
            destino = _ask("Digite a especialidade padronizada: ").strip()
            while not destino:
                destino = _ask("Valor vazio. Digite a especialidade padronizada: ").strip()
        else:
            destino = lista_especialidades_ambulatorio[idx]

    acao = _confirmar_mapeamento(esp_escolhida, destino)  # 'c', 'v', 'l', 'q'
    if acao in ("q", "l"):
        print(_warn("Operação cancelada (Q/L)."))
        return
    if acao == "v":
        print(_warn("Voltando à seleção — reinicie a opção."))
        return

    # Monta dataframe para inserir na base
    sub["cnes"] = sub["cnes"].astype(str).str.strip()
    sub["competencia"] = sub["competencia"].astype(str).str.strip()
    sub["quantitativo"] = sub["quantitativo"].astype(int)

    df_ins = pd.DataFrame({
        "cnes": sub["cnes"],
        "competencia": sub["competencia"],
        "especialidade_original": sub["especialidade_original"].astype(str),
        "especialidade": destino,
        "quantitativo de atendimentos": sub["quantitativo"].astype(int)
    })

    # Inserir na base (evita duplicatas)
    df_exist = carregar_base_existente()
    df_novos = remover_duplicatas(df_ins.copy(), df_exist)
    inserir_novos_dados(df_novos)

    # Registrar em Mudanças e Registros (uma linha por ocorrência)
    for _, r in sub.iterrows():
        cnes = str(r["cnes"]).strip()
        nome_hosp = _nome_oficial_por_cnes(cnes) or ""
        registrar_mudancas_e_registros({
            "data_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "arquivo": r.get("arquivo", ""),
            "cnes": cnes,
            "nome_hospital": nome_hosp,
            "competencia": str(r["competencia"]).strip(),
            "especialidade_original": str(r["especialidade_original"]),
            "especialidade_final": destino,
            "resolucao": "Mudança por retificação"
        })

    # Atualizar Qualificação de Dados (aba Ambulatorial – Qualificação)
    # Deltas: saem do LOG (linhas_logs/soma_logs) e entram na BASE (linhas_base/soma_base)
    ajustes = []
    grp = sub.groupby(["arquivo","competencia","cnes"], dropna=False, as_index=False).agg(
        linhas=("quantitativo","size"),
        soma=("quantitativo","sum")
    )
    for _, g in grp.iterrows():
        cnes = str(g["cnes"]).strip()
        ajustes.append({
            "arquivo": g["arquivo"],
            "competencia": g["competencia"],
            "cnes": cnes,
            "nome_hospital": _nome_oficial_por_cnes(cnes) or "",
            "delta_linhas_logs": -int(g["linhas"]),
            "delta_linhas_base": +int(g["linhas"]),
            "delta_soma_logs": -int(g["soma"]),
            "delta_soma_base": +int(g["soma"]),
        })
    if ajustes:
        _atualizar_qualificacao_por_retificacao(ajustes)

    # Remover APENAS essas ocorrências do LOG
    df_log_restante = df_log[~mask].copy()
    with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df_log_restante.to_excel(w, sheet_name="ambulatorio_log", index=False)

    print(_ok(f"✔ Retificação concluída: {len(sub)} ocorrência(s) movidas para a base e removidas do log."))

def retificar_uma_especialidade():
    """Fluxo: lista únicas do LOG → escolhe 1 → [Alterar motivo] ou [Alterar especialidade e inserir]."""
    esp_unicas = _listar_especialidades_log_unicas()
    if not esp_unicas:
        print(_warn("Nenhuma especialidade pendente no log."))
        _pause()
        return

    print(_title("\n▶ Retificar uma especialidade"))
    escolha = _escolher_da_lista_numerada("Escolha a especialidade para retificar:", esp_unicas)
    if escolha is None:
        print(_warn("Operação cancelada (entrada manual não suportada neste fluxo)."))
        _pause()
        return

    print(f"\nEspecialidade selecionada: {_ansi(escolha, '1;36')}")
    print("O que deseja fazer?")
    print("  [1] Alterar motivo (apenas atualiza a coluna 'motivo' no log)")
    print("  [2] Alterar especialidade e inserir na base (remove do log)")

    while True:
        op = _ask("Escolha 1 ou 2: ").strip()
        if op in ("1","2"):
            break
        print("Opção inválida.")

    if op == "1":
        _retificar_alterar_motivo(escolha)
    else:
        _retificar_alterar_especialidade_e_inserir(escolha)

    _pause()

# ===============================================================
# EXTRAÇÃO E PADRONIZAÇÃO DE INFORMAÇÕES
# ===============================================================

def extrair_nome_hospital_e_competencia(nome_arquivo):
    """
    Extrai o nome do hospital e a competência (mês/ano) a partir do nome do arquivo.
    Exemplo: "HM_JABAQUARA-01_2025.xlsx" → "HM JABAQUARA", "2025-01"
    """
    match = re.search(r"-?(\d{2})[_-](\d{4})", nome_arquivo)
    if match:
        competencia = f"{match.group(2)}-{match.group(1)}"
        nome = nome_arquivo.split("-")[0].replace("_", " ").strip()
        return nome.upper(), competencia
    return None, None

def resolver_especialidade_nao_reconhecida(especialidade_original: str, cnes: str):
    """
    Mostra um menu visual e:
      1) Envia ao log com um MOTIVO padronizado (ou “outros” com input), OU
      2) Permite MODIFICAR e inserir na base:
         a) digitando a especialidade, OU
         b) escolhendo da lista de especialidades (com paginação).
    Agora inclui uma ETAPA DE CONFIRMAÇÃO visual (original → destino) e opção de voltar.
    Também memoriza a decisão para (CNES, termo) via _set_decisao SOMENTE após confirmar.
    Retorna o destino (string) se for para base, ou None se for para log/cancelar.
    Define ULTIMA_RESOLUCAO_TEXTO / ULTIMO_MOTIVO_ERRO.
    """
    global ULTIMO_MOTIVO_ERRO, ULTIMA_RESOLUCAO_TEXTO

    termo = str(especialidade_original).strip()
    termo_up = termo.upper()

    # Decisão memorizada?
    dec = _get_decisao(cnes, termo_up)
    if dec:
        if dec["acao"] == "M" and dec.get("destino"):
            ULTIMA_RESOLUCAO_TEXTO = dec.get("motivo") or ""
            return dec["destino"]
        else:
            ULTIMO_MOTIVO_ERRO = dec.get("motivo") or "Especialidade não reconhecida (decisão memorizada)"
            return None

    # Menu principal (Log x Modificar)
    print()
    print(_hdr("Especialidade não encontrada"))
    print(f"Entrada: {termo}")
    print("\nO que deseja fazer?\n")
    print("  [1] Enviar para o LOG")
    print("  [2] Modificar e inserir na base\n")

    while True:
        escolha = _ask("Digite 1 ou 2: ")
        if escolha in ("1", "2"):
            break
        print("Opção inválida.")

    # ===== Caminho: LOG =====
    if escolha == "1":
        print("\nMotivo do LOG:")
        print("  [a] Perguntar para Sara")
        print("  [b] Rever nos registros anteriores")
        print("  [c] Pedir ao hospital para retificar")
        print("  [d] Outros")
        while True:
            m = _ask("Escolha (a/b/c/d): ").lower()
            if m in ("a", "b", "c", "d"):
                break
            print("Opção inválida.")
        if m == "a":
            motivo = "Perguntar para Sara"
        elif m == "b":
            motivo = "Rever nos registros anteriores"
        elif m == "c":
            motivo = "Solicitar retificação ao hospital"
        else:
            motivo = _ask("Digite o motivo: ") or "Outros (sem detalhamento)"

        ULTIMO_MOTIVO_ERRO = motivo
        ULTIMA_RESOLUCAO_TEXTO = None
        _set_decisao(cnes, termo_up, "L", None, motivo)  # memoriza
        return None

    # ===== Caminho: MODIFICAR =====
    # Loop para permitir VOLTAR após visualizar a confirmação
    while True:
        print("\nComo deseja modificar?")
        print("  [a] Digitar a especialidade padronizada")
        print("  [b] Buscar na lista de especialidades")
        while True:
            sub = _ask("Escolha (a/b): ").lower()
            if sub in ("a", "b"):
                break
            print("Opção inválida.")

        if sub == "a":
            destino = _ask("Digite a especialidade padronizada: ").strip()
            while not destino:
                destino = _ask("Valor vazio. Digite a especialidade padronizada: ").strip()
        else:
            idx = _lista_paginada(lista_especialidades_ambulatorio, "Especialidades – escolha um destino", por_pagina=50)
            if idx == -1:
                destino = _ask("Digite a especialidade padronizada: ").strip()
                while not destino:
                    destino = _ask("Valor vazio. Digite a especialidade padronizada: ").strip()
            else:
                destino = lista_especialidades_ambulatorio[idx]

        # ===== NOVA ETAPA: CONFIRMAÇÃO VISUAL =====
        acao = _confirmar_mapeamento(termo, destino)  # 'c', 'v', 'l', 'q'
        if acao == "v":
            # volta para escolher novamente (reinicia o loop)
            continue
        if acao == "l":
            # envia para LOG (mesmo fluxo do caminho 1)
            print("\nMotivo do LOG:")
            print("  [a] Perguntar para Sara")
            print("  [b] Rever nos registros anteriores")
            print("  [c] Pedir ao hospital para retificar")
            print("  [d] Outros")
            while True:
                m = _ask("Escolha (a/b/c/d): ").lower()
                if m in ("a", "b", "c", "d"):
                    break
                print("Opção inválida.")
            if m == "a":
                motivo = "Perguntar para Sara"
            elif m == "b":
                motivo = "Rever nos registros anteriores"
            elif m == "c":
                motivo = "Solicitar retificação ao hospital"
            else:
                motivo = _ask("Digite o motivo: ") or "Outros (sem detalhamento)"

            ULTIMO_MOTIVO_ERRO = motivo
            ULTIMA_RESOLUCAO_TEXTO = None
            _set_decisao(cnes, termo_up, "L", None, motivo)  # memoriza
            return None
        if acao == "q":
            # cancela sem ação
            ULTIMO_MOTIVO_ERRO = "Operação cancelada pelo usuário"
            ULTIMA_RESOLUCAO_TEXTO = None
            return None

        # ===== Confirmado ('c'): perguntar a RESOLUÇÃO e então gravar =====
        print("\nQual foi a resolução?")
        print("  [a] Especialidade não médica que agora é mapeada")
        print("  [b] Erro gramatical")
        print("  [c] Nomenclatura diferente com mesmo sentido")
        print("  [d] Sinalizou setor e não especialidade; ajustado")
        print("  [e] Ainda não registrado")
        print("  [f] Nomenclatura mais detalhada que o necessário")
        print("  [g] Outros")
        while True:
            r = _ask("Escolha (a/b/c/d/e/f/g): ").lower()
            if r in list("abcdefg"):
                break
            print("Opção inválida.")

        if   r == "a": resol = "Especialidade não médica – mapeada"
        elif r == "b": resol = "Erro gramatical"
        elif r == "c": resol = "Nomenclatura diferente com mesmo sentido"
        elif r == "d": resol = "Indicou setor; ajustado para especialidade"
        elif r == "e": resol = "Ainda não registrado"
        elif r == "f": resol = "Nomenclatura mais detalhada que o necessário"
        else:
            resol = _ask("Descreva a resolução: ") or "Outros (sem detalhamento)"

        # Só AQUI gravamos substituições e decisão — após confirmação!
        ULTIMA_RESOLUCAO_TEXTO = resol
        ULTIMO_MOTIVO_ERRO = None

        # guarda substituição global (em UPPER como chave de origem)
        substituicoes_especialidades[termo_up] = destino
        if destino not in lista_especialidades_ambulatorio:
            lista_especialidades_ambulatorio.append(destino)
        salvar_config()

        # memoriza decisão por hospital+termo
        _set_decisao(cnes, termo_up, "M", destino, resol)

        print(f"\n✔️ Mapeado '{termo}' → '{destino}'.")
        return destino

def padronizar_especialidade(especialidade_original, cnes: str):
    """
    1) Se houver decisão memorizada (cnes, termo), aplica.
    2) Senão, usa substituições globais e fuzzy (>= 90).
    3) Senão, pergunta (L/M) e memoriza.
    """
    global ULTIMO_MOTIVO_ERRO
    from fuzzywuzzy import process as fz

    termo = str(especialidade_original).strip()
    termo_up = termo.upper()

    # 1) Decisão memorizada por hospital
    dec = _get_decisao(cnes, termo_up)
    if dec:
        if dec["acao"] == "M" and dec.get("destino"):
            return dec["destino"]
        else:
            ULTIMO_MOTIVO_ERRO = dec.get("motivo") or "Especialidade não reconhecida (decisão memorizada)"
            return None

    # 2) Políticas
    if termo_up in termos_proibidos:
        print(f"🚫 Ignorada por política: '{termo}'")
        return None

    # 3) Substituição global
    subs_upper = {k.upper(): v for k, v in substituicoes_especialidades.items()}
    if termo_up in subs_upper:
        return subs_upper[termo_up]

    # 4) Fuzzy “seguro”
    if lista_especialidades_ambulatorio:
        melhor, score = fz.extractOne(termo, lista_especialidades_ambulatorio)
        if score >= 95:
            return melhor

    # 5) Perguntar (L/M) e memorizar
    return resolver_especialidade_nao_reconhecida(termo, cnes)

def buscar_cnes_por_nome(nome_hospital_bruto):
    """
    Busca o CNES pelo nome do hospital usando fuzzy matching.
    1) Tenta ler da planilha 'dHospitais.xlsx' (em Bases de Dados).
       - Preferência pela aba 'hospitais'; se não existir, usa a primeira aba.
       - Assume CNES na coluna A e Nome do hospital na coluna D (mesma convenção antiga).
    2) (fallback) Se falhar, tenta a aba 'hospitais' do dbProducao.xlsx (se existir).
    """
    def _tentar_ler_hospitais_xlsx(caminho):
        # Retorna DataFrame com colunas "cnes" e "nome_hospital" padronizadas
        xls = pd.ExcelFile(caminho, engine="openpyxl")
        sheet = "hospitais" if "hospitais" in [s.lower() for s in xls.sheet_names] else xls.sheet_names[0]
        df_raw = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        # Tenta pegar CNES (coluna A) e Nome (coluna D), como você já usava
        df = df_raw.iloc[:, [0, 3]].copy()
        df.columns = ["cnes", "nome_hospital"]
        df["cnes"] = df["cnes"].astype(str).str.strip()
        df["nome_hospital"] = df["nome_hospital"].astype(str).str.strip().str.upper()
        return df

    try:
        # 1) Tenta dHospitais.xlsx
        if os.path.exists(CAMINHO_DHOSPITAIS):
            df_hosp = _tentar_ler_hospitais_xlsx(CAMINHO_DHOSPITAIS)
            melhor_nome, score = process.extractOne(str(nome_hospital_bruto).strip().upper(), df_hosp["nome_hospital"].tolist())
            if score >= 90:
                cnes = df_hosp.loc[df_hosp["nome_hospital"] == melhor_nome, "cnes"].iloc[0]
                print(f"🏥 Hospital (dHospitais): '{melhor_nome}' → CNES: {cnes} (confiança: {score}%)")
                return cnes
            else:
                print(f"⚠️ dHospitais: '{nome_hospital_bruto}' abaixo do limiar (score={score}). Tentando fallback...")
        else:
            print(f"ℹ️ Arquivo dHospitais não encontrado em: {CAMINHO_DHOSPITAIS}")

        # 2) Fallback: tenta dbProducao.xlsx (se já existir)
        if os.path.exists(CAMINHO_BASE):
            df_hospitais = pd.read_excel(CAMINHO_BASE, sheet_name="hospitais", engine="openpyxl")
            nomes_candidatos = df_hospitais.iloc[:, 3].astype(str).str.upper().tolist()  # Coluna D
            melhor_nome, score = process.extractOne(str(nome_hospital_bruto).strip().upper(), nomes_candidatos)

            if score >= 90:
                linha = df_hospitais[df_hospitais.iloc[:, 3].astype(str).str.upper() == melhor_nome]
                cnes = str(linha.iloc[0, 0]).strip()  # Coluna A
                print(f"🏥 Hospital (dbProducao): '{melhor_nome}' → CNES: {cnes} (confiança: {score}%)")
                return cnes
            else:
                print(f"⚠️ dbProducao: '{nome_hospital_bruto}' abaixo do limiar (score={score}).")
        else:
            print(f"ℹ️ CAMINHO_BASE ainda não existe: {CAMINHO_BASE}")

        print(f"⚠️ Hospital '{nome_hospital_bruto}' não encontrado com confiança suficiente.")
        return None

    except Exception as e:
        print(f"❌ Erro ao buscar CNES: {e}")
        return None



# ===============================================================
# FUNÇÃO PRINCIPAL DE LEITURA DAS PLANILHAS NOVAS
# ===============================================================

def ler_planilhas_ambulatorio():
    """
    Lê todas as planilhas da pasta de entrada, extrai dados da aba 'Ambulatorio',
    faz a padronização e monta os DataFrames a serem inseridos.
    Também detecta erros e extrai informações de consultórios.
    """
    global ULTIMA_RESOLUCAO_TEXTO, ULTIMO_MOTIVO_ERRO
    erros_consultorios = []
    print("📁 Conteúdo da pasta:")
    print(os.listdir(CAMINHO_PLANILHAS))

    dados_para_inserir = []
    arquivos_lidos = []
    consultorios_extraidos = []

    arquivos = [
        arq for arq in os.listdir(CAMINHO_PLANILHAS)
        if not arq.startswith("~$")
    ]


    if not arquivos:
        print(f"⚠️ Nenhuma planilha encontrada para processar em: {CAMINHO_PLANILHAS}")
        # Retorna 5 valores: df_resultado, arquivos_lidos, linhas_invalidas, consultorios_extraidos, erros_consultorios
        return pd.DataFrame(), [], [], [], []

    
    linhas_invalidas = []

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(CAMINHO_PLANILHAS, arquivo)
        print(f"\n📄 Processando: {arquivo}")

        try:
            with pd.ExcelFile(caminho_arquivo) as xls:
                abas_disponiveis = xls.sheet_names
                abas_normalizadas = {normalizar(nome): nome for nome in abas_disponiveis}

                if "ambulatorio" not in abas_normalizadas:
                    print(f"⚠️ A planilha '{arquivo}' não possui a aba 'Ambulatorio' (ou variação).")

                    nome_hospital, _ = extrair_nome_hospital_e_competencia(arquivo)
                    cnes = buscar_cnes_por_nome(nome_hospital)

                    if cnes and cnes in cnes_sem_ambulatorio:
                        print(f"🔕 Hospital '{nome_hospital}' (CNES: {cnes}) não possui ambulatório. Movendo para 'Arquivadas'.")
                        arquivos_lidos.append(arquivo)  # mover para Arquivadas normalmente
                    else:
                        print(f"⚠️ CNES '{cnes}' não está na lista de exceções. Planilha ignorada.")

                    continue


                aba_certa = abas_normalizadas["ambulatorio"]
                df = pd.read_excel(xls, sheet_name=aba_certa)

            # --- contadores por arquivo ---
            linhas_raw = 0
            soma_raw = 0
            linhas_base = 0
            soma_base = 0
            linhas_erros = 0
            soma_erros = 0
            competencias_vistas = set()

            tem_coluna_mes = any(col.lower().strip() == "mês referente" for col in df.columns)
            usar_mes_referente = False

            if tem_coluna_mes:
                colunas_mes = [col for col in df.columns if col.lower().strip() == "mês referente"]
                coluna_mes_nome = colunas_mes[0]
                meses_unicos = df[coluna_mes_nome].dropna().unique()

                if len(meses_unicos) > 1:
                    usar_mes_referente = True


            nome_hospital, competencia_padrao = extrair_nome_hospital_e_competencia(arquivo)
            cnes = buscar_cnes_por_nome(nome_hospital)
            if not cnes:
                print(f"⚠️ CNES não encontrado para '{nome_hospital}'. Planilha ignorada.")
                continue





            for _, row in df.iterrows():
                especialidade_bruta = row.iloc[2]
                quantitativo = row.iloc[3]

                # ⚠️ Aviso se quantitativo for 0 ou ausente
                if pd.isna(quantitativo) or quantitativo == 0:
                    print(f"⚠️ Quantitativo zero ou ausente ignorado | Arquivo: {arquivo} | Especialidade: {especialidade_bruta}")
                    continue

                # 📅 Definindo competência baseada na coluna "Mês Referente" (se aplicável)
                competencia = competencia_padrao
                if usar_mes_referente:
                    try:
                        mes_raw = str(row[coluna_mes_nome])
                        if "," in mes_raw:
                            nome_mes, ano = [s.strip() for s in mes_raw.split(",")]
                            mes_num = mes_para_numero(nome_mes)
                            if mes_num:
                                competencia = f"{ano}-{mes_num}"
                    except Exception as e:
                        print(f"⚠️ Erro ao interpretar mês referente na linha: {e}")

                # Ignora linha com especialidade "TOTAL"
                if str(especialidade_bruta).strip().upper() == "TOTAL":
                    print(f"⚠️ Linha ignorada por conter especialidade TOTAL | Arquivo: {arquivo}")
                    continue

                # Contabiliza RAW (linha candidata) antes de padronizar
                linhas_raw += 1
                try:
                    soma_raw += int(quantitativo)
                except Exception:
                    pass
                

                # Padronização / decisão L ou M
                especialidade_corrigida = padronizar_especialidade(especialidade_bruta, cnes)


                if not especialidade_corrigida:
                    # Usa o motivo digitado (se houver), senão mantém o padrão
                    motivo = (ULTIMO_MOTIVO_ERRO or "Especialidade não reconhecida pelo fuzzy")
                    ULTIMO_MOTIVO_ERRO = None  # limpa a flag

                    linhas_invalidas.append({
                        "arquivo": arquivo,
                        "cnes": cnes,
                        "competencia": competencia,
                        "especialidade_original": especialidade_bruta,
                        "quantitativo": quantitativo,
                        "motivo": motivo
                    })
                    # contadores de erro
                    linhas_erros += 1
                    try:
                        soma_erros += int(quantitativo)
                    except Exception:
                        pass

                    continue

                # Linha válida: guarda original e final
                dados_para_inserir.append({
                    "cnes": cnes,
                    "competencia": competencia,
                    "especialidade_original": str(especialidade_bruta),
                    "especialidade": especialidade_corrigida,  # final
                    "quantitativo de atendimentos": int(quantitativo)
                })

                # contadores de base
                linhas_base += 1
                try:
                    soma_base += int(quantitativo)
                except Exception:
                    pass

                # marca competencia vista
                competencias_vistas.add(competencia)

                
                # Se houve correção manual agora, grava **uma vez** no arquivo unificado de mudanças/registros
                if ULTIMA_RESOLUCAO_TEXTO:
                    registrar_mudancas_e_registros({
                        "data_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "arquivo": arquivo,
                        "cnes": cnes,
                        "nome_hospital": nome_hospital,
                        "competencia": competencia,
                        "especialidade_original": str(especialidade_bruta),
                        "especialidade_final": especialidade_corrigida,
                        "resolucao": ULTIMA_RESOLUCAO_TEXTO
                    })
                    ULTIMA_RESOLUCAO_TEXTO = None

            # === RESUMO POR ARQUIVO ===
            if competencias_vistas:
                competencia_resumo = ";".join(sorted(competencias_vistas))
            else:
                competencia_resumo = competencia_padrao  # fallback

            diff_linhas = linhas_raw - (linhas_base + linhas_erros)
            diff_soma = soma_raw - (soma_base + soma_erros)

            registrar_controle_resumo({
                "arquivo": arquivo,
                "cnes": cnes,
                "nome_hospital": nome_hospital,
                "competencia": competencia_resumo,
                "n° de linhas raw": linhas_raw,
                "n° de linhas base": linhas_base,
                "n° de linhas erros": linhas_erros,
                "diferença linhas": diff_linhas,
                "soma dos dados raw": soma_raw,
                "soma dos dados base": soma_base,
                "soma dos dados erros": soma_erros,
                "diferença soma": diff_soma
            })
            
            status = "OK"
            if linhas_erros > 0 and linhas_base > 0:
                status = "Com Erros"
            elif linhas_base == 0 and linhas_erros > 0:
                status = "Somente Erros"
            elif linhas_base == 0 and linhas_erros == 0:
                status = "Sem Dados"

            
            # 👉 Atualiza também a grade em colunas por competência (formato do print)
            try:
                atualizar_controle_atualizacao_grade(
                    cnes=cnes,
                    nome_hospital=nome_hospital,
                    competencias=sorted(list(competencias_vistas)) if competencias_vistas else [competencia_padrao]
                )
            except Exception as e:
                print(f"❌ Erro ao atualizar grade de controle: {e}")


            # 👇 estes blocos rodam UMA VEZ por arquivo (fora do for)
            qtd_consultorios = extrair_consultorios(df)
            if qtd_consultorios is not None:
                consultorios_extraidos.append({
                    "cnes": cnes,
                    "competencia": competencia_padrao,  # ou 'competencia' se quiser refletir o último
                    "qtd_consultorios_disponiveis": qtd_consultorios
                })
                print(f"🏥 Consultórios detectados: {qtd_consultorios}")
            else:
                erros_consultorios.append({
                    "arquivo": arquivo,
                    "cnes": cnes,
                    "competencia": competencia_padrao,
                    "motivo": "Consultórios não detectados"
                })
                print("❔ Quantitativo de consultórios não identificado nesta planilha.")

            arquivos_lidos.append(arquivo)



        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")

    df_resultado = pd.DataFrame(dados_para_inserir)
    df_resultado.columns = [col.strip().lower() for col in df_resultado.columns]

    return df_resultado, arquivos_lidos, linhas_invalidas, consultorios_extraidos, erros_consultorios

# ===============================================================
# INSERÇÃO DE DADOS AMBULATORIAIS NA BASE PRINCIPAL
# ===============================================================

def carregar_base_existente():
    """
    Carrega os dados existentes da aba db_ambulatorio.
    """
    try:
        df_existente = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA, engine="openpyxl")
        df_existente.columns = [col.strip().lower() for col in df_existente.columns]
    except Exception:
        print("⚠️ Arquivo base não encontrado. Criando nova base.")
        df_existente = pd.DataFrame(columns=[
            "cnes", "competencia",
            "especialidade_original",   # nova
            "especialidade",            # final
            "quantitativo de atendimentos"
        ])
    return df_existente

def remover_duplicatas(df_novo, df_existente):
    """
    Evita duplicação de linhas já existentes, comparando por CNES + competência + especialidade + quantitativo.
    Mostra no terminal as duplicatas identificadas e removidas.
    """
    chave = ["cnes", "competencia", "especialidade", "quantitativo de atendimentos"]

    # Normaliza os campos para comparação
    for col in chave:
        df_novo[col] = df_novo[col].astype(str).str.strip().str.lower()
        df_existente[col] = df_existente[col].astype(str).str.strip().str.lower()

    # Junta os novos com os existentes
    df_merged = df_novo.merge(df_existente[chave], on=chave, how="left", indicator=True)
    df_novos = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # Duplicatas removidas
    duplicatas = df_merged[df_merged["_merge"] == "both"]
    if not duplicatas.empty:
        print(f"🚫 {len(duplicatas)} linha(s) duplicada(s) foram identificadas e **removidas**:")
        for _, row in duplicatas.iterrows():
            print(f"   → {row['cnes']} | {row['competencia']} | {row['especialidade']} | {row['quantitativo de atendimentos']}")

    return df_novos

def inserir_novos_dados(df_novos):
    """
    Insere os novos dados limpos na aba db_ambulatorio, mantendo capitalização padronizada.
    """
    if df_novos.empty:
        print("✅ Nenhum novo dado para inserir.")
        return

    print(f"🟢 Inserindo {len(df_novos)} novas linhas na base '{NOME_ABA}'.")

    try:
        # Tenta carregar a base antiga
        df_antigo = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA, engine="openpyxl")
        df_antigo.columns = [col.strip().lower() for col in df_antigo.columns]
    except Exception:
        print("⚠️ Base existente não encontrada ou com erro. Criando nova.")
        df_antigo = pd.DataFrame(columns=[
            "cnes", "competencia",
            "especialidade_original",
            "especialidade",
            "quantitativo de atendimentos"
        ])


    # Junta os dados antigos com os novos
    df_novo_total = pd.concat([df_antigo, df_novos], ignore_index=True)

    # Normaliza capitalização das especialidades para visualização mais limpa
    df_novo_total["especialidade"] = df_novo_total["especialidade"].astype(str).str.title()

    # Salva no Excel preservando outras abas
    try:
        # Se o arquivo ainda não existir, cria com essa aba
        if not os.path.exists(CAMINHO_BASE):
            print("📁 Arquivo base não encontrado. Criando um novo com a aba db_ambulatorio.")
            with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl") as writer:
                df_novo_total.to_excel(writer, sheet_name=NOME_ABA, index=False)
        else:
            with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_novo_total.to_excel(writer, sheet_name=NOME_ABA, index=False)
    except Exception as e:
        print(f"❌ Erro ao salvar dados em '{NOME_ABA}': {e}")

    print("✅ Base atualizada com sucesso, mantendo demais abas intactas.")

# ===============================================================
# LOGS E REGISTROS DE ERROS
# ===============================================================

def registrar_erros_ambulatorio(linhas_invalidas):
    """
    Registra erros **apenas** no arquivo de logs central:
      Controle/Log de Erros.xlsx → aba: 'ambulatorio_log'
    (não escreve nada em dbProducao.xlsx)
    """
    if not linhas_invalidas:
        return

    print(f"📝 Registrando {len(linhas_invalidas)} erros em 'ambulatorio_log' (arquivo de LOG).")

    os.makedirs(CONTROLE_DIR, exist_ok=True)

    sheet = "ambulatorio_log"
    try:
        df_exist = pd.read_excel(CAMINHO_LOGS, sheet_name=sheet, engine="openpyxl")
    except Exception:
        df_exist = pd.DataFrame()

    df_new = pd.DataFrame(linhas_invalidas)
    df_out = pd.concat([df_exist, df_new], ignore_index=True)

    with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl",
                        mode=("a" if os.path.exists(CAMINHO_LOGS) else "w"),
                        if_sheet_exists=("replace" if os.path.exists(CAMINHO_LOGS) else None)) as w:
        df_out.to_excel(w, sheet_name=sheet, index=False)

    print("✅ Log de erros atualizado em 'Log de Erros.xlsx'.")

def registrar_mudancas_e_registros(registro: dict):
    """
    Grava todas as decisões manuais (mudanças + registros) em:
      Controle/Controle de Mudanças e Registros.xlsx → aba: 'Ambulatório'
    Usa superset de colunas; o que não vier no dict fica vazio.
    """
    try:
        os.makedirs(CONTROLE_DIR, exist_ok=True)

        try:
            df_exist = pd.read_excel(CAMINHO_CONTROLE_MUD_REG, sheet_name=ABA_MUD_REG, engine="openpyxl")
        except Exception:
            df_exist = pd.DataFrame()

        df_new = pd.DataFrame([registro])
        df_out = pd.concat([df_exist, df_new], ignore_index=True)

        with pd.ExcelWriter(CAMINHO_CONTROLE_MUD_REG, engine="openpyxl",
                            mode=("a" if os.path.exists(CAMINHO_CONTROLE_MUD_REG) else "w"),
                            if_sheet_exists=("replace" if os.path.exists(CAMINHO_CONTROLE_MUD_REG) else None)) as w:
            df_out.to_excel(w, sheet_name=ABA_MUD_REG, index=False)

        print(f"🗂️ Mudanças/Registros salvos em '{CAMINHO_CONTROLE_MUD_REG}' (aba '{ABA_MUD_REG}').")
    except Exception as e:
        print(f"❌ Erro ao salvar em 'Controle de Mudanças e Registros': {e}")

def registrar_controle_resumo(resumo: dict):
    """
    Grava Qualificação (linhas/somas + 2 status) em:
      Controle/Qualificação de Dados.xlsx → aba: 'Ambulatorio'
    """
    try:
        os.makedirs(CONTROLE_DIR, exist_ok=True)
        garantir_quali_dados()

        try:
            df_exist = pd.read_excel(CAMINHO_QUALI_DADOS, sheet_name=ABA_QUALI_AMB, engine="openpyxl")
        except Exception:
            df_exist = pd.DataFrame()

        raw_linhas  = int(resumo.get("n° de linhas raw", 0) or 0)
        base_linhas = int(resumo.get("n° de linhas base", 0) or 0)
        erro_linhas = int(resumo.get("n° de linhas erros", 0) or 0)

        raw_soma    = int(resumo.get("soma dos dados raw", 0) or 0)
        base_soma   = int(resumo.get("soma dos dados base", 0) or 0)
        erro_soma   = int(resumo.get("soma dos dados erros", 0) or 0)

        if erro_linhas > 0:
            status_linhas = "Pendente"
        elif raw_linhas == (base_linhas + erro_linhas):
            status_linhas = "OK"
        elif raw_linhas > (base_linhas + erro_linhas):
            status_linhas = "Falta linha"
        else:
            status_linhas = "Sobrando linha"

        status_soma = "OK" if raw_soma == (base_soma + erro_soma) else "Divergente"

        df_new = pd.DataFrame([{
            "Data_Registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "arquivo":       resumo.get("arquivo"),
            "cnes":          str(resumo.get("cnes", "")).strip(),
            "nome_hospital": resumo.get("nome_hospital"),
            "competencia":   resumo.get("competencia"),
            "linhas_raw":    raw_linhas,
            "linhas_base":   base_linhas,
            "linhas_logs":   erro_linhas,
            "status_linhas": status_linhas,
            "soma_raw":      raw_soma,
            "soma_base":     base_soma,
            "soma_logs":     erro_soma,
            "status_soma":   status_soma
        }])

        # Dedup (arquivo, competencia)
        if not df_exist.empty and all(c in df_exist.columns for c in ["arquivo","competencia"]):
            mask_dup = (df_exist["arquivo"] == df_new.iloc[0]["arquivo"]) & \
                       (df_exist["competencia"] == df_new.iloc[0]["competencia"])
            df_exist = df_exist[~mask_dup]

        df_out = pd.concat([df_exist, df_new], ignore_index=True)

        with pd.ExcelWriter(CAMINHO_QUALI_DADOS, engine="openpyxl",
                            mode="a", if_sheet_exists="replace") as w:
            df_out.to_excel(w, sheet_name=ABA_QUALI_AMB, index=False)

        print(f"🧪 Qualificação atualizada em '{CAMINHO_QUALI_DADOS}' (aba '{ABA_QUALI_AMB}').")
    except Exception as e:
        print(f"❌ Erro ao salvar Qualificação: {e}")


def mover_arquivos_processados(lista_arquivos):
    """
    Move planilhas processadas para a pasta de arquivamento
    """
    for nome in lista_arquivos:
        origem = os.path.join(CAMINHO_PLANILHAS, nome)
        destino = os.path.join(CAMINHO_ARQUIVADAS, nome)
        try:
            shutil.move(origem, destino)
            print(f"📦 Arquivo '{nome}' movido para 'Arquivadas'.")
        except Exception as e:
            print(f"❌ Erro ao mover '{nome}': {e}")

# ===============================================================
# CONTROLE DE ENVIO DE PLANILHAS (por hospital e mês)
# ===============================================================

def atualizar_aba_controle():
    """
    Cria ou atualiza a aba 'controle_ambulatorio' mostrando,
    para os últimos 6 meses, quais hospitais enviaram planilhas
    e quais ainda não enviaram.
    Agora lê os hospitais diretamente do arquivo dHospitais.xlsx.
    """
    try:
        # ✅ Carrega base de hospitais
        if not os.path.exists(CAMINHO_DHOSPITAIS):
            print(f"⚠️ Arquivo dHospitais.xlsx não encontrado em: {CAMINHO_DHOSPITAIS}")
            return

        xls = pd.ExcelFile(CAMINHO_DHOSPITAIS, engine="openpyxl")
        aba = "hospitais" if "hospitais" in [s.lower() for s in xls.sheet_names] else xls.sheet_names[0]
        df_hospitais = pd.read_excel(xls, sheet_name=aba, engine="openpyxl")

        # assume CNES na primeira coluna (A) e Nome do hospital na quarta (D)
        df_hospitais = df_hospitais.iloc[:, [0, 3]].copy()
        df_hospitais.columns = ["cnes", "nome_hospital"]
        df_hospitais["cnes"] = df_hospitais["cnes"].astype(str).str.strip()

        # ✅ Carrega base já processada (dbProducao.xlsx)
        df_base = pd.read_excel(CAMINHO_BASE, sheet_name=NOME_ABA, engine="openpyxl")
        df_base["cnes"] = df_base["cnes"].astype(str)

        # ✅ Gera lista dos últimos 6 meses (exclui mês atual)
        hoje = datetime.today().replace(day=1)
        meses = [(hoje - relativedelta(months=i)).strftime("%Y-%m") for i in range(1, 7)]
        meses.reverse()

        # ✅ Cria tabela de controle
        df_controle = df_hospitais.copy()
        for mes in meses:
            col_mes = []
            for cnes in df_controle["cnes"]:
                enviado = not df_base[
                    (df_base["cnes"] == cnes) &
                    (df_base["competencia"] == mes)
                ].empty
                col_mes.append("✅" if enviado else "❌")
            df_controle[mes] = col_mes

        # ✅ Salva na aba controle_ambulatorio
        DESTINO_ENVIO = CAMINHO_CONTROLE_ATUALIZACAO_GRADE  # \\...\\Produção Hospitalar\\Controle\\Controle de Atualização.xlsx
        NOME_ABA_ENVIO = "Ambulatorial – Envio (6 meses)"    # evita conflito com "Ambulatorial – Grade"

        with pd.ExcelWriter(
            DESTINO_ENVIO,
            engine="openpyxl",
            mode=("a" if os.path.exists(DESTINO_ENVIO) else "w"),
            if_sheet_exists=("replace" if os.path.exists(DESTINO_ENVIO) else None)
        ) as writer:
            df_controle.to_excel(writer, sheet_name=NOME_ABA_ENVIO, index=False)

        print(f"✅ Aba '{NOME_ABA_ENVIO}' atualizada em '{DESTINO_ENVIO}'.")

        print("✅ Aba 'controle_ambulatorio' atualizada com sucesso.")

    except Exception as e:
        print(f"❌ Erro ao atualizar controle: {e}")

# ===============================================================
# EXECUÇÃO PRINCIPAL DO SCRIPT
# ===============================================================

def executar_processamento():
    """
    Executa o pipeline padrão: lê planilhas da pasta 'A serem processadas',
    insere dados na base, registra erros/logs e atualiza controles.
    """
    # Lê todas as planilhas novas
    df_dados, arquivos_lidos, linhas_invalidas, consultorios_extraidos, erros_consultorios = ler_planilhas_ambulatorio()
    print("\n✅ Dados lidos e limpos:")
    print(df_dados if not df_dados.empty else "(vazio)")

    if df_dados.empty:
        print("ℹ️ Nenhum dado foi processado (nada a inserir).")
    else:
        # Carrega base existente e insere novos dados
        df_base = carregar_base_existente()
        df_para_inserir = remover_duplicatas(df_dados, df_base)
        inserir_novos_dados(df_para_inserir)

        # Registra erros e atualiza log
        registrar_erros_ambulatorio(linhas_invalidas)
        processar_log_de_erros()
        atualizar_aba_controle()

    # Mover arquivos mesmo se não geraram dados
    if arquivos_lidos:
        mover_arquivos_processados(arquivos_lidos)
    else:
        print("ℹ️ Nenhum arquivo foi marcado para arquivamento.")

    # Inserir dados de consultórios (db_ambulatorio2) e espelhar erros
    if consultorios_extraidos:
        df_consult = pd.DataFrame(consultorios_extraidos)
        if not df_consult.empty:
            inserir_consultorios(df_consult)
    if erros_consultorios:
        registrar_erros_consultorios(erros_consultorios)
    # Reconstrói a grade global (estilo Envio) após todo o processamento
    try:
        atualizar_controle_atualizacao_grade()
    except Exception as e:
        print(f"❌ Erro ao atualizar grade de controle: {e}")
    print("\n✅ Processamento concluído.\n")

# ===============================================================
# EXECUÇÃO PRINCIPAL DO SCRIPT
# ===============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processa planilhas de Ambulatório.")
    parser.add_argument("--config", type=str, default=CONFIG_PATH, help="Caminho do arquivo de configuração JSON.")
    args = parser.parse_args()

    # Ajusta caminho do config e carrega
    CONFIG_PATH = args.config
    carregar_config()

    # Inicia o menu principal (fluxo interativo)
    try:
        menu_principal()
    except KeyboardInterrupt:
        print("\nEncerrado pelo usuário.")