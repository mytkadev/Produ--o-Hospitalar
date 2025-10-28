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

# Arquivos do Controle
CAMINHO_CONTROLE_MUDANCAS = os.path.join(CONTROLE_DIR, "Controle de Mudanças.xlsx")
CAMINHO_QUALIFICACAO_DADOS = os.path.join(CONTROLE_DIR, "Qualificação de Dados.xlsx")
CAMINHO_CONTROLE_REGISTROS = os.path.join(CONTROLE_DIR, "Controle de Registros.xlsx")  # <- NOVO

# Decisões por hospital+termo (persistidas em JSON)
# Formato: { cnes_str: { TERMO_UP: {"acao": "M"|"L", "destino": str|None, "motivo": str|None} } }
decisoes_especialidades = {}

CAMINHO_TEMPLATE_CONTROLE_REM = os.path.join(CONTROLE_DIR, "Controle de Atualização do REM.xlsx")
CAMINHO_CONTROLE_PRODUCAO    = os.path.join(CONTROLE_DIR, "Controle de Atualização de Produção.xlsx")

# Aba onde vamos registrar o status de processamento do Ambulatorial
ABA_STATUS_AMB = "Ambulatorial – Status (Automático)"

# Pastas de planilhas
PLANILHAS_DIR = os.path.join(PRODUCAO_DIR, "Planilhas")
CAMINHO_PLANILHAS = os.path.join(PLANILHAS_DIR, "A serem processadas")
CAMINHO_ARQUIVADAS = os.path.join(PLANILHAS_DIR, "Processadas")

# Pasta "Bases de Dados" é irmã de "Produção Hospitalar"
# (ou seja, fica no mesmo nível da pasta “Produção Hospitalar”)
BASES_DIR = os.path.abspath(os.path.join(PRODUCAO_DIR, os.pardir, "Bases de Dados"))

# Nome do arquivo da base
NOME_ARQUIVO_BASE = "dbProducao.xlsx"
CAMINHO_BASE = os.path.join(BASES_DIR, NOME_ARQUIVO_BASE)
CAMINHO_DHOSPITAIS = os.path.join(BASES_DIR, "dHospitais.xlsx")
# Arquivo de configuração (JSON) ficará ao lado do script, em /Códigos
CONFIG_PATH = os.path.join(SCRIPT_DIR, "ambulatorio_config.json")

# Garante que as pastas existem
os.makedirs(CAMINHO_PLANILHAS, exist_ok=True)
os.makedirs(CAMINHO_ARQUIVADAS, exist_ok=True)
os.makedirs(BASES_DIR, exist_ok=True)

NOME_ABA = "db_ambulatorio"
NOME_ABA_2 = "db_ambulatorio2"
CAMINHO_LOGS = os.path.join(BASES_DIR, "base_logs.xlsx")  


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
    "Traumatologia", 
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

# Substituições manuais de nomes de especialidades para padronização
substituicoes_especialidades = {
}
# Flags auxiliares de interação
ULTIMA_RESOLUCAO_TEXTO = None   # preenchida quando usuário escolhe correção manual (M)
ULTIMO_MOTIVO_ERRO = None       # preenchida quando usuário escolhe mandar pro log (L)

def _criar_controle_producao_vazio():
    """Cria um arquivo do zero com a aba de status se o template não existir."""
    cols = [
        "Data_Registro", "CNES", "Hospital", "Competencia",
        "Arquivo", "Linhas_RAW", "Linhas_Base", "Linhas_Erros", "Status"
    ]
    df_vazio = pd.DataFrame(columns=cols)
    with pd.ExcelWriter(CAMINHO_CONTROLE_PRODUCAO, engine="openpyxl", mode="w") as w:
        df_vazio.to_excel(w, sheet_name=ABA_STATUS_AMB, index=False)
    print(f"📄 Criado '{CAMINHO_CONTROLE_PRODUCAO}' com a aba '{ABA_STATUS_AMB}'.")

def garantir_controle_producao():
    """
    Garante que o arquivo 'Controle de Atualização de Produção.xlsx' exista.
    Se houver o template 'Controle de Atualização do REM.xlsx', copia o template
    para manter o mesmo formato (abas, estilos etc.) e depois cria/atualiza a
    aba 'Ambulatorial – Status (Automático)'.
    """
    os.makedirs(CONTROLE_DIR, exist_ok=True)

    if not os.path.exists(CAMINHO_CONTROLE_PRODUCAO):
        if os.path.exists(CAMINHO_TEMPLATE_CONTROLE_REM):
            # Copia o modelo para manter o "mesmo formato"
            shutil.copy2(CAMINHO_TEMPLATE_CONTROLE_REM, CAMINHO_CONTROLE_PRODUCAO)
            print(f"📎 Copiado template → '{CAMINHO_CONTROLE_PRODUCAO}'.")
            # Garante a aba de status (sem apagar nada do template)
            try:
                # Se a aba já existir no template, não recriamos
                pd.read_excel(CAMINHO_CONTROLE_PRODUCAO, sheet_name=ABA_STATUS_AMB, engine="openpyxl")
            except Exception:
                _criar_controle_producao_vazio()
        else:
            # Sem template: cria um arquivo novo e limpo
            _criar_controle_producao_vazio()

# ===============================================================
# FUNÇÕES AUXILIARES GERAIS
# ===============================================================

def registrar_status_ambulatorio_no_controle_producao(cnes: str,
                                                      nome_hospital: str,
                                                      competencias_str: str,
                                                      arquivo: str,
                                                      linhas_raw: int,
                                                      linhas_base: int,
                                                      linhas_erros: int,
                                                      status: str):
    """
    Adiciona/atualiza o status na aba 'Ambulatorial – Status (Automático)' do
    arquivo 'Controle de Atualização de Produção.xlsx'. Dedup por (CNES, Competencia).
    """
    garantir_controle_producao()

    try:
        try:
            df_exist = pd.read_excel(CAMINHO_CONTROLE_PRODUCAO, sheet_name=ABA_STATUS_AMB, engine="openpyxl")
        except ValueError:
            df_exist = pd.DataFrame()
        except FileNotFoundError:
            df_exist = pd.DataFrame()

        competencias = [c.strip() for c in str(competencias_str).split(";") if c.strip()]
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for comp in competencias:
            rows.append({
                "Data_Registro": agora,
                "CNES": str(cnes).strip(),
                "Hospital": str(nome_hospital).strip(),
                "Competencia": comp,
                "Arquivo": arquivo,
                "Linhas_RAW": linhas_raw,
                "Linhas_Base": linhas_base,
                "Linhas_Erros": linhas_erros,
                "Status": status
            })
        df_new = pd.DataFrame(rows)

        # Dedup por (CNES, Competencia) — remove antigos e mantém os novos
        if not df_exist.empty and all(col in df_exist.columns for col in ["CNES", "Competencia"]):
            chave = ["CNES", "Competencia"]
            # Normalizar CNES como texto antes da comparação
            df_exist["CNES"] = df_exist["CNES"].astype(str).str.strip()
            df_new["CNES"] = df_new["CNES"].astype(str).str.strip()

            mask = df_exist.merge(
                df_new[chave].drop_duplicates(),
                on=chave,
                how="left",
                indicator=True
            )["_merge"] == "both"
            df_exist = df_exist[~mask]

        df_out = pd.concat([df_exist, df_new], ignore_index=True)

        with pd.ExcelWriter(CAMINHO_CONTROLE_PRODUCAO, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            df_out.to_excel(w, sheet_name=ABA_STATUS_AMB, index=False)

        print(f"🗂️ Controle atualizado em '{CAMINHO_CONTROLE_PRODUCAO}' (aba '{ABA_STATUS_AMB}').")
    except Exception as e:
        print(f"❌ Erro ao atualizar controle de produção: {e}")

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
    """Opção 1: Reprocessa o log e tenta inserir o que for corrigível, em seguida atualiza o controle."""
    print(_title("\n▶ Retificação de dados pendentes"))
    processar_log_de_erros()
    atualizar_aba_controle()
    print(_ok("✔ Retificação concluída.\n"))
    input(_muted("Pressione Enter para voltar ao menu... "))

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
    Registra erros de extração de dados de consultórios na aba 'consultorios_log'
    """

    if not erros_consultorios:
        return

    print(f"📝 Registrando {len(erros_consultorios)} erros em 'consultorios_log'.")

    try:
        df_log_existente = pd.read_excel(CAMINHO_BASE, sheet_name="consultorios_log", engine="openpyxl")
    except:
        df_log_existente = pd.DataFrame()

    df_novos_logs = pd.DataFrame(erros_consultorios)
    df_completo = pd.concat([df_log_existente, df_novos_logs], ignore_index=True)

    with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_completo.to_excel(writer, sheet_name="consultorios_log", index=False)

    print("✅ Log de consultórios atualizado com sucesso.")

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
    Reanalisa o log de erros ambulatoriais, tenta corrigir linhas com substituições manuais
    e insere na base principal caso possível.
    """
    try:
        df_log = pd.read_excel(CAMINHO_BASE, sheet_name="ambulatorio_log", engine="openpyxl")
        if df_log.empty:
            print("✅ Log de erros está vazio.")
            return
    except:
        print("ℹ️ Nenhum log encontrado.")
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


    # Atualiza o log
    df_novo_log = pd.DataFrame(ainda_invalidos)
    with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_novo_log.to_excel(writer, sheet_name="ambulatorio_log", index=False)

    print(f"✅ Log atualizado. {len(corrigidos)} linhas corrigidas, {len(df_para_inserir)} inseridas na base.")

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
         b) escolhendo da lista de especialidades.
    Também memoriza a decisão para (CNES, termo) via _set_decisao.
    Retorna o destino (string) se for para base, ou None se for para log.
    Define ULTIMA_RESOLUCAO_TEXTO / ULTIMO_MOTIVO_ERRO.
    """
    global ULTIMO_MOTIVO_ERRO, ULTIMA_RESOLUCAO_TEXTO

    termo = str(especialidade_original).strip()
    termo_up = termo.upper()

    # Se já houver decisão memorizada p/ este hospital + termo, aplica:
    dec = _get_decisao(cnes, termo_up)
    if dec:
        if dec["acao"] == "M" and dec.get("destino"):
            ULTIMA_RESOLUCAO_TEXTO = dec.get("motivo") or ""
            return dec["destino"]
        else:
            ULTIMO_MOTIVO_ERRO = dec.get("motivo") or "Especialidade não reconhecida (decisão memorizada)"
            return None

    # Menu principal
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

    if escolha == "1":
        # Submenu de MOTIVOS:
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
            motivo = _ask("Digite o motivo: ")
            if not motivo:
                motivo = "Outros (sem detalhamento)"

        ULTIMO_MOTIVO_ERRO = motivo
        ULTIMA_RESOLUCAO_TEXTO = None

        # memoriza decisão “L”
        _set_decisao(cnes, termo_up, "L", None, motivo)
        return None

    # escolha == "2"  → modificar e inserir
    print("\nComo deseja modificar?")
    print("  [a] Digitar a especialidade padronizada")
    print("  [b] Buscar na lista de especialidades")
    while True:
        sub = _ask("Escolha (a/b): ").lower()
        if sub in ("a", "b"):
            break
        print("Opção inválida.")

    if sub == "a":
        destino = _ask("Digite a especialidade padronizada: ")
        while not destino.strip():
            destino = _ask("Valor vazio. Digite a especialidade padronizada: ")
    else:
        idx = _lista_paginada(lista_especialidades_ambulatorio, "Especialidades – escolha um destino", por_pagina=50)

        if idx == -1:
            destino = _ask("Digite a especialidade padronizada: ")
            while not destino.strip():
                destino = _ask("Valor vazio. Digite a especialidade padronizada: ")
        else:
            destino = lista_especialidades_ambulatorio[idx]

    # “Qual foi a resolução?” (categorias)
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
        resol = _ask("Descreva a resolução: ")
        if not resol:
            resol = "Outros (sem detalhamento)"

    # Atualiza estruturas:
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
        if score >= 90:
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

                
                # Se houve correção manual agora, grava na aba 'registros' e no Controle
                if ULTIMA_RESOLUCAO_TEXTO:
                    # 1) Aba 'registros' dentro do CAMINHO_BASE
                    registrar_resolucao_registro({
                        "data_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "arquivo": arquivo,
                        "cnes": cnes,
                        "competencia": competencia,
                        "especialidade_original": str(especialidade_bruta),
                        "especialidade_final": especialidade_corrigida,
                        "resolucao": ULTIMA_RESOLUCAO_TEXTO
                    })

                    # 2) Arquivo de Controle (Controle/controle_especialidades.xlsx)
                    registrar_controle_especialidade({
                        "data_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "arquivo": arquivo,
                        "cnes": cnes,
                        "nome_hospital": nome_hospital,          # <- usamos o nome extraído do arquivo
                        "competencia": competencia,
                        "especialidade_original": str(especialidade_bruta),
                        "especialidade_final": especialidade_corrigida,
                        "resolucao": ULTIMA_RESOLUCAO_TEXTO
                    })

                    # limpa a flag para não registrar mais de uma vez indevidamente
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

            # 👉 Escreve no novo arquivo "Controle de Atualização de Produção.xlsx"
            registrar_status_ambulatorio_no_controle_producao(
                cnes=cnes,
                nome_hospital=nome_hospital,
                competencias_str=competencia_resumo,  # ex.: '2025-01;2025-02'
                arquivo=arquivo,
                linhas_raw=linhas_raw,
                linhas_base=linhas_base,
                linhas_erros=linhas_erros,
                status=status
            )

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
    Registra as linhas com especialidades não reconhecidas no log 'ambulatorio_log'
    e espelha em CAMINHO_LOGS.
    """
    if not linhas_invalidas:
        return

    print(f"📝 Registrando {len(linhas_invalidas)} erros em 'ambulatorio_log'.")

    # 1) Carregar o log existente na base principal
    try:
        df_log_existente = pd.read_excel(CAMINHO_BASE, sheet_name="ambulatorio_log", engine="openpyxl")
    except:
        df_log_existente = pd.DataFrame()

    # 2) Montar novos logs
    df_novos_logs = pd.DataFrame(linhas_invalidas)
    df_completo = pd.concat([df_log_existente, df_novos_logs], ignore_index=True)

    # 3) Salvar na base principal
    with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_completo.to_excel(writer, sheet_name="ambulatorio_log", index=False)
    print("✅ Log de erros atualizado com sucesso.")

    # 4) Espelhar no arquivo de logs central
    # ✅ Espelhar no arquivo de logs central
    try:
        if os.path.exists(CAMINHO_LOGS):
            # Atualiza preservando outras abas
            df_log_existente2 = pd.read_excel(CAMINHO_LOGS, sheet_name="ambulatorio_log", engine="openpyxl")
            df_completo2 = pd.concat([df_log_existente2, df_novos_logs], ignore_index=True)

            with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl",
                                mode="a", if_sheet_exists="replace") as writer:
                df_completo2.to_excel(writer, sheet_name="ambulatorio_log", index=False)

        else:
            # Cria o arquivo do zero
            df_completo2 = pd.DataFrame(df_novos_logs)

            with pd.ExcelWriter(CAMINHO_LOGS, engine="openpyxl",
                                mode="w") as writer:
                df_completo2.to_excel(writer, sheet_name="ambulatorio_log", index=False)


        print("📚 Log espelhado em 'base_logs.xlsx'.")
    except Exception as e:
        print(f"❌ Erro ao espelhar log em CAMINHO_LOGS: {e}")

def registrar_resolucao_registro(registro: dict):
    """
    Salva SOMENTE em:
      Controle/Controle de Registros.xlsx → aba: 'Registros'
    """
    try:
        os.makedirs(CONTROLE_DIR, exist_ok=True)

        sheet = "Registros"

        # Lê o que já existe (se existir)
        try:
            df_exist = pd.read_excel(CAMINHO_CONTROLE_REGISTROS, sheet_name=sheet, engine="openpyxl")
        except FileNotFoundError:
            df_exist = pd.DataFrame()
        except Exception:
            df_exist = pd.DataFrame()

        df_new = pd.DataFrame([registro])
        df_out = pd.concat([df_exist, df_new], ignore_index=True)

        if os.path.exists(CAMINHO_CONTROLE_REGISTROS):
            with pd.ExcelWriter(CAMINHO_CONTROLE_REGISTROS, engine="openpyxl",
                                mode="a", if_sheet_exists="replace") as writer:
                df_out.to_excel(writer, sheet_name=sheet, index=False)
        else:
            with pd.ExcelWriter(CAMINHO_CONTROLE_REGISTROS, engine="openpyxl",
                                mode="w") as writer:
                df_out.to_excel(writer, sheet_name=sheet, index=False)

        print("📝 Registro salvo em 'Controle de Registros.xlsx' → aba 'Registros'.")
    except Exception as e:
        print(f"❌ Erro ao salvar em 'Controle de Registros.xlsx': {e}")


def registrar_controle_especialidade(registro: dict):
    """
    Salva o registro de mudança manual em:
      Controle/Controle de Mudanças.xlsx  → aba: 'Ambulatório'
    Colunas esperadas no dict:
      data_registro, arquivo, cnes, nome_hospital, competencia,
      especialidade_original, especialidade_final, resolucao
    """
    try:
        os.makedirs(CONTROLE_DIR, exist_ok=True)

        # Lê o que já existe (se existir)
        try:
            df_exist = pd.read_excel(
                CAMINHO_CONTROLE_MUDANCAS,
                sheet_name="Ambulatório",
                engine="openpyxl"
            )
        except FileNotFoundError:
            df_exist = pd.DataFrame()
        except Exception as e:
            print(f"⚠️ Erro ao ler 'Controle de Mudanças' (será recriado): {e}")
            df_exist = pd.DataFrame()

        df_new = pd.DataFrame([registro])
        df_out = pd.concat([df_exist, df_new], ignore_index=True)

        # Cria ou atualiza o arquivo/aba
        if os.path.exists(CAMINHO_CONTROLE_MUDANCAS):
            with pd.ExcelWriter(CAMINHO_CONTROLE_MUDANCAS, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_out.to_excel(writer, sheet_name="Ambulatório", index=False)
        else:
            with pd.ExcelWriter(CAMINHO_CONTROLE_MUDANCAS, engine="openpyxl", mode="w") as writer:
                df_out.to_excel(writer, sheet_name="Ambulatório", index=False)

        print(f"🗂️ Mudança registrada em '{CAMINHO_CONTROLE_MUDANCAS}' (aba 'Ambulatório').")
    except Exception as e:
        print(f"❌ Erro ao salvar em 'Controle de Mudanças': {e}")

def registrar_controle_resumo(resumo: dict):
    """
    Salva/atualiza o resumo do processamento por arquivo em:
      Controle/Qualificação de Dados.xlsx  → aba: 'Ambulatório'
    Colunas esperadas no dict:
      arquivo, cnes, nome_hospital, competencia,
      n° de linhas raw, n° de linhas base, n° de linhas erros, diferença linhas,
      soma dos dados raw, soma dos dados base, soma dos dados erros, diferença soma
    """
    try:
        os.makedirs(CONTROLE_DIR, exist_ok=True)

        # Lê o que já existe
        try:
            df_exist = pd.read_excel(
                CAMINHO_QUALIFICACAO_DADOS,
                sheet_name="Ambulatório",
                engine="openpyxl"
            )
        except FileNotFoundError:
            df_exist = pd.DataFrame()
        except Exception as e:
            print(f"⚠️ Erro ao ler 'Qualificação de Dados' (será recriado): {e}")
            df_exist = pd.DataFrame()

        df_new = pd.DataFrame([resumo])

        # Se já houver linha desse arquivo+competência, substitui (mantém 1 por par)
        chave = ["arquivo", "competencia"]
        # Normalizar CNES e Competência para garantir merge correto
        df_exist["cnes"] = df_exist["cnes"].astype(str).str.strip()
        resumo["cnes"] = str(resumo["cnes"]).strip()

        if not df_exist.empty and all(c in df_exist.columns for c in chave):
            mask_dup = (df_exist["arquivo"] == resumo["arquivo"]) & (df_exist["competencia"] == resumo["competencia"])
            df_exist = df_exist[~mask_dup]

        df_out = pd.concat([df_exist, df_new], ignore_index=True)
        
        # Cria ou atualiza o arquivo/aba
        if os.path.exists(CAMINHO_QUALIFICACAO_DADOS):
            with pd.ExcelWriter(CAMINHO_QUALIFICACAO_DADOS, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_out.to_excel(writer, sheet_name="Ambulatório", index=False)
        else:
            with pd.ExcelWriter(CAMINHO_QUALIFICACAO_DADOS, engine="openpyxl", mode="w") as writer:
                df_out.to_excel(writer, sheet_name="Ambulatório", index=False)

        print(f"📊 Resumo salvo em '{CAMINHO_QUALIFICACAO_DADOS}' (aba 'Ambulatório').")
    except Exception as e:
        print(f"❌ Erro ao salvar em 'Qualificação de Dados': {e}")



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
        with pd.ExcelWriter(CAMINHO_BASE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_controle.to_excel(writer, sheet_name="controle_ambulatorio", index=False)

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