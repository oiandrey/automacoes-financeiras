"""
nfse_processor/cep_corrector.py
================================
Corretor automatizado de arquivos NFS-e rejeitados pelas prefeituras.

Problema resolvido:
    Arquivos NFS-e são rejeitados pela prefeitura com erros específicos
    (CEP inválido, e-mail incorreto, IM do prestador, PIS incorreto).
    Este script monitora uma pasta de gatilho, lê o arquivo de erro,
    identifica o tipo de problema e aplica a correção automaticamente
    consultando as APIs do ViaCEP e IBGE quando necessário.

Destaques técnicos:
    - Consumo de APIs REST externas (ViaCEP + IBGE)
    - Cache em memória para evitar chamadas repetidas
    - Fuzzy matching para comparação de logradouros (rapidfuzz)
    - Fallback progressivo na busca de CEP
    - Arquitetura orientada a eventos via arquivo de gatilho

Autor: Andrey Araujo
Versão: 1.0.0
"""

import os
import requests
import unicodedata
import re
import shutil
import logging
from rapidfuzz import fuzz
from urllib.parse import quote
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ======================
# CONFIGURAÇÃO
# ======================

# Caminhos lidos de variáveis de ambiente — nunca hardcodados
BASE    = os.environ.get("NFSE_BASE_PATH",    r"C:\nfse\entradas")
GATILHO = os.environ.get("NFSE_GATILHO_PATH", r"C:\nfse\gatilho")

# Códigos de filial a processar
CODIGOS = os.environ.get("NFSE_CODIGOS", "FILIAL_01").split(",")

SEP       = ";"
SCORE_MIN = 85  # Score mínimo para aceitar match de logradouro (0-100)

# Índices dos campos no arquivo NFS-e
IDX_LOGRADOURO = 1
IDX_NUMERO     = 2
IDX_BAIRRO     = 4
IDX_CIDADE     = 5
IDX_UF         = 7
IDX_CEP        = 8

# Valor de IM do prestador a remover — configure via variável de ambiente
IM_PRESTADOR = os.environ.get("NFSE_IM_PRESTADOR", "")

# ======================
# LOGGING
# ======================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            f"logs/cep_corrector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ======================
# CACHE EM MEMÓRIA
# Evita chamadas repetidas às APIs externas
# ======================

CACHE_VIACEP = {}
CACHE_IBGE   = {}

# ======================
# HELPERS
# ======================

def normalizar(texto: str) -> str:
    """
    Normaliza string para comparação:
    - Converte para maiúsculas
    - Remove acentos
    - Remove prefixos de logradouro (RUA, AV, etc.)
    """
    if not texto:
        return ""
    texto = texto.upper().strip()
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    for prefixo in ["R ", "RUA ", "AV ", "AVENIDA ", "TRAV ", "TRAVESSA ", "ESTRADA ", "RODOVIA "]:
        if texto.startswith(prefixo):
            texto = texto[len(prefixo):]
    return texto

# ======================
# INTEGRAÇÃO IBGE API
# ======================

def cidade_por_ibge(codigo_ibge: str):
    """
    Consulta a API do IBGE para obter nome da cidade e UF pelo código IBGE.
    Retorna (cidade, uf) ou None se não encontrar.
    """
    if codigo_ibge in CACHE_IBGE:
        return CACHE_IBGE[codigo_ibge]

    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{codigo_ibge}"
    log.info("[IBGE] Consultando: %s", url)

    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        data = r.json()

        cidade = data.get("nome")
        uf     = data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla")

        if cidade:
            CACHE_IBGE[codigo_ibge] = (cidade, uf)
            log.info("[IBGE] %s -> %s/%s", codigo_ibge, cidade, uf)
            return cidade, uf

    except Exception as e:
        log.error("[IBGE] Erro ao consultar %s: %s", codigo_ibge, e)

    CACHE_IBGE[codigo_ibge] = None
    return None

# ======================
# INTEGRAÇÃO VIACEP API
# ======================

def buscar_cep(uf: str, cidade: str, logradouro: str) -> list:
    """
    Consulta a API ViaCEP para buscar CEPs pelo endereço.
    Implementa fallback progressivo:
    1. Tenta com logradouro completo
    2. Tenta sem prefixo (sem RUA, AV, etc.)
    3. Tenta palavra por palavra
    Retorna lista de resultados ou lista vazia.
    """
    chave = (uf, cidade, normalizar(logradouro))
    if chave in CACHE_VIACEP:
        return CACHE_VIACEP[chave]

    def consultar(lg: str) -> list:
        url = f"https://viacep.com.br/ws/{uf}/{quote(cidade)}/{quote(lg)}/json/"
        log.info("[VIACEP] Consultando: %s", url)
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                log.info("[VIACEP] %d resultado(s) encontrado(s)", len(data))
                return data
        except Exception as e:
            log.error("[VIACEP] Erro: %s", e)
        return []

    # Tentativa 1: logradouro completo
    resultados = consultar(logradouro)

    # Tentativa 2: sem prefixo
    if not resultados:
        sem_prefixo = normalizar(logradouro)
        resultados  = consultar(sem_prefixo)

        # Tentativa 3: palavra por palavra
        if not resultados:
            for palavra in sem_prefixo.split():
                resultados = consultar(palavra)
                if resultados:
                    break

    CACHE_VIACEP[chave] = resultados
    return resultados


def selecionar_cep(numero: str, logradouro: str, bairro: str, resultados: list) -> str:
    """
    Seleciona o CEP mais adequado da lista de resultados usando:
    1. Fuzzy matching de logradouro (peso 70%) + bairro (peso 30%)
    2. Fallback para o primeiro resultado se score mínimo não atingido
    Retorna o CEP selecionado ou None.
    """
    if not resultados:
        return None

    melhor_cep   = None
    melhor_score = 0

    for d in resultados:
        score_logradouro = fuzz.ratio(normalizar(d.get("logradouro", "")), normalizar(logradouro))
        score_bairro     = fuzz.ratio(normalizar(d.get("bairro", "")),     normalizar(bairro))
        score_total      = score_logradouro * 0.7 + score_bairro * 0.3

        if score_total > melhor_score:
            melhor_score = score_total
            melhor_cep   = d.get("cep")

    if melhor_score >= SCORE_MIN:
        log.info("[CEP] Match encontrado com score %.1f: %s", melhor_score, melhor_cep)
        return melhor_cep

    # Fallback: retorna primeiro resultado
    log.warning("[CEP] Score baixo (%.1f), usando primeiro resultado", melhor_score)
    return resultados[0].get("cep")

# ======================
# CORREÇÕES ESPECÍFICAS
# ======================

def corrigir_cep(linhas: list) -> bool:
    """Consulta ViaCEP e corrige o CEP na linha alvo do arquivo."""
    try:
        campos = linhas[1].rstrip("\n").split(SEP)

        logradouro = campos[IDX_LOGRADOURO] if len(campos) > IDX_LOGRADOURO else ""
        numero     = campos[IDX_NUMERO]     if len(campos) > IDX_NUMERO     else ""
        bairro     = campos[IDX_BAIRRO]     if len(campos) > IDX_BAIRRO     else ""
        cidade     = campos[IDX_CIDADE]     if len(campos) > IDX_CIDADE     else ""
        uf         = campos[IDX_UF]         if len(campos) > IDX_UF         else ""

        if not all([logradouro, cidade, uf]):
            log.warning("[CEP] Campos insuficientes para busca")
            return False

        resultados = buscar_cep(uf, cidade, logradouro)
        novo_cep   = selecionar_cep(numero, logradouro, bairro, resultados)

        if novo_cep and len(campos) > IDX_CEP:
            log.info("[CEP] %s -> %s", campos[IDX_CEP], novo_cep)
            campos[IDX_CEP] = novo_cep
            linhas[1]       = SEP.join(campos) + "\n"
            return True

    except Exception as e:
        log.error("[CEP] Erro ao corrigir: %s", e)

    return False


def corrigir_email(linhas: list) -> bool:
    """Remove campo de e-mail inválido da linha 15."""
    idx_linha = 14
    idx_campo = 2

    if len(linhas) <= idx_linha:
        return False

    campos = linhas[idx_linha].rstrip("\n").split(SEP)

    if len(campos) <= idx_campo:
        return False

    campos.pop(idx_campo)
    linhas[idx_linha] = SEP.join(campos) + "\n"
    log.info("[EMAIL] Campo removido da linha 15")
    return True


def corrigir_im_prestador(linhas: list) -> bool:
    """Remove IM do prestador incorreto da linha 9."""
    idx_linha = 8
    idx_campo = 2

    if len(linhas) <= idx_linha:
        return False

    campos = linhas[idx_linha].rstrip("\n").split(SEP)

    if len(campos) <= idx_campo:
        return False

    if IM_PRESTADOR and campos[idx_campo] == IM_PRESTADOR:
        campos[idx_campo] = ""
        linhas[idx_linha] = SEP.join(campos) + "\n"
        log.info("[IM] Valor removido da linha 9")
        return True

    return False


def corrigir_pis(linhas: list) -> bool:
    """
    Recalcula valores de PIS/COFINS com base na base de cálculo e alíquotas.
    Fórmula: valor = base * aliquota
    """
    try:
        if len(linhas) < 5:
            return False

        linha4 = linhas[3].rstrip("\n").split(SEP)
        linha5 = linhas[4].rstrip("\n").split(SEP)

        base   = float(linha4[1].replace(",", "."))
        aliq1  = float(linha5[1].replace(",", "."))
        aliq2  = float(linha5[2].replace(",", "."))

        novo4 = base * aliq1
        novo5 = base * aliq2

        log.info("[PIS] %s/%s -> %s/%s",
                 linha4[3], linha4[4],
                 f"{novo4:.2f}".replace(".", ","),
                 f"{novo5:.2f}".replace(".", ","))

        linha4[3] = f"{novo4:.2f}".replace(".", ",")
        linha4[4] = f"{novo5:.2f}".replace(".", ",")
        linhas[3] = SEP.join(linha4) + "\n"
        return True

    except Exception as e:
        log.error("[PIS] Erro ao corrigir: %s", e)
        return False

# ======================
# PIPELINE PRINCIPAL
# ======================

def corrigir_arquivo(path_arquivo: str, path_erro: str, codigo: str) -> bool:
    """
    Pipeline de correção de um arquivo NFS-e:
    1. Lê o arquivo de erro para identificar o tipo de problema
    2. Aplica as correções necessárias
    3. Salva e move para pasta de saída
    """
    log.info("Processando: %s", path_arquivo)

    try:
        with open(path_arquivo, encoding="latin-1", errors="ignore") as f:
            linhas = f.readlines()
    except Exception as e:
        log.error("Erro ao ler arquivo %s: %s", path_arquivo, e)
        return False

    try:
        with open(path_erro, encoding="latin-1", errors="ignore") as f:
            erro = f.read().lower()
    except Exception as e:
        log.warning("Arquivo de erro não encontrado: %s", e)
        erro = ""

    alterou = False

    # Aplica correções conforme tipo de erro identificado
    if "cep" in erro:
        log.info("Tipo de erro: CEP")
        if corrigir_cep(linhas):
            alterou = True

    if "email" in erro:
        log.info("Tipo de erro: EMAIL")
        if corrigir_email(linhas):
            alterou = True

    if "im do prestador" in erro:
        log.info("Tipo de erro: IM DO PRESTADOR")
        if corrigir_im_prestador(linhas):
            alterou = True

    if "pis" in erro:
        log.info("Tipo de erro: PIS")
        if corrigir_pis(linhas):
            alterou = True

    if alterou:
        try:
            with open(path_arquivo, "w", encoding="latin-1") as f:
                f.writelines(linhas)

            pasta_saida = os.path.join(BASE, codigo, "nfse")
            os.makedirs(pasta_saida, exist_ok=True)

            destino = os.path.join(pasta_saida, os.path.basename(path_arquivo))
            shutil.move(path_arquivo, destino)
            log.info("Arquivo corrigido e movido para: %s", destino)

        except Exception as e:
            log.error("Erro ao salvar/mover arquivo: %s", e)
            return False
    else:
        log.warning("Nenhuma correção aplicada em: %s", path_arquivo)

    return alterou


def main():
    log.info("=" * 50)
    log.info("Iniciando Corretor NFS-e")
    log.info("Filiais: %s", CODIGOS)
    log.info("=" * 50)

    total_corrigidos = 0
    total_sem_acao   = 0

    for codigo in CODIGOS:
        pasta_gatilho = os.path.join(GATILHO, codigo)
        pasta_base    = os.path.join(BASE,    codigo)

        if not os.path.isdir(pasta_gatilho):
            log.warning("Pasta de gatilho não encontrada: %s", pasta_gatilho)
            continue

        # Processa apenas arquivos de erro (_ERRO.TXT)
        arquivos_erro = [
            nome for nome in os.listdir(pasta_gatilho)
            if nome.upper().endswith("_ERRO.TXT")
        ]

        log.info("Filial %s: %d arquivo(s) de erro encontrado(s)", codigo, len(arquivos_erro))

        for nome_erro in arquivos_erro:
            lote_match = re.match(r"(\d+)_", nome_erro)
            if not lote_match:
                continue

            lote      = lote_match.group(1)
            path_erro = os.path.join(pasta_gatilho, nome_erro)

            # Localiza arquivo base correspondente ao lote
            arquivo_base = next(
                (os.path.join(pasta_base, f) for f in os.listdir(pasta_base) if f"_{lote}_" in f),
                None
            )

            if not arquivo_base:
                log.warning("Arquivo base não encontrado para lote %s", lote)
                continue

            ok = corrigir_arquivo(arquivo_base, path_erro, codigo)

            if ok:
                total_corrigidos += 1
            else:
                total_sem_acao += 1

    log.info("=" * 50)
    log.info("Finalizado. Corrigidos: %d | Sem acao: %d", total_corrigidos, total_sem_acao)
    log.info("=" * 50)


if __name__ == "__main__":
    main()
