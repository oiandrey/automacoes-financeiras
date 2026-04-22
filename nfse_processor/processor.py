"""
nfse_processor/processor.py
============================
Processador automatizado de arquivos NFS-e (Nota Fiscal de Serviço Eletrônica).

Problema resolvido:
    Arquivos de NFS-e gerados pelo ERP continham inconsistências por município,
    causando rejeição nas prefeituras. Este script vasculha as pastas de cada
    filial, aplica as correções necessárias por código de município e move os
    arquivos para a pasta de saída — sem intervenção manual.

Impacto:
    Eliminou processo manual de correção arquivo a arquivo,
    viabilizando o faturamento recorrente de múltiplas filiais.

Autor: Andrey Araujo
Versão: 1.0.0
"""
from nfse_processor.alertas import alerta_erro, relatorio_execucao
import os
import shutil
import time
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from nfse_processor.db import criar_tabelas, registrar


# ======================
# CONFIGURAÇÃO
# ======================d

# Lê o caminho base de variável de ambiente para não expor caminhos de rede
# Configure no seu ambiente: set NFSE_BASE_PATH=\\servidor\pasta
BASE = os.environ.get("NFSE_BASE_PATH", r"C:\nfse\entradas")

# Códigos de filial a processar — substitua pelos códigos reais no ambiente
CODIGOS = [
    "FILIAL_01",
    "FILIAL_02",
]

SEP = ";"

# ======================
# LOGGING
# ======================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/nfse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ======================
# HELPERS
# ======================

def limpar_linha(l: str) -> str:
    return l.replace("\r", "").replace("\n", "")


def campo(campos: list, i: int) -> str:
    if i >= len(campos):
        return ""
    return campos[i].strip().replace("\xa0", "").replace("\t", "")


def dec_br(v) -> Decimal:
    """Converte string no formato brasileiro (vírgula decimal) para Decimal."""
    if v is None:
        return Decimal("0")
    v = str(v)
    v = (v
         .replace("\ufeff", "")
         .replace("\xa0", "")
         .replace(" ", "")
         .replace("\t", "")
         .replace("\r", "")
         .replace("\n", "")
         .strip())
    if v == "":
        return Decimal("0")
    try:
        return Decimal(v.replace(",", "."))
    except InvalidOperation:
        return Decimal("0")


def fmt_br(d: Decimal) -> str:
    """Formata Decimal de volta para string no formato brasileiro."""
    return format(d, "f").replace(".", ",")


def ler_arquivo_multi_encoding(caminho: str) -> tuple:
    """Tenta ler o arquivo com diferentes encodings, retorna (linhas, encoding)."""
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            with open(caminho, "r", encoding=enc) as f:
                return f.readlines(), enc
        except UnicodeDecodeError:
            continue
    with open(caminho, "r", encoding="latin-1", errors="ignore") as f:
        return f.readlines(), "latin-1"


def get_campo_safe(split: list, linha: int, campo_n: int) -> str:
    """Retorna campo de uma linha de forma segura, sem lançar exceção."""
    try:
        return split[linha - 1][campo_n - 1]
    except (IndexError, TypeError):
        return ""

# ======================
# REGRAS GLOBAIS
# Aplicadas em todas as filiais, salvo override específico
# ======================

def regra_linha4(linhas: list) -> tuple:
    """
    Consolida valores tributários na linha 4.
    Soma campos de deduções e zera os campos de origem.
    """
    alterou = False
    if len(linhas) >= 4:
        linha = limpar_linha(linhas[3])
        campos = linha.split(SEP)
        while len(campos) <= 7:
            campos.append("")
        c4 = dec_br(campo(campos, 3))
        c5 = dec_br(campo(campos, 4))
        c8 = dec_br(campo(campos, 7))
        novo = c8 + c4 + c5
        campos[7] = fmt_br(novo)
        campos[3] = "0"
        campos[4] = "0"
        linhas[3] = SEP.join(campos) + "\n"
        alterou = True
    return linhas, alterou


def regra_linha5(linhas: list) -> tuple:
    """
    Ajusta código de tributação e zera campos de alíquota na linha 5.
    Regra: código 1 vira 3, código 2 vira 0.
    """
    alterou = False
    if len(linhas) >= 5:
        linha = limpar_linha(linhas[4])
        campos = linha.split(SEP)
        while len(campos) <= 19:
            campos.append("")
        v19 = campo(campos, 19)
        if v19 == "1":
            campos[19] = "3"
        elif v19 == "2":
            campos[19] = "0"
        campos[1] = "0,0000"
        campos[2] = "0,0000"
        campos[5] = "0,0000"
        linhas[4] = SEP.join(campos) + "\n"
        alterou = True
    return linhas, alterou

# ======================
# REGRAS ESPECÍFICAS POR MUNICÍPIO
# Cada município pode ter layout diferente de NFS-e
# ======================

def regra_municipio_a(linhas: list) -> tuple:
    """
    Município A: corrige campo de código IBGE na linha 8.
    Necessário pois o ERP gera código incorreto para este município.
    """
    alterou = False
    CODIGO_IBGE_CORRETO = "0000000"  # substituir pelo código IBGE real no ambiente

    if len(linhas) >= 8:
        campos = linhas[7].rstrip("\n").split(SEP)
        while len(campos) <= 10:
            campos.append("")
        if campos[10] != CODIGO_IBGE_CORRETO:
            campos[10] = CODIGO_IBGE_CORRETO
            linhas[7] = SEP.join(campos) + "\n"
            alterou = True
            log.info("Município A → código IBGE corrigido na linha 8")
    return linhas, alterou


def regra_municipio_b(linhas: list) -> tuple:
    """
    Município B: complementa linha 8 com campo de valor repetido no final.
    Layout exige campo duplicado para validação da prefeitura.
    """
    if len(linhas) < 8:
        return linhas, False
    linha = limpar_linha(linhas[7])
    campos = linha.split(SEP)
    if len(campos) < 8:
        return linhas, False
    v8 = campo(campos, 7)
    if not linha.endswith(f";;;{v8};"):
        linhas[7] = linha + f";;;{v8};\n"
        return linhas, True
    return linhas, False


def regra_municipio_c(linhas: list) -> tuple:
    """
    Município C: expande linha 7 com campos complementares de tributos
    e remove linhas após marcador 4000 (blocos não aceitos pela prefeitura).
    """
    alterou = False
    split = [l.rstrip("\n").split(SEP) for l in linhas]
    if len(split) < 7:
        return linhas, False
    linha7 = split[6]
    while len(linha7) < 20:
        linha7.append("")
    novos_campos = [
        get_campo_safe(split, 3, 10),
        get_campo_safe(split, 4, 9),
        get_campo_safe(split, 4, 10),
        "0,00", "0,00", "0,00",
        get_campo_safe(split, 3, 6),
        "0,00", "0,00", "0,0000",
        "0,00",
        get_campo_safe(split, 3, 22),
        "0,00", "0,00", ""
    ]
    linha7.extend(novos_campos)
    linhas[6] = SEP.join(linha7) + "\n"
    alterou = True

    # Remove blocos após marcador 4000
    for i, l in enumerate(linhas):
        if l.lstrip().startswith("4000"):
            linhas = linhas[:i]
            log.info("Município C → bloco 4000 removido a partir da linha %d", i)
            break

    return linhas, alterou


def regra_municipio_d(linhas: list) -> tuple:
    """
    Município D: corrige código de serviço na linha 16.
    ERP gera código legado; prefeitura exige código atualizado.
    """
    alterou = False
    LINHA_16 = 15
    IDX_CAMPO11 = 10
    CODIGO_ANTIGO = "0000"   # substituir pelo código legado real
    CODIGO_NOVO = "0000"     # substituir pelo código correto real

    if len(linhas) > LINHA_16:
        campos16 = linhas[LINHA_16].rstrip("\n").split(SEP)
        while len(campos16) <= IDX_CAMPO11:
            campos16.append("")
        if campos16[IDX_CAMPO11].strip() == CODIGO_ANTIGO:
            campos16[IDX_CAMPO11] = CODIGO_NOVO
            linhas[LINHA_16] = SEP.join(campos16) + "\n"
            alterou = True
            log.info("Município D → código de serviço corrigido na linha 16")
    return linhas, alterou

# ======================
# MAPEAMENTO DE REGRAS
# ======================

# Filiais que substituem completamente as regras globais
SUBSTITUI_GLOBAIS = {
    "FILIAL_02",
}

# Mapeamento filial → lista de regras específicas
REGRAS_ESPECIFICAS = {
    "FILIAL_01": [regra_municipio_a],
    "FILIAL_02": [regra_municipio_b, regra_municipio_c],
}

# ======================
# PIPELINE DE PROCESSAMENTO
# ======================

def processar_arquivo(caminho: str, pasta_filial: str, codigo: str) -> None:
    """
    Processa um único arquivo NFS-e:
    1. Lê com encoding automático
    2. Aplica regras globais e/ou específicas
    3. Salva se houve alteração
    4. Move para pasta /nfse de saída
    """

    # Ignora arquivos já processados
    if os.path.dirname(caminho).lower().endswith("nfse"):
        return

    try:
        if os.path.getsize(caminho) == 0:
            log.warning("Arquivo vazio ignorado: %s", caminho)
            return
    except OSError as e:
        log.error("Erro ao acessar arquivo %s: %s", caminho, e)
        return

    log.info("Processando: %s", caminho)

    try:
        linhas, encoding_usado = ler_arquivo_multi_encoding(caminho)
    except Exception as e:
        log.error("Erro ao ler arquivo %s: %s", caminho, e)
        return

    alterou = False

    # Aplica regras globais (se filial não tiver override)
    if codigo not in SUBSTITUI_GLOBAIS:
        for regra in (regra_linha4, regra_linha5):
            linhas, a = regra(linhas)
            if a:
                alterou = True

    # Aplica regras específicas da filial
    for regra in REGRAS_ESPECIFICAS.get(codigo, []):
        try:
            linhas, a = regra(linhas)
            if a:
                alterou = True
        except Exception as e:
            log.error("Erro na regra %s para arquivo %s: %s", regra.__name__, caminho, e)

    # Salva se houve alteração
    if alterou:
        try:
            with open(caminho, "w", encoding=encoding_usado) as f:
                f.writelines(linhas)
            log.info("Arquivo alterado e salvo: %s", caminho)
            time.sleep(0.3)
        except Exception as e:
            log.error("Erro ao salvar arquivo %s: %s", caminho, e)
            return

    # Move para pasta de saída /nfse
    pasta_nfse = os.path.join(pasta_filial, "nfse")
    os.makedirs(pasta_nfse, exist_ok=True)
    destino = os.path.join(pasta_nfse, os.path.basename(caminho))

    for tentativa in range(5):
        try:
            shutil.move(caminho, destino)
            log.info("Movido para: %s", destino)
            registrar(codigo, nome_arquivo, "sucesso", f"Movido para {destino}")
            break
        except Exception as e:
            log.warning("Tentativa %d falhou ao mover %s: %s", tentativa + 1, caminho, e)
            time.sleep(1)
    else:
        log.error("Falha ao mover arquivo após 5 tentativas: %s", caminho)


# ======================
# VARREDURA PRINCIPAL
# ======================

def main():
    log.info("=" * 50)
    log.info("Iniciando processamento NFS-e")
    log.info("Base: %s", BASE)
    log.info("Filiais: %s", CODIGOS)
    log.info("=" * 50)

    total_processados = 0
    total_erros = 0

    for codigo in CODIGOS:
        pasta_filial = os.path.join(BASE, codigo)

        if not os.path.exists(pasta_filial):
            log.warning("Pasta não encontrada para filial %s: %s", codigo, pasta_filial)
            continue

        log.info("\nProcessando filial: %s", codigo)

        arquivos = [
            nome for nome in os.listdir(pasta_filial)
            if os.path.isfile(os.path.join(pasta_filial, nome))
            and nome.lower().endswith(".txt")
        ]

        log.info("Arquivos encontrados: %d", len(arquivos))

        for nome in arquivos:
            caminho = os.path.join(pasta_filial, nome)
            try:
                processar_arquivo(caminho, pasta_filial, codigo)
                total_processados += 1
            except Exception as e:
                log.error("Erro inesperado no arquivo %s: %s", caminho, e)
                total_erros += 1
                registrar(codigo, nome, "erro", str(e))
                alerta_erro(codigo, nome, str(e))

    log.info("=" * 50)
    log.info("Processamento finalizado.")
    log.info("Total processados: %d | Erros: %d", total_processados, total_erros)
    log.info("=" * 50)

    relatorio_execucao(
    total=total_processados,
    sucessos=total_processados - total_erros,
    erros=total_erros,
    filiais=CODIGOS
)
    
if __name__ == "__main__":
    main()
