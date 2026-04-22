"""
nfse_processor/sap_fbl5n.py
============================
Pipeline de processamento de export FBL5N do SAP.

Problema resolvido:
    O relatório FBL5N é exportado manualmente do SAP e tratado
    no Excel por analistas. Este script automatiza o processamento
    gerando relatórios de vencimentos, inadimplência e aging list
    prontos para análise ou envio.

Autor: Andrey Araujo
Versão: 1.0.0
"""

import pandas as pd
import os
import logging
from datetime import datetime

# ======================
# LOGGING
# ======================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            f"logs/fbl5n_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ======================
# CONFIGURAÇÃO
# ======================

ARQUIVO_ENTRADA = os.environ.get("FBL5N_PATH", "FBL5N.xlsx")
PASTA_SAIDA     = "relatorios"

COLUNAS = {
    "Símb.prtds.em aberto/comp"  : "status",
    "Data do documento"          : "data_documento",
    "Divisão"                    : "divisao",
    "Nº documento"               : "numero_documento",
    "Chave referência 3"         : "chave_referencia_3",
    "Referência de pagamento"    : "referencia_pagamento",
    "Data de pagamento"          : "data_pagamento",
    "Vencimento líquido"         : "vencimento",
    "Tipo de documento"          : "tipo_documento",
    "Símbolo de vencimento líquido": "simbolo_vencimento",
    "Montante em moeda interna"  : "valor",
    "Referência"                 : "referencia",
    "Cód.Razão Especial"         : "cod_razao_especial",
    "Atribuição"                 : "atribuicao",
    "Doc.compensação"            : "doc_compensacao",
    "Data de compensação"        : "data_compensacao",
    "Conta"                      : "conta",
    "Texto"                      : "texto"
}

# ======================
# LEITURA
# ======================

def ler_fbl5n(caminho: str) -> pd.DataFrame:
    """Lê o export FBL5N do SAP e normaliza as colunas."""
    log.info("Lendo arquivo: %s", caminho)

    df = pd.read_excel(caminho, dtype=str)

    # Renomeia colunas para nomes amigáveis
    df = df.rename(columns=COLUNAS)

    # Converte datas
    for col in ["data_documento", "vencimento", "data_pagamento", "data_compensacao"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    # Converte valor para numérico
    if "valor" in df.columns:
        df["valor"] = (
            df["valor"]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    log.info("Registros carregados: %d", len(df))
    return df

# ======================
# ANÁLISES
# ======================

def aging_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera aging list — classifica partidas em aberto por faixa de atraso.
    Faixas: A vencer / 1-30 / 31-60 / 61-90 / 90+ dias
    """
    hoje = pd.Timestamp.today().normalize()

    # Filtra apenas partidas em aberto
    em_aberto = df[df["doc_compensacao"].isna() | (df["doc_compensacao"] == "")].copy()

    def faixa(row):
        if pd.isna(row["vencimento"]):
            return "Sem vencimento"
        dias = (hoje - row["vencimento"]).days
        if dias <= 0:
            return "A vencer"
        elif dias <= 30:
            return "1-30 dias"
        elif dias <= 60:
            return "31-60 dias"
        elif dias <= 90:
            return "61-90 dias"
        else:
            return "90+ dias"

    em_aberto["faixa_atraso"] = em_aberto.apply(faixa, axis=1)
    return em_aberto


def resumo_por_conta(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa valores em aberto por conta."""
    em_aberto = df[df["doc_compensacao"].isna() | (df["doc_compensacao"] == "")]
    return (
        em_aberto
        .groupby("conta")["valor"]
        .agg(total="sum", quantidade="count")
        .reset_index()
        .sort_values("total", ascending=False)
    )


def resumo_aging(df_aging: pd.DataFrame) -> pd.DataFrame:
    """Resume aging list por faixa de atraso."""
    return (
        df_aging
        .groupby("faixa_atraso")["valor"]
        .agg(total="sum", quantidade="count")
        .reset_index()
        .sort_values("total", ascending=False)
    )

# ======================
# EXPORTAÇÃO
# ======================

def exportar_relatorios(df: pd.DataFrame) -> None:
    """Exporta todos os relatórios para Excel com múltiplas abas."""
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    aging        = aging_list(df)
    res_conta    = resumo_por_conta(df)
    res_aging    = resumo_aging(aging)

    nome_arquivo = f"{PASTA_SAIDA}/fbl5n_processado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as writer:
        df.to_excel(writer,         sheet_name="Dados Completos", index=False)
        aging.to_excel(writer,      sheet_name="Aging List",      index=False)
        res_conta.to_excel(writer,  sheet_name="Por Conta",       index=False)
        res_aging.to_excel(writer,  sheet_name="Resumo Aging",    index=False)

    log.info("Relatorio exportado: %s", nome_arquivo)
    print(f"\n✅ Relatorio gerado: {nome_arquivo}")

# ======================
# MAIN
# ======================

def main():
    log.info("=" * 50)
    log.info("Iniciando pipeline FBL5N")
    log.info("=" * 50)

    if not os.path.exists(ARQUIVO_ENTRADA):
        log.error("Arquivo nao encontrado: %s", ARQUIVO_ENTRADA)
        return

    df = ler_fbl5n(ARQUIVO_ENTRADA)

    log.info("Gerando relatorios...")
    exportar_relatorios(df)

    # Preview no terminal
    print("\n📊 Resumo Aging:")
    aging = aging_list(df)
    print(resumo_aging(aging).to_string(index=False))

    print("\n📊 Top 5 contas por valor em aberto:")
    print(resumo_por_conta(df).head(5).to_string(index=False))

    log.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()