"""
nfse_processor/db.py
=====================
Cria e gerencia o banco de dados SQLite para logging de execuções.
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "nfse.db")

def conectar():
    """Retorna uma conexão com o banco de dados."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def criar_tabelas():
    """Cria as tabelas se ainda não existirem."""
    with conectar() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS execucoes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                data_hora   TEXT    NOT NULL,
                filial      TEXT    NOT NULL,
                arquivo     TEXT    NOT NULL,
                status      TEXT    NOT NULL,
                mensagem    TEXT
            )
        """)
        conn.commit()

def registrar(filial: str, arquivo: str, status: str, mensagem: str = ""):
    """Grava um registro de execução no banco."""
    with conectar() as conn:
        conn.execute("""
            INSERT INTO execucoes (data_hora, filial, arquivo, status, mensagem)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), filial, arquivo, status, mensagem))
        conn.commit()

def buscar_execucoes(filial: str = None):
    """Retorna execuções registradas, opcionalmente filtradas por filial."""
    with conectar() as conn:
        if filial:
            cursor = conn.execute("""
                SELECT id, data_hora, filial, arquivo, status, mensagem
                FROM execucoes
                WHERE filial = ?
                ORDER BY data_hora DESC
            """, (filial,))
        else:
            cursor = conn.execute("""
                SELECT id, data_hora, filial, arquivo, status, mensagem
                FROM execucoes
                ORDER BY data_hora DESC
            """)
        return cursor.fetchall()

if __name__ == "__main__":
    criar_tabelas()
    print("Banco de dados pronto.")

    # Teste — insere registros fictícios para visualizar no DBeaver
    registrar("FILIAL_01", "nfse_001.txt", "sucesso", "2 alterações aplicadas")
    registrar("FILIAL_01", "nfse_002.txt", "sucesso", "sem alterações")
    registrar("FILIAL_02", "nfse_003.txt", "erro", "arquivo vazio ignorado")
    registrar("FILIAL_02", "nfse_004.txt", "sucesso", "3 alterações aplicadas")

    print("Registros de teste inseridos.")

    # Mostra os registros
    print("\nTodos os registros:")
    for row in buscar_execucoes():
        print(row)