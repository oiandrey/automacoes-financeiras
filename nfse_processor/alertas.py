"""
nfse_processor/alertas.py
==========================
Módulo de alertas por e-mail para notificar erros e resumo de execução.
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA = os.getenv("EMAIL_SENHA")
DESTINATARIO = os.getenv("EMAIL_DESTINATARIO")


def enviar_email(assunto: str, corpo: str) -> bool:
    """Envia e-mail de alerta. Retorna True se enviou, False se falhou."""
    try:
        msg = MIMEMultipart()
        msg["From"] = REMETENTE
        msg["To"] = DESTINATARIO
        msg["Subject"] = assunto
        msg.attach(MIMEText(corpo, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(REMETENTE, SENHA)
            smtp.sendmail(REMETENTE, DESTINATARIO, msg.as_string())

        print(f"E-mail enviado: {assunto}")
        return True

    except Exception as e:
        print(f"Falha ao enviar e-mail: {e}")
        return False


def alerta_erro(filial: str, arquivo: str, erro: str) -> None:
    """Envia alerta imediato quando um arquivo falha."""
    assunto = f"[NFSE] Erro na filial {filial} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    corpo = f"""
    <h2 style="color:red">Erro no Processador NFS-e</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse">
        <tr><td><b>Filial</b></td><td>{filial}</td></tr>
        <tr><td><b>Arquivo</b></td><td>{arquivo}</td></tr>
        <tr><td><b>Erro</b></td><td>{erro}</td></tr>
        <tr><td><b>Data/Hora</b></td><td>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</td></tr>
    </table>
    <p>Verifique o arquivo de log para mais detalhes.</p>
    """
    enviar_email(assunto, corpo)


def relatorio_execucao(total: int, sucessos: int, erros: int, filiais: list) -> None:
    """Envia resumo ao final de cada execução do script."""
    status_geral = "✅ OK" if erros == 0 else "⚠️ COM ERROS"
    assunto = f"[NFSE] Relatorio de execucao - {status_geral} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    cor = "#28a745" if erros == 0 else "#dc3545"
    corpo = f"""
    <h2 style="color:{cor}">Relatorio de Execucao NFS-e</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse">
        <tr><td><b>Data/Hora</b></td><td>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</td></tr>
        <tr><td><b>Filiais processadas</b></td><td>{", ".join(filiais)}</td></tr>
        <tr><td><b>Total de arquivos</b></td><td>{total}</td></tr>
        <tr><td><b>Sucessos</b></td><td style="color:#28a745">{sucessos}</td></tr>
        <tr><td><b>Erros</b></td><td style="color:#dc3545">{erros}</td></tr>
    </table>
    """
    enviar_email(assunto, corpo)


if __name__ == "__main__":
    print("Testando envio de e-mail...")
    alerta_erro("FILIAL_01", "nfse_teste.txt", "Arquivo vazio ignorado")