@echo off
echo Iniciando processamento NFS-e...
cd /d "C:\Users\ARAUJODASILVAANDREY\Downloads\Projetos\automacoes-financeiras"
set PYTHONPATH=C:\Users\ARAUJODASILVAANDREY\Downloads\Projetos\automacoes-financeiras
python nfse_processor/processor.py
echo Finalizado.