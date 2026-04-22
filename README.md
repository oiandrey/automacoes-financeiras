# Automações de Processos Financeiros

Repositório de scripts e automações voltados para processos financeiros corporativos — faturamento, NFS-e, integração SAP e Power Automate.

## 🗂️ Estrutura

```
automacoes-financeiras/
│
├── nfse_processor/
│   └── processor.py       # Processador automatizado de arquivos NFS-e
│
└── README.md
```

---

## 📄 nfse_processor

### Problema resolvido
Arquivos de NFS-e gerados pelo ERP continham inconsistências específicas por município, causando rejeição nas prefeituras e bloqueando o faturamento das filiais.

### Solução
Script Python que vasculha automaticamente as pastas de cada filial, aplica as correções necessárias por código de município (regras globais + regras específicas) e move os arquivos para a fila de envio — sem intervenção manual.

### Destaques técnicos
- Leitura com detecção automática de encoding (UTF-8, CP1252, Latin-1)
- Arquitetura de regras extensível: fácil adicionar novos municípios
- Logging completo com arquivo de log por execução
- Tratamento de erros com retry na movimentação de arquivos
- Configuração via variável de ambiente (sem caminhos hardcodados)

### Como usar

**1. Configure a variável de ambiente com o caminho base:**
```bash
# Windows
set NFSE_BASE_PATH=\\servidor\pasta\entradas

# Linux/Mac
export NFSE_BASE_PATH=/mnt/entradas
```

**2. Configure os códigos de filial em `processor.py`:**
```python
CODIGOS = ["FILIAL_01", "FILIAL_02"]
```

**3. Execute:**
```bash
python nfse_processor/processor.py
```

Os logs serão gerados automaticamente na pasta `logs/`.

### Requisitos
```
Python 3.8+
Nenhuma dependência externa (apenas bibliotecas padrão)
```

---

## 🛠️ Stack

- Python 3.x
- Power Automate
- Power BI
- SAP FI
- UiPath

---

## 👤 Autor

Andrey Araujo  
Especialista em automação de processos financeiros | SAP FI · Power Platform · Python · RPA
