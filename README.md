# Gerenciador de CSVs para SQL Server 🚀

Cansado de importar CSVs para o SQL Server manualmente? Esta ferramenta automatiza o processo de importação (`ship`) e deleção (`dump`) de tabelas com base em arquivos CSV.

## Funcionalidades ✨

### 🚀 Importar Dados (`run_ship.py`)
-   **Detecção Automática:** Encontra o `encoding` e o separador (`,` ou `;`) sozinho.
-   **Criação de Tabelas:** Se a tabela não existe, ela é criada. Todas as colunas viram `NVARCHAR(MAX)` para evitar erros de tipo.
-   **Performance:** Processa arquivos gigantes em `chunks` sem travar.
-   **Logs Detalhados:** Tudo o que acontece fica registrado na pasta `/logs`.

### 🗑️ Deletar Tabelas (`dump/run_dump.py`)
-   **Deleção por Nome:** Usa os nomes dos arquivos `.csv` para saber quais tabelas apagar.
-   **Modo Simulação (`--dry-run`):** Veja o que *seria* deletado sem nenhum risco.
-   **Confirmação Obrigatória:** Pede sua permissão antes de executar um `DROP TABLE` para evitar acidentes.

## Configuração Rápida

**1. Pré-requisitos:**
   - Python 3.x e o [ODBC Driver for SQL Server](https://docs.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server).
   - Instale as bibliotecas:
     ```bash
     pip install pyodbc pandas chardet python-dotenv
     ```

**2. Crie o arquivo `.env`:**
   Na raiz do projeto, crie um arquivo `.env` com suas credenciais.

   ```env
   # Configurações do Banco de Dados
   DB_SERVER=SEU_SERVIDOR_SQL
   DB_NAME=SEU_BANCO_DE_DADOS
   DB_SCHEMA=dbo

   # Deixe em branco para usar Autenticação do Windows
   DB_USER=SEU_USUARIO_SQL
   DB_PASSWORD=SUA_SENHA_SQL

   Dica: Para simular a deleção sem riscos, use python dump/csv_dump.py --dry-run.

Dicas Importantes
Nomes: Nomes de arquivos e colunas com espaços ou caracteres especiais são "limpos" e têm esses caracteres trocados por _.
Permissões: O usuário do banco precisa de permissão para CREATE, INSERT, TRUNCATE e DROP tabelas.
Tipos de Dados: Lembre-se que todas as colunas são criadas como NVARCHAR(MAX). Ajuste os tipos no SQL Server depois, se precisar de otimização.
Estrutura do Projeto
.
├── csv/                      # Pasta para seus CSVs
├── dump/
│   ├── csv_dump.py           # Lógica de deleção
│   └── run_dump.py           # Script para EXECUTAR a deleção
├── logs/                     # Logs de execução
├── .env                      # Suas configurações de conexão
├── csv_ship.py               # Lógica de importação
├── run_ship.py               # Script para EXECUTAR a importação
└── README.md                 # Este arquivo