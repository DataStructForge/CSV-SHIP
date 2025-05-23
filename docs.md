# Documentação do Script `deleter.py`

## 1. Propósito

O script `deleter.py` é uma ferramenta de linha de comando desenvolvida em Python para deletar tabelas em um banco de dados SQL Server. A identificação das tabelas a serem deletadas é baseada nos nomes dos arquivos `.csv` presentes em um diretório especificado. Para cada arquivo CSV encontrado, o script tenta deletar uma tabela no banco de dados cujo nome é derivado do nome do arquivo CSV (sem a extensão).

Este script é útil para limpar tabelas que foram previamente criadas por um processo de importação que também utiliza nomes de arquivos CSV para nomear tabelas, como por exemplo, o script `importer.py`.

## 2. Funcionalidades Principais

*   **Deleção de Tabelas Baseada em Nomes de Arquivos CSV:** Varre um diretório em busca de arquivos `.csv` e tenta deletar as tabelas SQL Server correspondentes.
*   **Suporte a Esquemas (Schemas):** Permite especificar um esquema de banco de dados para direcionar as operações de `DROP TABLE`. Se nenhum esquema for especificado, utiliza um esquema padrão.
*   **Conexão Configurável com SQL Server:**
    *   Suporte para autenticação via usuário/senha do SQL Server.
    *   Suporte para Autenticação do Windows (`Trusted_Connection`).
    *   As credenciais e detalhes do servidor podem ser definidos como constantes globais no script ou fornecidos via argumentos de linha de comando.
*   **Logging Detalhado:** Registra todas as operações importantes, tentativas de conexão, erros e tabelas processadas em um arquivo de log e também no console. Os arquivos de log são armazenados no diretório `logs/` com rotação baseada em tamanho.
*   **Sanitização de Nomes:** Nomes de tabelas derivados de arquivos CSV são sanitizados (caracteres não alfanuméricos são substituídos por `_`) para garantir compatibilidade com SQL.
*   **Modo "Dry Run":** Permite simular o processo de deleção, listando quais tabelas seriam deletadas sem executar de fato o comando `DROP TABLE`. Isso é útil para verificação antes de realizar alterações destrutivas.
*   **Interface de Linha de Comando (CLI):** Utiliza `argparse` para fornecer uma interface flexível para configurar o comportamento do script em tempo de execução.

## 3. Pré-requisitos

*   **Python:** Versão 3.6 ou superior.
*   **Bibliotecas Python:**
    *   `pyodbc`: Para conectar ao SQL Server.
    *   `glob` (padrão do Python): Para encontrar arquivos.
    *   `os` (padrão do Python): Para operações de sistema de arquivos.
    *   `logging` (padrão do Python): Para logging.
    *   `datetime` (padrão do Python): Para carimbos de data/hora nos logs.
    *   `argparse` (padrão do Python): Para parsing de argumentos da CLI.
*   **SQL Server:** Uma instância do SQL Server acessível.
*   **Driver ODBC para SQL Server:** O "ODBC Driver 17 for SQL Server" (ou compatível) deve estar instalado na máquina onde o script será executado. O script está configurado para usar este driver especificamente.

## 4. Configuração

O script pode ser configurado de duas maneiras principais:

### 4.1. Constantes Globais no Script

No início do arquivo `deleter.py`, você encontrará seções para configurações padrão:

```python
# --- CONFIGURAÇÕES DO BANCO DE DADOS (Padrões Globais) ---
DB_SERVER = "SEU_SERVIDOR"
DB_NAME = "SEU_BANCO_DE_DADOS"
DB_USER = "SEU_USUARIO" # Opcional se usar Trusted Connection
DB_PASSWORD = "SUA_SENHA" # Opcional se usar Trusted Connection
DB_SCHEMA = "dbo"  # Esquema padrão

# --- CONFIGURAÇÕES DOS ARQUIVOS (Padrões Globais) ---
CSV_DIRECTORY = "csv" # Diretório padrão para buscar arquivos CSV
```

*   `DB_SERVER`: Nome ou endereço do servidor SQL Server.
*   `DB_NAME`: Nome do banco de dados.
*   `DB_USER`: Nome de usuário para autenticação SQL (deixe como está ou vazio se usar Autenticação do Windows por padrão).
*   `DB_PASSWORD`: Senha para autenticação SQL (deixe como está ou vazia se usar Autenticação do Windows por padrão).
*   `DB_SCHEMA`: O esquema padrão do SQL Server onde as tabelas serão procuradas para deleção (ex: `dbo`).
*   `CSV_DIRECTORY`: O caminho relativo ou absoluto para o diretório que contém os arquivos CSV.

**Nota:** Se `DB_USER` e `DB_PASSWORD` não forem fornecidos (ou mantidos com os valores placeholder "SEU_USUARIO"/"SUA_SENHA") e a Autenticação do Windows não for explicitamente solicitada via CLI, o script tentará usar a Autenticação do Windows por padrão.

### 4.2. Argumentos de Linha de Comando

Os argumentos de linha de comando têm precedência sobre as constantes globais definidas no script.

*   `--csv-dir TEXT`: Diretório contendo os arquivos CSV. (Padrão: o valor de `CSV_DIRECTORY`)
*   `--db-server TEXT`: Nome ou endereço do servidor SQL. (Padrão: o valor de `DB_SERVER`)
*   `--db-name TEXT`: Nome do banco de dados SQL. (Padrão: o valor de `DB_NAME`)
*   `--db-user TEXT`: Usuário para autenticação SQL Server. (Padrão: o valor de `DB_USER`)
*   `--db-password TEXT`: Senha para autenticação SQL Server. (Padrão: o valor de `DB_PASSWORD`)
*   `--db-schema TEXT`: Nome do esquema do banco de dados onde as tabelas estão localizadas. (Padrão: o valor de `DB_SCHEMA`)
*   `--trusted-connection`: Usar Autenticação do Windows. Se especificado, ignora `--db-user` e `--db-password`.
*   `--dry-run`: Listar tabelas que seriam deletadas, mas não executar o comando `DROP TABLE`.

## 5. Logging

O script utiliza o módulo `logging` do Python para registrar informações sobre sua execução.

*   **Nível de Log:** Configurado para `INFO` por padrão, capturando eventos importantes, sucessos e erros.
*   **Formato do Log:** `%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s`
*   **Saídas (Handlers):**
    *   `StreamHandler`: Envia logs para o console (saída padrão).
    *   `RotatingFileHandler`: Salva logs em arquivos no diretório `logs/`.
        *   O nome do arquivo de log é prefixado com `delete_tables_process_` seguido da data atual (ex: `delete_tables_process_2023-10-27.log`).
        *   Rotação de arquivo: Cria um novo arquivo de log quando o atual atinge 5MB, mantendo até 2 arquivos de backup.

O diretório `logs` é criado automaticamente se não existir.

## 6. Como Funciona

1.  **Inicialização e Parsing de Argumentos:** O script inicializa o logging e analisa os argumentos fornecidos na linha de comando.
2.  **Determinação das Configurações:** As configurações finais para diretório de CSV, detalhes do servidor, banco de dados, esquema e autenticação são determinadas, com os argumentos da CLI tendo prioridade sobre as constantes globais.
3.  **Conexão com SQL Server:**
    *   A função `get_sql_server_connection` tenta estabelecer uma conexão.
    *   Se `--trusted-connection` for usado, ou se nenhum usuário/senha for fornecido (nem via CLI nem nas constantes globais), tenta-se a Autenticação do Windows.
    *   Caso contrário, utiliza o usuário e senha fornecidos.
    *   Erros de conexão são logados e o script é abortado se a conexão falhar.
4.  **Busca por Arquivos CSV:**
    *   O script usa `glob.glob` para encontrar todos os arquivos com a extensão `.csv` no `current_csv_directory` especificado.
    *   Se nenhum arquivo CSV for encontrado, uma mensagem de aviso é logada e o script termina.
5.  **Derivação dos Nomes das Tabelas:**
    *   Para cada arquivo CSV encontrado (ex: `meu_arquivo-dados.csv`), o nome base do arquivo é extraído (ex: `meu_arquivo-dados`).
    *   Este nome base é então sanitizado:
        *   Caracteres não alfanuméricos são substituídos por `_`.
        *   Hífens (`-`) são especificamente substituídos por `_`.
    *   O resultado é o nome da tabela alvo (ex: `meu_arquivo_dados`).
6.  **Modo "Dry Run":**
    *   Se o argumento `--dry-run` for fornecido, o script listará todos os arquivos CSV encontrados e os nomes das tabelas correspondentes que *seriam* deletadas.
    *   Nenhuma operação `DROP TABLE` é executada. O script então se encerra.
7.  **Deleção de Tabelas (se não for Dry Run):**
    *   Para cada nome de tabela derivado:
        *   A função `delete_sql_table` é chamada.
        *   Esta função constrói o nome completo da tabela, incluindo o esquema (ex: `[meu_esquema].[minha_tabela_sanitizada]`).
        *   Um comando `DROP TABLE IF EXISTS nome_completo_da_tabela` é executado.
            *   `IF EXISTS` garante que o script não falhe se a tabela não existir.
        *   O sucesso ou falha da operação de `DROP` é logado. Em caso de falha, um rollback é tentado na transação atual para aquela operação.
8.  **Encerramento:**
    *   A conexão com o banco de dados é fechada.
    *   Um resumo das operações é logado, informando quantas tabelas foram alvo de deleção e quantas falhas ocorreram.

## 7. Uso

O script é executado a partir da linha de comando.

### 7.1. Sintaxe Básica

```bash
python deleter.py [OPÇÕES]
```

### 7.2. Exemplos

**Exemplo 1: Deletar tabelas usando configurações globais e Autenticação do Windows (se configurada como padrão):**

```bash
python deleter.py
```
*(Assume que `DB_USER` e `DB_PASSWORD` nas constantes globais estão vazios ou são os placeholders, ou que `--trusted-connection` seria o fallback natural).*

**Exemplo 2: Deletar tabelas especificando o servidor, banco de dados, esquema e usando Autenticação do Windows:**

```bash
python deleter.py --db-server "MEU_SERVIDOR_SQL" --db-name "MeuBanco" --db-schema "data_stage" --trusted-connection
```

**Exemplo 3: Deletar tabelas usando usuário/senha SQL, especificando o diretório de CSVs e um esquema diferente:**

```bash
python deleter.py --csv-dir "/caminho/para/meus/csvs" --db-server "192.168.1.100" --db-name "Producao" --db-user "sa" --db-password "SenhaSuperSegura" --db-schema "raw"
```

**Exemplo 4: Realizar um "Dry Run" para ver quais tabelas seriam deletadas no esquema 'testes':**

```bash
python deleter.py --db-schema "testes" --dry-run
```
*(Irá conectar ao banco configurado, mas apenas listará as tabelas alvo do esquema 'testes' sem deletá-las).*

**Exemplo 5: Deletar tabelas de um diretório específico, usando Autenticação do Windows implícita (assumindo que as variáveis `DB_USER` e `DB_PASSWORD` no script estão com os valores padrão "SEU_USUARIO" e "SUA_SENHA" ou vazias):**

```bash
python deleter.py --csv-dir "arquivos_para_deletar"
```

## 8. Notas Importantes e Considerações

*   **PERMISSÕES NO SQL SERVER:** O usuário do banco de dados (seja o usuário da Autenticação do Windows ou o usuário SQL Server especificado) DEVE ter as permissões necessárias para executar `DROP TABLE` nas tabelas alvo e no esquema especificado.
*   **OPERAÇÃO DESTRUTIVA:** A deleção de tabelas é uma operação destrutiva e irreversível. Use o modo `--dry-run` para verificar as tabelas alvo antes de executar o script em modo de deleção real. Faça backups do seu banco de dados regularmente.
*   **SANITIZAÇÃO DE NOMES:** O script tenta sanitizar os nomes das tabelas. Se você tiver uma convenção de nomenclatura muito complexa para os arquivos CSV que não se traduz bem após a sanitização, as tabelas correspondentes podem não ser encontradas ou nomes incorretos podem ser gerados.
*   **NÃO INTERAGE COM CONTEÚDO CSV:** Este script usa os arquivos CSV *apenas* para derivar os nomes das tabelas a serem deletadas. Ele não lê o conteúdo dos arquivos CSV.
*   **DRIVER ODBC:** O script está codificado para usar `DRIVER={ODBC Driver 17 for SQL Server}`. Se você precisar usar um driver diferente, esta string de conexão precisará ser modificada na função `get_sql_server_connection`.
*   **Tratamento de Erros:** O script tenta capturar e logar erros comuns, como falhas de conexão ou erros durante a execução do `DROP TABLE`. Verifique os logs para detalhes em caso de problemas.

# Documentação do Script `importer.py`

## 1. Propósito

O script `importer.py` é uma ferramenta de linha de comando desenvolvida em Python para importar dados de arquivos CSV para um banco de dados SQL Server. Para cada arquivo `.csv` encontrado em um diretório especificado, o script pode:
1.  Criar uma nova tabela no SQL Server (se ela não existir) com uma estrutura derivada das colunas do CSV. O nome da tabela é derivado do nome do arquivo CSV.
2.  Inserir os dados do arquivo CSV na tabela correspondente.

O script é projetado para ser robusto, lidando com arquivos CSV grandes através do processamento em *chunks* (pedaços) e oferecendo flexibilidade na configuração da conexão com o banco de dados e no comportamento da importação.

## 2. Funcionalidades Principais

*   **Importação de Dados de CSV para SQL Server:** Lê arquivos CSV e insere seus dados em tabelas do SQL Server.
*   **Criação Dinâmica de Tabelas:**
    *   Se uma tabela correspondente ao nome do arquivo CSV não existir, o script a cria.
    *   Os nomes das colunas da tabela são derivados dos cabeçalhos do arquivo CSV.
    *   Todos os tipos de coluna são criados como `NVARCHAR(MAX)` por padrão para simplificar a importação e evitar erros de tipo de dados.
*   **Sanitização de Nomes:** Nomes de tabelas e colunas derivados de arquivos CSV são sanitizados (caracteres não alfanuméricos são substituídos por `_`) para garantir compatibilidade com SQL.
*   **Suporte a Esquemas (Schemas):**
    *   Permite especificar um esquema de banco de dados para a criação e inserção nas tabelas.
    *   Pode tentar criar o esquema se ele não existir (requer permissões adequadas).
    *   Se nenhum esquema for especificado, utiliza um esquema padrão (`dbo` ou configurável).
*   **Processamento em Chunks:** Lê e insere dados de arquivos CSV grandes em pedaços (chunks) para otimizar o uso de memória e lidar com grandes volumes de dados.
*   **Detecção de Encoding de Arquivo:** Tenta detectar automaticamente o encoding dos arquivos CSV usando a biblioteca `chardet`. Oferece fallback para UTF-8 e, em último caso, para `latin1` se a detecção falhar ou a leitura inicial falhar.
*   **Conexão Configurável com SQL Server:**
    *   Suporte para autenticação via usuário/senha do SQL Server.
    *   Suporte para Autenticação do Windows (`Trusted_Connection`).
    *   As credenciais e detalhes do servidor podem ser definidos como constantes globais no script ou fornecidos via argumentos de linha de comando.
*   **Opção para Truncar Tabelas Existentes:** Permite truncar tabelas existentes antes de inserir novos dados, útil para recargas completas.
*   **Logging Detalhado:** Registra todas as operações importantes, tentativas de conexão, criação de tabelas, progresso da inserção, erros e arquivos processados em um arquivo de log e também no console. Os arquivos de log são armazenados no diretório `logs/` com rotação baseada em tamanho.
*   **Interface de Linha de Comando (CLI):** Utiliza `argparse` para fornecer uma interface flexível para configurar o comportamento do script em tempo de execução.

## 3. Pré-requisitos

*   **Python:** Versão 3.6 ou superior.
*   **Bibliotecas Python:**
    *   `pyodbc`: Para conectar ao SQL Server.
    *   `pandas`: Para leitura e processamento eficiente de arquivos CSV.
    *   `chardet`: Para detecção de encoding de arquivos.
    *   `glob` (padrão do Python): Para encontrar arquivos.
    *   `os` (padrão do Python): Para operações de sistema de arquivos.
    *   `logging` (padrão do Python): Para logging.
    *   `datetime` (padrão do Python): Para carimbos de data/hora nos logs.
    *   `argparse` (padrão do Python): Para parsing de argumentos da CLI.
*   **SQL Server:** Uma instância do SQL Server acessível.
*   **Driver ODBC para SQL Server:** O "ODBC Driver 17 for SQL Server" (ou compatível) deve estar instalado na máquina onde o script será executado. O script está configurado para usar este driver especificamente.

## 4. Configuração

O script pode ser configurado de duas maneiras principais:

### 4.1. Constantes Globais no Script

No início do arquivo `importer.py`, você encontrará seções para configurações padrão:

```python
# --- Configurações do Logging (Exemplo) ---
LOG_DIR = 'logs'
# ... (outras configs de log) ...

# --- CONFIGURAÇÕES DO BANCO DE DADOS (Padrões Globais) ---
DB_SERVER = "SEU_SERVIDOR"
DB_NAME = "SEU_BANCO_DE_DADOS"
DB_USER = "SEU_USUARIO" # Opcional se usar Trusted Connection
DB_PASSWORD = "SUA_SENHA" # Opcional se usar Trusted Connection
DB_SCHEMA = "dbo"  # Esquema padrão

# --- CONFIGURAÇÕES DOS ARQUIVOS (Padrões Globais) ---
CSV_DIRECTORY = "csv" # Diretório padrão para buscar arquivos CSV
```

*   `DB_SERVER`: Nome ou endereço do servidor SQL Server.
*   `DB_NAME`: Nome do banco de dados.
*   `DB_USER`: Nome de usuário para autenticação SQL (deixe como está ou vazio se usar Autenticação do Windows por padrão).
*   `DB_PASSWORD`: Senha para autenticação SQL (deixe como está ou vazia se usar Autenticação do Windows por padrão).
*   `DB_SCHEMA`: O esquema padrão do SQL Server onde as tabelas serão criadas/gerenciadas (ex: `dbo`).
*   `CSV_DIRECTORY`: O caminho relativo ou absoluto para o diretório que contém os arquivos CSV a serem importados.

**Nota:** Se `DB_USER` e `DB_PASSWORD` não forem fornecidos (ou mantidos com os valores placeholder "SEU_USUARIO"/"SUA_SENHA") e a Autenticação do Windows não for explicitamente solicitada via CLI, o script tentará usar a Autenticação do Windows por padrão.

### 4.2. Argumentos de Linha de Comando

Os argumentos de linha de comando têm precedência sobre as constantes globais definidas no script.

*   `--csv-dir TEXT`: Diretório contendo os arquivos CSV. (Padrão: o valor de `CSV_DIRECTORY`)
*   `--db-server TEXT`: Nome ou endereço do servidor SQL. (Padrão: o valor de `DB_SERVER`)
*   `--db-name TEXT`: Nome do banco de dados SQL. (Padrão: o valor de `DB_NAME`)
*   `--db-user TEXT`: Usuário para autenticação SQL Server. (Padrão: o valor de `DB_USER`)
*   `--db-password TEXT`: Senha para autenticação SQL Server. (Padrão: o valor de `DB_PASSWORD`)
*   `--db-schema TEXT`: Nome do esquema do banco de dados. (Padrão: o valor de `DB_SCHEMA`)
*   `--trusted-connection`: Usar Autenticação do Windows. Se especificado, ignora `--db-user` e `--db-password`.
*   `--truncate`: Se especificado, as tabelas existentes serão truncadas antes da inserção de novos dados. (Padrão: Não truncar).

## 5. Logging

O script utiliza o módulo `logging` do Python para registrar informações sobre sua execução.

*   **Nível de Log:** Configurado para `INFO` por padrão, capturando eventos importantes, sucessos e erros.
*   **Formato do Log:** `%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s`
*   **Saídas (Handlers):**
    *   `StreamHandler`: Envia logs para o console (saída padrão).
    *   `RotatingFileHandler`: Salva logs em arquivos no diretório `logs/`.
        *   O nome do arquivo de log é prefixado com `upload_csv_` seguido da data atual (ex: `upload_csv_2023-10-27.log`).
        *   Rotação de arquivo: Cria um novo arquivo de log quando o atual atinge 5MB, mantendo até 2 arquivos de backup.

O diretório `logs` é criado automaticamente se não existir.

## 6. Como Funciona

1.  **Inicialização e Parsing de Argumentos:** O script inicializa o logging e analisa os argumentos fornecidos na linha de comando.
2.  **Determinação das Configurações:** As configurações finais para diretório de CSV, detalhes do servidor, banco de dados, esquema, autenticação e opção de truncar são determinadas, com os argumentos da CLI tendo prioridade sobre as constantes globais.
3.  **Conexão com SQL Server:**
    *   A função `get_sql_server_connection` tenta estabelecer uma conexão.
    *   Se `--trusted-connection` for usado, ou se nenhum usuário/senha for fornecido, tenta-se a Autenticação do Windows.
    *   Caso contrário, utiliza o usuário e senha fornecidos.
    *   Erros de conexão são logados e o script é abortado se a conexão falhar.
4.  **Busca por Arquivos CSV:**
    *   O script usa `glob.glob` para encontrar todos os arquivos com a extensão `.csv` no `current_csv_directory` especificado.
    *   Se nenhum arquivo CSV for encontrado, uma mensagem de aviso é logada e o script termina.
5.  **Processamento de Cada Arquivo CSV:** Para cada arquivo encontrado:
    *   **Nome da Tabela:** O nome da tabela de destino é derivado do nome do arquivo CSV (sem a extensão) e sanitizado (caracteres não alfanuméricos e hífens são substituídos por `_`).
    *   **Detecção de Encoding:** A função `detect_encoding` é chamada para determinar o encoding do arquivo.
    *   **Leitura do Primeiro Chunk:** Um pequeno chunk inicial do CSV é lido para:
        *   Verificar se o arquivo não está vazio.
        *   Obter os nomes das colunas (cabeçalhos) para a criação da tabela.
        *   Testar o encoding detectado. Se `UnicodeDecodeError` ocorrer, tenta-se com `latin1` como fallback.
    *   **Criação da Tabela (Função `create_table_from_csv`):**
        *   Verifica se a tabela já existe no esquema especificado.
        *   Se existir e a opção `--truncate` estiver ativa, a tabela é truncada.
        *   Se não existir, uma instrução `CREATE TABLE` é gerada. Todas as colunas são definidas como `NVARCHAR(MAX)`. Os nomes das colunas são sanitizados.
        *   O script tenta criar o esquema se ele não existir (requer permissões).
        *   A tabela é criada.
    *   **Inserção de Dados (Função `insert_data_from_csv`):**
        *   Se a tabela foi criada ou já existia (e opcionalmente truncada), o script começa a inserir os dados.
        *   O arquivo CSV é lido em chunks (o tamanho do chunk é 10000 linhas por padrão).
        *   Para cada chunk:
            *   Os nomes das colunas no chunk são sanitizados.
            *   Uma instrução `INSERT INTO` parametrizada é preparada.
            *   Os dados do chunk são convertidos para uma lista de tuplas, com valores `NaN` do Pandas convertidos para `None` (SQL NULL) e outros valores para strings.
            *   `cursor.executemany()` com `fast_executemany = True` é usado para inserção eficiente em lote.
            *   `conn.commit()` é chamado após cada chunk bem-sucedido.
            *   Erros durante a inserção de um chunk são logados, um `conn.rollback()` é tentado, e a inserção para aquele arquivo CSV é interrompida.
    *   Erros durante o processamento de um arquivo (ex: arquivo vazio, falha na criação da tabela) são logados, e o script passa para o próximo arquivo.
6.  **Encerramento:**
    *   Após processar todos os arquivos, a conexão com o banco de dados é fechada.
    *   Uma mensagem final de conclusão do processo é logada.

## 7. Uso

O script é executado a partir da linha de comando.

### 7.1. Sintaxe Básica

```bash
python importer.py [OPÇÕES]
```

### 7.2. Exemplos

**Exemplo 1: Importar CSVs usando configurações globais e Autenticação do Windows (se configurada como padrão):**

```bash
python importer.py
```

**Exemplo 2: Importar CSVs especificando servidor, banco, esquema e usando Autenticação do Windows, truncando tabelas existentes:**

```bash
python importer.py --db-server "MEU_SERVIDOR_SQL" --db-name "MeuBanco" --db-schema "staging" --trusted-connection --truncate
```

**Exemplo 3: Importar CSVs usando usuário/senha SQL, especificando o diretório de CSVs e um esquema diferente:**

```bash
python importer.py --csv-dir "/caminho/para/meus/csvs" --db-server "192.168.1.100" --db-name "Producao" --db-user "importer_user" --db-password "ImportPass123" --db-schema "raw_data"
```

## 8. Notas Importantes e Considerações

*   **PERMISSÕES NO SQL SERVER:** O usuário do banco de dados (seja o usuário da Autenticação do Windows ou o usuário SQL Server especificado) DEVE ter as permissões necessárias para:
    *   `CREATE TABLE` no esquema especificado.
    *   `CREATE SCHEMA` se o esquema não existir e o script tentar criá-lo.
    *   `INSERT` nas tabelas.
    *   `TRUNCATE TABLE` se a opção `--truncate` for usada.
*   **TIPOS DE DADOS:** Todas as colunas nas tabelas SQL Server são criadas como `NVARCHAR(MAX)`. Isso simplifica a importação e evita erros de conversão de tipo durante a criação da tabela. No entanto, pode não ser o tipo de dado mais eficiente para armazenamento ou consulta. Considere refinar os tipos de dados no SQL Server após a importação, se necessário.
*   **SANITIZAÇÃO DE NOMES:** O script sanitiza nomes de arquivos e cabeçalhos de CSV para criar nomes de tabelas e colunas válidos em SQL. Esteja ciente de como seus nomes originais serão transformados.
*   **ARQUIVOS CSV VAZIOS:** Arquivos CSV vazios ou que contêm apenas cabeçalhos são detectados e pulados.
*   **ERROS DE ENCODING:** Apesar da tentativa de detecção automática e fallbacks, arquivos com encodings muito incomuns ou corrompidos podem ainda causar falhas. Verifique os logs para `UnicodeDecodeError`.
*   **LOGS:** Verifique sempre os arquivos de log no diretório `logs/` para detalhes sobre o processo de importação, especialmente se ocorrerem erros.
*   **PERFORMANCE:** Para arquivos CSV extremamente grandes ou um número muito grande de arquivos, o tempo de importação pode ser significativo. A inserção em chunks e `fast_executemany` ajudam, mas a performance também depende do servidor SQL, da rede e do disco.
*   **DRIVER ODBC:** O script está codificado para usar `DRIVER={ODBC Driver 17 for SQL Server}`. Se você precisar usar um driver diferente, esta string de conexão precisará ser modificada na função `get_sql_server_connection`.