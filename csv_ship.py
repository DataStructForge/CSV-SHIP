import os
import glob
import pyodbc
import pandas as pd
import logging
import csv
from logging.handlers import RotatingFileHandler
import datetime
import chardet
import argparse

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
LOG_FILENAME_BASE = "upload_csv"
LOG_FILENAME = os.path.join(LOG_DIR, f"{LOG_FILENAME_BASE}_{current_date_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=1024 * 1024 * 5, backupCount=2, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)

DB_SERVER = "SEU_SERVIDOR"
DB_NAME = "SEU_BANCO_DE_DADOS"
DB_USER = "SEU_USUARIO"
DB_PASSWORD = "SUA_SENHA"
DB_SCHEMA = "dbo"

CSV_DIRECTORY = "csv"


def get_sql_server_connection(
    server=None, database=None, user=None, password=None, trusted_connection=False
):
    """
    Estabelece e retorna uma conexão com o SQL Server.
    Usa os parâmetros fornecidos ou recorre às constantes globais se os parâmetros não forem fornecidos.
    """
    db_server_to_use = server if server else DB_SERVER
    db_name_to_use = database if database else DB_NAME
    db_user_to_use = user if user else DB_USER
    db_password_to_use = password if password else DB_PASSWORD

    try:
        if trusted_connection or (
            not db_user_to_use and not db_password_to_use and not user and not password
        ):
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server_to_use};DATABASE={db_name_to_use};Trusted_Connection=yes;"
            logging.info(
                f"Tentando conectar ao SQL Server: {db_server_to_use}, Banco de Dados: {db_name_to_use} usando Autenticação do Windows."
            )
        elif db_user_to_use and db_password_to_use:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server_to_use};DATABASE={db_name_to_use};UID={db_user_to_use};PWD={db_password_to_use}"
            logging.info(
                f"Tentando conectar ao SQL Server: {db_server_to_use}, Banco de Dados: {db_name_to_use} com usuário: {db_user_to_use}."
            )
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server_to_use};DATABASE={db_name_to_use};Trusted_Connection=yes;"
            logging.info(
                f"Tentando conectar ao SQL Server: {db_server_to_use}, Banco de Dados: {db_name_to_use} usando Autenticação do Windows (fallback)."
            )

        conn = pyodbc.connect(conn_str)
        logging.info("Conexão com SQL Server estabelecida com sucesso.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Erro ao conectar ao SQL Server: {sqlstate} - {ex}")
        if "08001" in sqlstate:
            logging.error(
                "Verifique se o nome do servidor SQL está correto e se o servidor está acessível."
            )
        elif "28000" in sqlstate:
            logging.error(
                "Falha na autenticação. Verifique suas credenciais (usuário/senha) ou configuração de Trusted_Connection."
            )
        elif "42000" in sqlstate:
            logging.error(
                f"Não foi possível abrir o banco de dados solicitado pelo login. O login falhou ou verifique se o banco de dados existe e você tem permissão."
            )
        return None


def create_table_from_csv(
    conn, table_name, df_chunk, schema_name=None, truncate_existing=False
):
    """
    Cria uma tabela no SQL Server com base no DataFrame (primeiro chunk).
    Todas as colunas são criadas como NVARCHAR(MAX) para simplicidade e para evitar erros de tipo.
    """
    cursor = conn.cursor()
    sanitized_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
    current_schema = schema_name if schema_name else DB_SCHEMA
    full_table_name_for_query = f"[{current_schema}].[{sanitized_table_name}]"
    full_table_name_for_log = f"{current_schema}.{sanitized_table_name}"

    check_table_sql = f"IF OBJECT_ID(N'{current_schema}.{sanitized_table_name}', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0"
    logging.debug(f"Verificando existência da tabela com SQL: {check_table_sql}")
    cursor.execute(check_table_sql)
    if cursor.fetchone()[0] == 1:
        logging.info(f"Tabela '{full_table_name_for_log}' já existe.")
        if truncate_existing:
            try:
                logging.info(
                    f"Opção TRUNCATE habilitada. Truncando tabela '{full_table_name_for_log}'..."
                )
                cursor.execute(f"TRUNCATE TABLE {full_table_name_for_query}")
                conn.commit()
                logging.info(
                    f"Tabela '{full_table_name_for_log}' truncada com sucesso."
                )
            except pyodbc.Error as e_truncate:
                logging.error(
                    f"Erro ao truncar a tabela '{full_table_name_for_log}': {e_truncate}"
                )
                conn.rollback()
                return sanitized_table_name, current_schema, False
        return sanitized_table_name, current_schema, True

    column_definitions = []
    for col_name in df_chunk.columns:
        sanitized_col_name = "".join(c if c.isalnum() else "_" for c in col_name)
        column_definitions.append(f"[{sanitized_col_name}] NVARCHAR(MAX)")

    create_table_sql = (
        f"CREATE TABLE {full_table_name_for_query} ({', '.join(column_definitions)})"
    )

    try:
        logging.info(
            f"Criando tabela '{full_table_name_for_log}' com as colunas: {', '.join(df_chunk.columns)}"
        )
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info(f"Tabela '{full_table_name_for_log}' criada com sucesso.")
        return sanitized_table_name, current_schema, False
    except pyodbc.Error as e:
        if (
            "2760" in str(e)
            or "schema" in str(e).lower()
            and ("does not exist" in str(e).lower() or "cannot find" in str(e).lower())
        ):
            logging.warning(
                f"O esquema '{current_schema}' parece não existir ou não há permissão para usá-lo. Tentando criar o esquema '{current_schema}'..."
            )
            try:
                cursor.execute(
                    f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{current_schema}') EXEC('CREATE SCHEMA [{current_schema}]')"
                )
                conn.commit()
                logging.info(
                    f"Esquema '{current_schema}' verificado/criado. Tentando criar a tabela '{full_table_name_for_log}' novamente."
                )
                cursor.execute(create_table_sql)
                conn.commit()
                logging.info(
                    f"Tabela '{full_table_name_for_log}' criada com sucesso após criação do esquema."
                )
                return sanitized_table_name, current_schema, False
            except pyodbc.Error as e_schema:
                logging.error(
                    f"Erro ao tentar criar o esquema '{current_schema}' ou a tabela '{full_table_name_for_log}' após tentativa de criação do esquema: {e_schema}"
                )
                conn.rollback()
                return sanitized_table_name, current_schema, False
        else:
            logging.error(f"Erro ao criar tabela '{full_table_name_for_log}': {e}")
            conn.rollback()
            return sanitized_table_name, current_schema, False


def detect_encoding(file_path, sample_size=1024 * 10):
    """Detecta o encoding de um arquivo usando chardet lendo uma amostra."""
    try:
        with open(file_path, "rb") as f_raw:
            raw_data = f_raw.read(sample_size)
        result = chardet.detect(raw_data)
        encoding = result["encoding"]
        confidence = result["confidence"]
        logging.info(
            f"Detecção de encoding para {file_path}: {encoding} com confiança {confidence:.2f}"
        )
        
        # Se detectar ASCII, verificar se realmente não há caracteres especiais
        # no arquivo inteiro, pois pode haver caracteres especiais após a amostra
        if encoding and encoding.lower() == 'ascii' and confidence > 0.9:
            # Verifica se o arquivo realmente é ASCII completo ou possui caracteres UTF-8
            try:
                # Abre uma pequena parte do final do arquivo para verificar
                with open(file_path, 'rb') as f_check:
                    # Move para uma posição mais avançada do arquivo
                    f_check.seek(max(0, os.path.getsize(file_path) - 50000))
                    check_data = f_check.read(50000)
                    
                try:
                    # Tenta decodificar como ASCII
                    check_data.decode('ascii')
                except UnicodeDecodeError:
                    # Se falhar como ASCII, provavelmente tem caracteres especiais
                    logging.warning(
                        f"Arquivo {file_path} foi detectado como ASCII mas contém caracteres não-ASCII. "
                        f"Usando UTF-8 como encoding."
                    )
                    return "utf-8"
            except Exception as e:
                logging.warning(
                    f"Erro ao verificar caracteres especiais em {file_path}: {e}. "
                    f"Por precaução, usando UTF-8 como encoding."
                )
                return "utf-8"
        
        # Ajuste para lidar com arquivos CSV em português
        if encoding and encoding.lower() == 'ascii' and os.path.splitext(file_path)[1].lower() in ['.csv', '.txt']:
            logging.info(f"Arquivo CSV/TXT detectado como ASCII, usando UTF-8 como prevenção para caracteres especiais.")
            return "utf-8"
            
        if encoding and confidence > 0.7:
            return encoding
        else:
            logging.warning(
                f"Confiança baixa ({confidence:.2f}) para encoding detectado ('{encoding}') em {file_path}. Usando utf-8 como fallback."
            )
            return "utf-8"
    except Exception as e:
        logging.error(
            f"Erro ao detectar encoding para {file_path}: {e}. Usando utf-8 como fallback."
        )
        return "utf-8"


def detect_separator(file_path, encoding="utf-8", sample_size=1024 * 10):
    """
    Detecta o separador usado no arquivo CSV analisando uma amostra do arquivo.
    Retorna ',' ou ';' baseado na análise da primeira linha não vazia.
    """
    try:
        with open(file_path, "r", encoding=encoding) as f:
            for line in f:
                line = line.strip()
                if line:  # Primeira linha não vazia
                    comma_count = line.count(",")
                    semicolon_count = line.count(";")

                    if semicolon_count > comma_count:
                        logging.info(f"Detectado separador ';' para {file_path}") 
                        return ";"
                    else:
                        logging.info(f"Detectado separador ',' para {file_path}")
                        return ","

        logging.warning(
            f"Não foi possível detectar o separador em {file_path}. Usando ',' como padrão."
        )
        return ","
    except Exception as e:
        logging.error(
            f"Erro ao detectar separador para {file_path}: {e}. Usando ',' como padrão."
        )
        return ","


def insert_data_from_csv(
    conn,
    table_name,
    schema_name,
    csv_file_path,
    file_encoding="utf-8",
    chunk_size=10000,
):
    """
    Insere dados de um arquivo CSV em uma tabela do SQL Server usando chunks.
    A tabela já deve existir.
    """
    cursor = conn.cursor()
    sanitized_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
    current_schema = schema_name if schema_name else DB_SCHEMA
    full_table_name_for_query = f"[{current_schema}].[{sanitized_table_name}]"
    full_table_name_for_log = f"{current_schema}.{sanitized_table_name}"

    try:
        separator = detect_separator(csv_file_path, file_encoding)
        logging.info(
            f"Iniciando leitura do arquivo CSV: {csv_file_path} para a tabela {full_table_name_for_log} com encoding {file_encoding} e separador '{separator}'"
        )
        
        # Lista de encodings para tentar caso o principal falhe
        encodings_to_try = [file_encoding, 'utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        # Remove duplicações
        encodings_to_try = list(dict.fromkeys(encodings_to_try))
        
        success = False
        last_error = None
        
        for encoding in encodings_to_try:
            if success:
                break
                
            try:
                logging.info(f"Tentando ler {csv_file_path} com encoding: {encoding}")
                
                # Estatísticas globais
                total_linhas_processadas = 0
                total_linhas_inseridas = 0
                num_colunas_detectadas_no_arquivo = 0
                # Configurar opções para pandas
                csv_options = {
                    'sep': separator,
                    'encoding': encoding,
                    'chunksize': chunk_size,
                    'dtype': str,  # Para garantir que todas as colunas sejam lidas como string
                    'quoting': csv.QUOTE_MINIMAL,  # Adicionar esta linha para ajudar com campos que contêm separadores
                    'quotechar': '"'  # Garantir que as aspas duplas sejam reconhecidas corretamente
                }
                
                # Modificação para verificar versão do pandas
                try:
                    pd_version = pd.__version__
                    logging.info(f"Versão do pandas detectada: {pd_version}")
                    
                    from packaging import version
                    if version.parse(pd_version) >= version.parse('1.3.0'):
                        csv_options['on_bad_lines'] = 'skip'  # Melhor opção é 'skip' para não perder dados
                        # Parâmetros para pandas >= 1.3.0
                    # Parâmetros para pandas < 1.3.0
                    else:
                        csv_options['error_bad_lines'] = False  # Não levanta erro em linhas ruins
                        csv_options['warn_bad_lines'] = True  # Avisa sobre linhas ruins
                except Exception as version_error:
                    logging.warning(f"Erro ao verificar versão do pandas: {version_error}. Usando parâmetros seguros.")
                    # Não adicionar parâmetros potencialmente incompatíveis
                
                # Verifica se deve usar a abordagem alternativa linha por linha
                usar_abordagem_alternativa = True  # Defina como True para forçar o uso da abordagem alternativa
                
                if usar_abordagem_alternativa:
                    logging.info(f"Usando abordagem alternativa (linha por linha) para processamento do arquivo {csv_file_path}")
                    # Abordagem alternativa: ler o arquivo linha a linha e processar manualmente
                    with open(csv_file_path, 'r', encoding=encoding) as file:
                        import csv as csv_module  # Para evitar conflito com o módulo já importado
                        
                        # Ler o cabeçalho
                        header_line = file.readline().strip()
                        reader = csv_module.reader([header_line], delimiter=separator, quotechar='"')
                        header = next(reader)
                        num_colunas_detectadas_no_arquivo = len(header)
                        
                        # Sanitizar nomes de colunas
                        sanitized_columns = ["".join(c if c.isalnum() else "_" for c in col) for col in header]
                        
                        # Preparar SQL
                        cols = ", ".join([f"[{col}]" for col in sanitized_columns])
                        placeholders = ", ".join(["?"] * len(sanitized_columns))
                        insert_sql = f"INSERT INTO {full_table_name_for_query} ({cols}) VALUES ({placeholders})"
                        
                        # Processar linhas em chunks
                        batch = []
                        line_count = 1  # Já lemos a primeira linha (cabeçalho)
                        
                        # Estatísticas
                        linhas_com_colunas_divergentes = 0
                        total_colunas_originais = 0
                        total_colunas_inseridas = 0
                        linhas_por_colunas = {}  # Dicionário para contar linhas por quantidade de colunas
                        
                        for line in file:
                            line_count += 1
                            try:
                                # Ler a linha como CSV
                                row_reader = csv_module.reader([line.strip()], delimiter=separator, quotechar='"')
                                row = next(row_reader)
                                
                                # Verificar se o número de campos é diferente do cabeçalho
                                if len(row) != len(header):
                                    colunas_originais = len(row)
                                    colunas_esperadas = len(header)
                                    
                                    linhas_com_colunas_divergentes += 1
                                    total_colunas_originais += colunas_originais
                                    total_colunas_inseridas += min(colunas_originais, colunas_esperadas)
                                    
                                    # Contagem de linhas por quantidade de colunas
                                    if colunas_originais not in linhas_por_colunas:
                                        linhas_por_colunas[colunas_originais] = 0
                                    linhas_por_colunas[colunas_originais] += 1
                                    
                                    logging.warning(f"Linha {line_count} tem {colunas_originais} campos, esperado {colunas_esperadas}. " + 
                                                    f"Relação: {min(colunas_originais, colunas_esperadas)}/{colunas_originais} colunas.")
                                    
                                    # Se tiver campos a mais, corta
                                    if len(row) > len(header):
                                        row = row[:len(header)]
                                    # Se tiver campos a menos, completa com vazios
                                    else:
                                        row.extend([''] * (len(header) - len(row)))
                                
                                # Processar valores nulos
                                processed_row = []
                                for value in row:
                                    stripped_value = value.strip() if isinstance(value, str) else value
                                    if stripped_value == '':
                                        processed_row.append(None)
                                    else:
                                        processed_row.append(stripped_value)
                                
                                batch.append(tuple(processed_row))
                                
                                # Inserir em chunks
                                if len(batch) >= chunk_size:
                                    cursor.fast_executemany = True
                                    cursor.executemany(insert_sql, batch)
                                    conn.commit()
                                    total_linhas_inseridas += len(batch)
                                    logging.info(f"Inseridas {len(batch)} linhas (até linha {line_count}) na tabela '{full_table_name_for_log}'")
                                    batch = []
                                    
                            except Exception as line_error:
                                logging.warning(f"Erro ao processar linha {line_count}: {line_error}. Continuando...")
                        
                        # Inserir o último batch
                        if batch:
                            cursor.fast_executemany = True
                            cursor.executemany(insert_sql, batch)
                            conn.commit()
                            total_linhas_inseridas += len(batch)
                            logging.info(f"Inseridas últimas {len(batch)} linhas na tabela '{full_table_name_for_log}'")
                        
                        total_linhas_processadas = line_count - 1
                        # Exibir estatísticas finais
                        if linhas_com_colunas_divergentes > 0:
                            logging.info(f"Estatísticas do arquivo {csv_file_path}:")
                            logging.info(f"- Total de linhas com colunas divergentes: {linhas_com_colunas_divergentes}")
                            logging.info(f"- Relação colunas inseridas/originais: {total_colunas_inseridas}/{total_colunas_originais}")
                            logging.info(f"- Total de colunas processadas: {len(header)}, colunas inseridas: {len(header)}")
                            logging.info(f"- Distribuição de linhas por quantidade de colunas:")
                            for num_cols, count in sorted(linhas_por_colunas.items()):
                                logging.info(f"  * {num_cols} colunas: {count} linhas")
                    
                    success = True
                    logging.info(f"Processamento alternativo bem-sucedido para '{csv_file_path}'")
                    logging.info(f"Total de linhas processadas: {total_linhas_processadas}, linhas inseridas: {total_linhas_inseridas}")
                    logging.info(f"Total de colunas processadas: {num_colunas_detectadas_no_arquivo}, colunas inseridas: {num_colunas_detectadas_no_arquivo}")
                else:
                    # Abordagem padrão com pandas
                    first_chunk = True
                    for i, chunk_df in enumerate(pd.read_csv(csv_file_path, **csv_options)):
                        logging.info(
                            f"Processando chunk {i+1} do arquivo {csv_file_path} ({len(chunk_df)} linhas)"
                        )
                        
                        total_linhas_processadas += len(chunk_df)

                        if first_chunk:
                            num_colunas_detectadas_no_arquivo = len(chunk_df.columns)
                            first_chunk = False

                        chunk_df.columns = [
                            "".join(c if c.isalnum() else "_" for c in col)
                            for col in chunk_df.columns
                        ]

                        # Verificar se existem linhas com colunas incorretas
                        colunas_esperadas = len(chunk_df.columns)
                        logging.info(f"Número de colunas esperado: {colunas_esperadas}")

                        cols = ", ".join([f"[{col}]" for col in chunk_df.columns])
                        placeholders = ", ".join(["?"] * len(chunk_df.columns))
                        insert_sql = f"INSERT INTO {full_table_name_for_query} ({cols}) VALUES ({placeholders})"

                        data_tuples = []
                        for row_tuple in chunk_df.itertuples(index=False, name=None):
                            processed_row = []
                            for item in row_tuple:
                                if pd.isna(item):
                                    processed_row.append(None)
                                else:
                                    processed_row.append(str(item).strip())
                            data_tuples.append(tuple(processed_row))

                        try:
                            cursor.fast_executemany = True
                            cursor.executemany(insert_sql, data_tuples)
                            conn.commit()
                            total_linhas_inseridas += len(data_tuples)
                            logging.info(
                                f"Chunk {i+1} ({len(chunk_df)} linhas) inserido com sucesso na tabela '{full_table_name_for_log}'."
                            )
                        except pyodbc.Error as e:
                            logging.error(
                                f"Erro ao inserir dados do chunk {i+1} na tabela '{full_table_name_for_log}': {e}"
                            )
                            logging.error(
                                f"Dados do chunk que falhou (primeiras 5 linhas):\n{chunk_df.head()}"
                            )
                            conn.rollback()
                            raise e
                
                # Se chegou aqui sem exceções, foi um sucesso
                success = True
                logging.info(
                    f"Todos os dados do arquivo '{csv_file_path}' foram inseridos com sucesso na tabela '{full_table_name_for_log}' usando encoding {encoding}."
                )
                logging.info(f"Total de linhas processadas: {total_linhas_processadas}, linhas inseridas: {total_linhas_inseridas}")
                logging.info(f"Total de colunas processadas: {num_colunas_detectadas_no_arquivo}, colunas inseridas: {num_colunas_detectadas_no_arquivo}")
                
            except UnicodeDecodeError as e:
                last_error = e
                logging.warning(
                    f"Erro de decodificação ao ler {csv_file_path} com encoding {encoding}: {e}. Tentando próximo encoding..."
                )
                continue
            except Exception as e:
                last_error = e
                logging.error(
                    f"Erro ao processar o arquivo CSV '{csv_file_path}' com encoding {encoding}: {e}"
                )
                if "codec can't decode" in str(e) or "Error tokenizing data" in str(e):
                    logging.warning("Erro parece ser de encoding ou formato de dados, tentando próximo encoding ou método alternativo...")
                    
                    if "Error tokenizing data" in str(e):
                        try:
                            logging.info(f"Tentando abordagem alternativa com leitura linha a linha para {csv_file_path}")
                            # Abordagem alternativa: ler o arquivo linha a linha e processar manualmente
                            with open(csv_file_path, 'r', encoding=encoding) as file:
                                import csv as csv_module  # Para evitar conflito com o módulo já importado
                                
                                # Ler o cabeçalho
                                header_line = file.readline().strip()
                                reader = csv_module.reader([header_line], delimiter=separator)
                                header = next(reader)
                                num_colunas_detectadas_no_arquivo = len(header)
                                
                                # Sanitizar nomes de colunas
                                sanitized_columns = ["".join(c if c.isalnum() else "_" for c in col) for col in header]
                                
                                # Preparar SQL
                                cols = ", ".join([f"[{col}]" for col in sanitized_columns])
                                placeholders = ", ".join(["?"] * len(sanitized_columns))
                                insert_sql = f"INSERT INTO {full_table_name_for_query} ({cols}) VALUES ({placeholders})"
                                
                                # Processar linhas em chunks
                                batch = []
                                line_count = 1  # Já lemos a primeira linha (cabeçalho)
                                
                                # Estatísticas
                                linhas_com_colunas_divergentes = 0
                                total_colunas_originais = 0
                                total_colunas_inseridas = 0
                                linhas_por_colunas = {}  # Dicionário para contar linhas por quantidade de colunas
                                
                                for line in file:
                                    line_count += 1
                                    try:
                                        # Ler a linha como CSV
                                        row_reader = csv_module.reader([line.strip()], delimiter=separator)
                                        row = next(row_reader)
                                        
                                        # Verificar se o número de campos é diferente do cabeçalho
                                        if len(row) != len(header):
                                            colunas_originais = len(row)
                                            colunas_esperadas = len(header)
                                            
                                            linhas_com_colunas_divergentes += 1
                                            total_colunas_originais += colunas_originais
                                            total_colunas_inseridas += min(colunas_originais, colunas_esperadas)
                                            
                                            # Contagem de linhas por quantidade de colunas
                                            if colunas_originais not in linhas_por_colunas:
                                                linhas_por_colunas[colunas_originais] = 0
                                            linhas_por_colunas[colunas_originais] += 1
                                            
                                            logging.warning(f"Linha {line_count} tem {colunas_originais} campos, esperado {colunas_esperadas}. " + 
                                                            f"Relação: {min(colunas_originais, colunas_esperadas)}/{colunas_originais} colunas.")
                                            
                                            # Se tiver campos a mais, corta
                                            if len(row) > len(header):
                                                row = row[:len(header)]
                                            # Se tiver campos a menos, completa com vazios
                                            else:
                                                row.extend([''] * (len(header) - len(row)))
                                        
                                        # Processar valores nulos
                                        processed_row = []
                                        for value in row:
                                            stripped_value = value.strip() if isinstance(value, str) else value
                                            if stripped_value == '':
                                                processed_row.append(None)
                                            else:
                                                processed_row.append(stripped_value)
                                        
                                        batch.append(tuple(processed_row))
                                        
                                        # Inserir em chunks
                                        if len(batch) >= chunk_size:
                                            cursor.fast_executemany = True
                                            cursor.executemany(insert_sql, batch)
                                            conn.commit()
                                            total_linhas_inseridas += len(batch)
                                            logging.info(f"Inseridas {len(batch)} linhas (até linha {line_count}) na tabela '{full_table_name_for_log}'")
                                            batch = []
                                            
                                    except Exception as line_error:
                                        logging.warning(f"Erro ao processar linha {line_count}: {line_error}. Continuando...")
                                
                                # Inserir o último batch
                                if batch:
                                    cursor.fast_executemany = True
                                    cursor.executemany(insert_sql, batch)
                                    conn.commit()
                                    total_linhas_inseridas += len(batch)
                                    logging.info(f"Inseridas últimas {len(batch)} linhas na tabela '{full_table_name_for_log}'")
                                
                                # Exibir estatísticas finais
                                if linhas_com_colunas_divergentes > 0:
                                    logging.info(f"Estatísticas do arquivo {csv_file_path}:")
                                    logging.info(f"- Total de linhas com colunas divergentes: {linhas_com_colunas_divergentes}")
                                    logging.info(f"- Relação colunas inseridas/originais: {total_colunas_inseridas}/{total_colunas_originais}")
                                    logging.info(f"- Total de colunas processadas: {len(header)}, colunas inseridas: {len(header)}")
                                    logging.info(f"- Distribuição de linhas por quantidade de colunas:")
                                    for num_cols, count in sorted(linhas_por_colunas.items()):
                                        logging.info(f"  * {num_cols} colunas: {count} linhas")
                            
                            success = True
                            total_linhas_processadas = line_count - 1
                            logging.info(f"Processamento alternativo bem-sucedido para '{csv_file_path}'")
                            logging.info(f"Total de linhas processadas: {total_linhas_processadas}, linhas inseridas: {total_linhas_inseridas}")
                            logging.info(f"Total de colunas processadas: {len(header)}, colunas inseridas: {len(header)}")
                            break
                            
                        except Exception as alt_error:
                            logging.error(f"Falha na abordagem alternativa: {alt_error}")
                    
                    continue
                else:
                    # Outro tipo de erro, não relacionado a encoding ou parsing
                    raise e
        
        if not success:
            logging.error(
                f"Falha ao processar {csv_file_path} após tentar todos os encodings disponíveis. Último erro: {last_error}"
            )
            return False
            
        return True
        
    except pd.errors.EmptyDataError:
        logging.warning(
            f"O arquivo CSV '{csv_file_path}' está vazio. Nenhuma tabela criada ou dados inseridos."
        )
        return False
    except Exception as e:
        logging.error(
            f"Erro inesperado ao processar o arquivo CSV '{csv_file_path}': {e}"
        )
        return False


def process_csv_uploads(
    csv_dir=None,
    db_server_override=None,
    db_name_override=None,
    db_user_override=None,
    db_password_override=None,
    use_trusted_connection=False,
    truncate_existing_tables=False,
    db_schema_override=None,
):
    """
    Função principal para orquestrar o upload dos CSVs.
    Permite override das configurações globais.
    """
    logging.info("Iniciando processo de upload de CSVs para o SQL Server.")

    current_csv_directory = csv_dir if csv_dir else CSV_DIRECTORY
    current_db_schema = db_schema_override if db_schema_override else DB_SCHEMA
    logging.info(f"Usando esquema: '{current_db_schema}'")

    conn = get_sql_server_connection(
        server=db_server_override,
        database=db_name_override,
        user=db_user_override,
        password=db_password_override,
        trusted_connection=use_trusted_connection,
    )
    if not conn:
        logging.error("Não foi possível conectar ao banco de dados. Abortando.")
        return

    csv_files = glob.glob(os.path.join(current_csv_directory, "*.csv"))
    if not csv_files:
        logging.warning(
            f"Nenhum arquivo CSV encontrado no diretório '{current_csv_directory}'."
        )
        conn.close()
        return

    logging.info(
        f"Arquivos CSV encontrados: {len(csv_files)} em '{current_csv_directory}'"
    )
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        table_name_base = os.path.splitext(file_name)[0]
        table_name = "".join(c if c.isalnum() else "_" for c in table_name_base)
        table_name = table_name.replace("-", "_")

        logging.info(
            f"Processando arquivo: {csv_file} -> Tabela: {current_db_schema}.{table_name}"
        )

        current_file_encoding = detect_encoding(csv_file)
        if not current_file_encoding:
            logging.error(
                f"Não foi possível determinar o encoding para {csv_file}. Pulando arquivo."
            )
            continue

        try:
            try:
                separator = detect_separator(csv_file, current_file_encoding)
                first_chunk = next(
                    pd.read_csv(
                        csv_file,
                        chunksize=5,
                        low_memory=False,
                        encoding=current_file_encoding,
                        sep=separator,
                    )
                )
                logging.info(
                    f"Primeiro chunk de {csv_file} lido com sucesso usando encoding '{current_file_encoding}' e separador '{separator}'."
                )
            except UnicodeDecodeError:
                logging.error(
                    f"Falha de UnicodeDecodeError ao ler {csv_file} com encoding detectado/fallback '{current_file_encoding}'. Verifique o arquivo."
                )
                logging.warning(
                    f"Tentando com latin1 como último recurso para {csv_file}"
                )
                try:
                    current_file_encoding = "latin1"
                    separator = detect_separator(csv_file, current_file_encoding)
                    first_chunk = next(
                        pd.read_csv(
                            csv_file,
                            chunksize=5,
                            low_memory=False,
                            encoding=current_file_encoding,
                            sep=separator,
                        )
                    )
                    logging.info(
                        f"Primeiro chunk de {csv_file} lido com sucesso usando encoding de último recurso '{current_file_encoding}' e separador '{separator}'."
                    )
                except Exception as e_fallback:
                    logging.error(
                        f"Falha ao ler {csv_file} mesmo com encoding de último recurso '{current_file_encoding}': {e_fallback}. Pulando arquivo."
                    )
                    continue
            except StopIteration:
                logging.warning(
                    f"O arquivo CSV '{csv_file}' parece estar vazio ou contém apenas cabeçalhos. Pulando."
                )
                continue

            if first_chunk.empty:
                logging.warning(
                    f"O arquivo CSV '{csv_file}' está vazio ou não contém dados após o cabeçalho. Pulando."
                )
                continue

            created_table_name, created_schema_name, table_existed = (
                create_table_from_csv(
                    conn,
                    table_name,
                    first_chunk,
                    schema_name=current_db_schema,
                    truncate_existing=truncate_existing_tables,
                )
            )

            if created_table_name:
                if table_existed:
                    logging.info(
                        f"Tabela '{created_schema_name}.{created_table_name}' já existia. Verifique logs para status de TRUNCATE se aplicável."
                    )

                success = insert_data_from_csv(
                    conn,
                    created_table_name,
                    created_schema_name,
                    csv_file,
                    file_encoding=current_file_encoding,
                )
                if success:
                    logging.info(
                        f"Arquivo '{file_name}' processado e dados inseridos na tabela '{created_schema_name}.{created_table_name}'."
                    )
                else:
                    logging.error(
                        f"Falha ao inserir dados do arquivo '{file_name}' na tabela '{created_schema_name}.{created_table_name}'."
                    )
            else:
                logging.error(
                    f"Não foi possível determinar o nome da tabela ou criar a tabela para o arquivo {csv_file}. Pulando inserção."
                )

        except pd.errors.EmptyDataError:
            logging.warning(
                f"O arquivo CSV '{csv_file}' está vazio. Nenhuma tabela criada ou dados inseridos."
            )
        except Exception as e:
            logging.error(f"Erro inesperado ao processar o arquivo '{csv_file}': {e}")

    if conn:
        conn.close()
        logging.info("Conexão com SQL Server fechada.")
    logging.info("Processo de upload de CSVs concluído.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processa arquivos CSV e os importa para tabelas do SQL Server."
    )

    parser.add_argument(
        "--csv-dir",
        type=str,
        default=CSV_DIRECTORY,
        help=f"Diretório contendo os arquivos CSV a serem processados. Padrão: '{CSV_DIRECTORY}'",
    )

    parser.add_argument(
        "--db-server",
        type=str,
        default=None,
        help=f"Nome ou endereço do servidor SQL. Padrão usa o valor em DB_SERVER ('{DB_SERVER}').",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=None,
        help=f"Nome do banco de dados SQL. Padrão usa o valor em DB_NAME ('{DB_NAME}').",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=None,
        help="Usuário para autenticação SQL Server. Se não fornecido e --trusted-connection não estiver ativo, usa DB_USER.",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=None,
        help="Senha para autenticação SQL Server. Se não fornecido e --trusted-connection não estiver ativo, usa DB_PASSWORD.",
    )
    parser.add_argument(
        "--db-schema",
        type=str,
        default=None,
        help=f"Nome do esquema do banco de dados. Padrão usa o valor em DB_SCHEMA ('{DB_SCHEMA}').",
    )
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Usar Autenticação do Windows. Se especificado, ignora --db-user e --db-password. "
        "Se não especificado, o script tentará usar usuário/senha se fornecidos (via arg ou globais), "
        "ou fallback para trusted connection se usuário/senha não estiverem disponíveis.",
    )

    parser.add_argument(
        "--truncate",
        action="store_true",
        default=False,
        help="Se especificado, as tabelas existentes serão truncadas antes da inserção de novos dados. Padrão: Não truncar.",
    )

    args = parser.parse_args()

    use_trusted_arg = args.trusted_connection
    if (
        not use_trusted_arg
        and not args.db_user
        and not args.db_password
        and not DB_USER
        and not DB_PASSWORD
    ):
        if (DB_USER == "SEU_USUARIO" or not DB_USER) and (
            DB_PASSWORD == "SUA_SENHA" or not DB_PASSWORD
        ):
            use_trusted_arg = True
            logging.info(
                "Nenhum usuário/senha fornecido e --trusted-connection não especificado. Usando Autenticação do Windows por padrão."
            )

    process_csv_uploads(
        csv_dir=args.csv_dir,
        db_server_override=args.db_server,
        db_name_override=args.db_name,
        db_user_override=args.db_user,
        db_password_override=args.db_password,
        use_trusted_connection=use_trusted_arg,
        truncate_existing_tables=args.truncate,
        db_schema_override=args.db_schema,
    )
