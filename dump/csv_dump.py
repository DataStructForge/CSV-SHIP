import os
import glob
import pyodbc
import logging
from logging.handlers import RotatingFileHandler
import datetime
import argparse

# --- Configuração do Logging ---
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
LOG_FILENAME_BASE = "delete_tables_process"
LOG_FILENAME = os.path.join(LOG_DIR, f"{LOG_FILENAME_BASE}_{current_date_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=1024 * 1024 * 5, backupCount=2),
        logging.StreamHandler(),
    ],
)

# --- CONFIGURAÇÕES DO BANCO DE DADOS (Padrões Globais) ---
DB_SERVER = "SEU_SERVIDOR"
DB_NAME = "SEU_BANCO_DE_DADOS"
DB_USER = "SEU_USUARIO"
DB_PASSWORD = "SUA_SENHA"
DB_SCHEMA = "dbo"  # Esquema padrão adicionado

# --- CONFIGURAÇÕES DOS ARQUIVOS (Padrões Globais) ---
CSV_DIRECTORY = "csv"


def get_sql_server_connection(
    server=None, database=None, user=None, password=None, trusted_connection=False
):
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
                f"Tentando conectar ao SQL Server: {db_server_to_use}, DB: {db_name_to_use} (Windows Auth)."
            )
        elif db_user_to_use and db_password_to_use:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server_to_use};DATABASE={db_name_to_use};UID={db_user_to_use};PWD={db_password_to_use}"
            logging.info(
                f"Tentando conectar ao SQL Server: {db_server_to_use}, DB: {db_name_to_use} com usuário: {db_user_to_use}."
            )
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server_to_use};DATABASE={db_name_to_use};Trusted_Connection=yes;"
            logging.info(
                f"Tentando conectar ao SQL Server: {db_server_to_use}, DB: {db_name_to_use} (Windows Auth fallback)."
            )

        conn = pyodbc.connect(conn_str)
        logging.info("Conexão com SQL Server estabelecida com sucesso.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Erro ao conectar ao SQL Server: {sqlstate} - {ex}")
        if "08001" in sqlstate:
            logging.error("Verifique nome/acesso ao servidor SQL.")
        elif "28000" in sqlstate:
            logging.error("Falha na autenticação.")
        elif "42000" in sqlstate:
            logging.error(
                f"Não foi possível abrir DB '{db_name_to_use}'. Login falhou ou DB não existe/sem permissão."
            )
        return None


def delete_sql_table(conn, table_name, schema_name):
    """Tenta deletar uma tabela no SQL Server, considerando o esquema."""
    cursor = conn.cursor()
    current_schema = schema_name if schema_name else DB_SCHEMA
    sanitized_table_name = "".join(c if c.isalnum() else "_" for c in table_name)
    sanitized_table_name = sanitized_table_name.replace("-", "_")

    full_table_name_for_query = f"[{current_schema}].[{sanitized_table_name}]"
    full_table_name_for_log = f"{current_schema}.{sanitized_table_name}"

    drop_table_sql = f"DROP TABLE IF EXISTS {full_table_name_for_query}"
    try:
        logging.info(f"Tentando deletar tabela: {full_table_name_for_log}...")
        cursor.execute(drop_table_sql)
        conn.commit()
        logging.info(
            f"Comando DROP TABLE IF EXISTS para {full_table_name_for_log} executado com sucesso."
        )
        return True
    except pyodbc.Error as e:
        logging.error(f"Erro ao tentar deletar tabela {full_table_name_for_log}: {e}")
        conn.rollback()
        return False


def process_table_deletions(
    csv_dir=None,
    db_server_override=None,
    db_name_override=None,
    db_user_override=None,
    db_password_override=None,
    use_trusted_connection=False,
    db_schema_override=None,  # Adicionado
    dry_run=False,
):
    """Função principal para orquestrar a deleção de tabelas baseadas em nomes de arquivos CSV."""
    logging.info(
        "Iniciando processo de deleção de tabelas SQL baseadas em nomes de arquivos CSV."
    )

    current_csv_directory = csv_dir if csv_dir else CSV_DIRECTORY
    current_db_schema = db_schema_override if db_schema_override else DB_SCHEMA
    logging.info(f"Usando esquema: '{current_db_schema}' para deleção.")

    if not os.path.isdir(current_csv_directory):
        logging.error(
            f"Diretório de CSV especificado não existe: {current_csv_directory}. Abortando."
        )
        return

    conn = get_sql_server_connection(
        server=db_server_override,
        database=db_name_override,
        user=db_user_override,
        password=db_password_override,
        trusted_connection=use_trusted_connection,
    )
    if not conn:
        logging.error(
            "Não foi possível conectar ao banco de dados. Abortando deleções."
        )
        return

    csv_files = glob.glob(os.path.join(current_csv_directory, "*.csv"))
    if not csv_files:
        logging.warning(
            f"Nenhum arquivo CSV encontrado no diretório '{current_csv_directory}'. Nenhuma tabela a ser deletada com base no esquema '{current_db_schema}'."
        )
        conn.close()
        return

    logging.info(
        f"Arquivos CSV encontrados: {len(csv_files)} em '{current_csv_directory}'. As seguintes tabelas serão alvo no esquema '{current_db_schema}' (se existirem):"
    )

    tables_to_delete = []
    for csv_file_path in csv_files:
        file_name = os.path.basename(csv_file_path)
        table_name_base = os.path.splitext(file_name)[0]
        table_name_to_delete = "".join(
            c if c.isalnum() else "_" for c in table_name_base
        )
        table_name_to_delete = table_name_to_delete.replace("-", "_")
        tables_to_delete.append(table_name_to_delete)
        logging.info(
            f"  - Arquivo: {file_name} -> Tabela Alvo: {current_db_schema}.{table_name_to_delete}"
        )

    if dry_run:
        logging.info("DRY RUN habilitado. Nenhuma tabela será realmente deletada.")
        conn.close()
        logging.info("Processo de deleção (dry run) concluído.")
        return

    deleted_count = 0
    failed_count = 0
    for table_name in tables_to_delete:
        if delete_sql_table(conn, table_name, current_db_schema):  # Passa o esquema
            deleted_count += 1
        else:
            failed_count += 1

    if conn:
        conn.close()
        logging.info("Conexão com SQL Server fechada.")

    logging.info(
        f"Processo de deleção de tabelas concluído. {deleted_count} comando(s) DROP TABLE para o esquema '{current_db_schema}' executado(s) com sucesso (ou tabela não existia). {failed_count} falha(s)."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deleta tabelas SQL Server cujos nomes são derivados de arquivos CSV em um diretório, considerando o esquema."
    )

    parser.add_argument(
        "--csv-dir",
        type=str,
        default=CSV_DIRECTORY,
        help=f"Diretório contendo os arquivos CSV cujos nomes basearão a deleção das tabelas. Padrão: '{CSV_DIRECTORY}'",
    )

    parser.add_argument("--db-server", type=str, default=None, help="Servidor SQL.")
    parser.add_argument(
        "--db-name", type=str, default=None, help="Nome do banco de dados SQL."
    )
    parser.add_argument("--db-user", type=str, default=None, help="Usuário SQL Server.")
    parser.add_argument(
        "--db-password", type=str, default=None, help="Senha SQL Server."
    )
    parser.add_argument(
        "--db-schema",
        type=str,
        default=None,
        help=f"Nome do esquema do banco de dados onde as tabelas estão localizadas. Padrão usa o valor em DB_SCHEMA ('{DB_SCHEMA}').",
    )
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Usar Autenticação do Windows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Listar tabelas que seriam deletadas, mas não executar o DROP.",
    )

    args = parser.parse_args()

    use_trusted_arg = args.trusted_connection
    if not use_trusted_arg and not args.db_user and not args.db_password:
        if (DB_USER == "SEU_USUARIO" or not DB_USER) and (
            DB_PASSWORD == "SUA_SENHA" or not DB_PASSWORD
        ):
            use_trusted_arg = True
            logging.info(
                "Nenhum usuário/senha fornecido via args e globais são padrão. Usando Autenticação do Windows."
            )

    process_table_deletions(
        csv_dir=args.csv_dir,
        db_server_override=args.db_server,
        db_name_override=args.db_name,
        db_user_override=args.db_user,
        db_password_override=args.db_password,
        use_trusted_connection=use_trusted_arg,
        db_schema_override=args.db_schema,  # Passa o schema
        dry_run=args.dry_run,
    )
