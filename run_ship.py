import csv_ship
import logging
import os
from dotenv import load_dotenv

load_dotenv()


def main():
    print(
        "Iniciando o processo de importação de CSVs através do scripts/run_importer.py"
    )

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER") or None
    password = os.getenv("DB_PASSWORD") or None

    use_trusted = not (user and password)
    csv_directory = os.getenv("CSV_FILES_DIR_SHIP") or None
    db_schema = os.getenv("DB_SCHEMA") or None

    print(
        f"Conectando ao servidor: {server}, banco de dados: {database}, Trusted Connection: {use_trusted}"
    )
    if db_schema:
        print(f"Usando esquema: {db_schema}")
    else:
        print(
            f"Usando esquema padrão (geralmente 'dbo', conforme definido em core/importer.py)"
        )

    if csv_directory:
        print(f"Buscando CSVs em: {csv_directory}")
    else:
        print(
            f"Buscando CSVs no diretório padrão configurado em core/importer.py (provavelmente 'csv/')"
        )

    try:
        csv_ship.process_csv_uploads(
            csv_dir=csv_directory,
            db_server_override=server,
            db_name_override=database,
            db_user_override=user if user else None,
            db_password_override=password if password else None,
            use_trusted_connection=use_trusted,
            truncate_existing_tables=True,
            db_schema_override=db_schema,
        )
        print(
            "Processo de importação de CSVs (scripts/run_importer.py) concluído com sucesso."
        )
    except Exception as e:
        print(
            f"ERRO em scripts/run_importer.py: {e}. Verifique os logs em 'logs/' para detalhes do core.importer."
        )


if __name__ == "__main__":
    main()
