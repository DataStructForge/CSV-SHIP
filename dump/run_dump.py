import csv_dump
import logging
import os
from dotenv import load_dotenv

load_dotenv()


def main():
    print(
        "Iniciando o processo de DELEÇÃO de tabelas através do scripts/run_deleter.py"
    )

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    schema = os.getenv("DB_SCHEMA") or None
    user = os.getenv("DB_USER") or None
    password = os.getenv("DB_PASSWORD") or None

    use_trusted = not (user and password)

    csv_files_directory = os.getenv("CSV_FILES_DIR_DUMPSTER") or None

    perform_dry_run = False

    print(
        f"CONFIGURAÇÃO (scripts/run_deleter.py): Servidor='{server}', Banco='{database}', Esquema='{schema or '(padrão de core.deleter)'}', TrustedConn='{use_trusted}', DirCSV='{csv_files_directory or '(padrão de core.deleter)'}', DryRun='{perform_dry_run}'"
    )

    if not perform_dry_run:
        confirmation = input(
            "ATENÇÃO: DRY RUN está DESABILITADO. TABELAS SERÃO DELETADAS PERMANENTEMENTE. Continuar? (S/N): "
        )
        if confirmation.lower() != "s":
            print("Deleção cancelada pelo usuário.")
            return

    try:
        csv_dump.process_table_deletions(
            csv_dir=csv_files_directory,
            db_server_override=server,
            db_name_override=database,
            db_user_override=user if user else None,
            db_password_override=password if password else None,
            use_trusted_connection=use_trusted,
            db_schema_override=schema,
            dry_run=perform_dry_run,
        )
        print("Processo de deleção de tabelas (scripts/run_deleter.py) concluído.")
    except Exception as e:
        print(
            f"ERRO em scripts/run_deleter.py: {e}. Verifique os logs em 'logs/' para detalhes do core.deleter."
        )


if __name__ == "__main__":
    main()
