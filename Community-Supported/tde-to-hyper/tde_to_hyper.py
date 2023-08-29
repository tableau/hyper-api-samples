import argparse
import os
from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, escape_string_literal, TableName, HyperException

def convert_tde_to_hyper(hyper_endpoint: str, tde_path: Path, hyper_path: Path):
    os.makedirs(hyper_path.parent.absolute(), exist_ok = True)
    with Connection(endpoint=hyper_endpoint, database=hyper_database, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        try:
            existing_tables = connection.execute_list_query(f"""
                SELECT "SCHEMAS_NAME", "TABLES_NAME"
                FROM external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => 'SYS.TABLES')
                JOIN external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => 'SYS.SCHEMAS')
                ON "TABLES_PARENT"="SCHEMAS_ID"
                WHERE "SCHEMAS_NAME" <> 'SYS' AND "TABLES_NAME"<>'$TableauMetadata' AND "TABLES_ACTIVE" AND "SCHEMAS_ACTIVE"
            """)
            for schema, table in existing_tables:
                # Create the destination table in the Hyper database
                connection.catalog.create_schema_if_not_exists(schema)
                connection.execute_command(f"""
                    CREATE TABLE {TableName(schema, table)} AS
                    SELECT * FROM external({escape_string_literal(str(tde_path))}, format => 'tde', "table" => {escape_string_literal(f"{schema}.{table}")})""")
        except HyperException:
            os.unlink(hyper_database)
            print(f"FAILED conversion {tde_path} -> {hyper_database}")

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Script to convert a TDE file to a Hyper file.")
    argparser.add_argument("input_tde_path", type=Path, help="The input TDE file path that will be converted to a Hyper file.")
    argparser.add_argument("--output", type=Path, help="The output path.", default=".")
    args = argparser.parse_args()
    
    input_tde_path = Path(args.input_tde_path)
    if not input_tde_path.exists():
        raise Exception(f"{input_tde_path} not found")

    if input_tde_path.is_dir():
        inputs = list(input_tde_path.glob("**/*.tde"))
    else:
        inputs = [input_tde_path]

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        for tde_path in inputs:
            # Rename path with a .hyper extension in the directory of the tde file
            hyper_database = args.output / tde_path.with_name(tde_path.stem + '.hyper')
            convert_tde_to_hyper(hyper.endpoint, tde_path, hyper_database)

