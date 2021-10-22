import argparse
from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, TableDefinition, escape_name, \
    escape_string_literal

def convert_tde_to_hyper(tde_path: Path):
    # Rename path with a .hyper extension in the directory of the tde file
    hyper_database = tde_path.with_name(tde_path.stem + '.hyper')

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_database, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            # Schema and table for TDE file is constant
            schema = 'Extract'
            table = 'Extract'

            # Create the temp external table for the TDE file
            create_external_table_query = _get_external_table_query(str(tde_path), schema, table)
            connection.execute_command(create_external_table_query)

            # Get the name of the table created from the catalog
            td = connection.catalog.get_table_definition(table)

            # Create the schema
            connection.catalog.create_schema(schema)

            # Create the destination table in the Hyper database
            schema_table = f"\"{schema}\".\"{table}\"" 
            create_table_command = f"CREATE TABLE {escape_name(schema_table)} AS SELECT * FROM {td.table_name}"

            # Execute
            connection.execute_command(create_table_command)

    print(f"Successfully converted {tde_path} to {hyper_database}")

def _get_external_table_query(tde_file_path: str,
                              schema: str,
                              table: str):
    return f"""CREATE TEMP EXTERNAL TABLE {escape_name(table)} FOR {escape_string_literal(tde_file_path)} 
    (WITH (FORMAT TDE, TABLE {escape_string_literal(f"{schema}.{table}")}, SANITIZE))"""

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Script to convert a TDE file to a Hyper file.")
    argparser.add_argument("input_tde_path", type=Path, help="The input TDE file path that will be converted to a Hyper file.")
    args = argparser.parse_args()
    
    input_tde_path = Path(args.input_tde_path)
    if not input_tde_path.exists():
        raise Exception(f"{input_tde_path} not found")

    convert_tde_to_hyper(input_tde_path)
