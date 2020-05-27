import argparse
from pathlib import PurePath
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableDefinition, TableName

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="This tool rewrites a given Hyper file in an optimized, dense format.")
    argparser.add_argument("input_hyper_file_path", type=PurePath, help="The input Hyper file path that will be rewritten")
    argparser.add_argument("--output-hyper-file-path", "-o", type=PurePath, help="The output Hyper file path for the rewritten output Hyper file")
    args = argparser.parse_args()
    if not args.output_hyper_file_path:
        args.output_hyper_file_path = args.input_hyper_file_path.parent / (args.input_hyper_file_path.stem + f".new.hyper")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint) as connection:
            # Create the output Hyper file or overwrite it
            catalog = connection.catalog
            catalog.drop_database_if_exists(args.output_hyper_file_path)
            catalog.create_database(args.output_hyper_file_path)
            catalog.attach_database(args.output_hyper_file_path, alias="output_database")
            catalog.attach_database(args.input_hyper_file_path, alias="input_database")

            # Process all tables of all schemas of the input Hyper file and copy them into the output Hyper file
            for input_schema_name in catalog.get_schema_names("input_database"):
                for input_table_name in catalog.get_table_names(input_schema_name):
                    output_table_name = TableName("output_database", input_schema_name.name, input_table_name.name)
                    output_table_definition = TableDefinition(output_table_name, catalog.get_table_definition(input_table_name).columns)
                    catalog.create_schema_if_not_exists(output_table_name.schema_name)
                    catalog.create_table(output_table_definition)
                    connection.execute_command(f"INSERT INTO {output_table_name} (SELECT * FROM {input_table_name})")
                    print(f"Successfully converted table {input_table_name}")
            print(f"Successfully converted {args.input_hyper_file_path} into {args.output_hyper_file_path}")
