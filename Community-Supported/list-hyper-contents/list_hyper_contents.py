# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------
import argparse
from pathlib import PurePath
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, Nullability

# Lists the schemas, tables, and columns inside a Hyper file

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="List the schemas, tables, and columns inside a Hyper file")
    argparser.add_argument("file", type=PurePath, help="The input Hyper file")
    args = argparser.parse_args()

    # Start Hyper and connect to our Hyper file
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, args.file, CreateMode.NONE) as connection:
            # The `connection.catalog` provides us with access to the meta-data we are interested in
            catalog = connection.catalog

            # Iterate over all schemas and print them
            schemas = catalog.get_schema_names()
            print(f"{len(schemas)} schemas:")
            for schema_name in schemas:
                # For each schema, iterate over all tables and print them
                tables = catalog.get_table_names(schema=schema_name)
                print(f" * Schema {schema_name}: {len(tables)} tables")
                for table in tables:
                    # For each table, iterate over all columns and print them
                    table_definition = catalog.get_table_definition(name=table)
                    print(f"  -> Table {table.name}: {len(table_definition.columns)} columns")
                    for column in table_definition.columns:
                        nullability = " NOT NULL" if column.nullability == Nullability.NOT_NULLABLE else ""
                        collation = " " + column.collation if column.collation is not None else ""
                        print(f"    -> {column.name} {column.type}{nullability}{collation}")
