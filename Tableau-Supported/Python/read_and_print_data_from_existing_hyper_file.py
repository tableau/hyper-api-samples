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
import shutil

from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException


def run_read_data_from_existing_hyper_file():
    """
    An example of how to read and print data from an existing Hyper file.
    """
    print("EXAMPLE - Read data from an existing Hyper file")

    # Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table.
    # See "insert_data_into_multiple_tables.py" for an example that works with the complete schema.
    path_to_source_database = Path(__file__).parent / "data" / "superstore_sample_denormalized.hyper"

    # Make a copy of the superstore denormalized sample Hyper file
    path_to_database = Path(shutil.copy(src=path_to_source_database,
                                        dst="superstore_sample_denormalized_read.hyper")).resolve()

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "superstore_sample_denormalized_read.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:
            # The table names in the "Extract" schema (the default schema).
            table_names = connection.catalog.get_table_names(schema="Extract")

            for table in table_names:
                table_definition = connection.catalog.get_table_definition(name=table)
                print(f"Table {table.name} has qualified name: {table}")
                for column in table_definition.columns:
                    print(f"Column {column.name} has type={column.type} and nullability={column.nullability}")
                print("")

            # Print all rows from the "Extract"."Extract" table.
            table_name = TableName("Extract", "Extract")
            print(f"These are all rows in the table {table_name}:")
            # `execute_list_query` executes a SQL query and returns the result as list of rows of data,
            # each represented by a list of objects.
            rows_in_table = connection.execute_list_query(query=f"SELECT * FROM {table_name}")
            print(rows_in_table)

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_read_data_from_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
