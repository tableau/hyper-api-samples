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
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException

# The table is called "Extract" and will be created in the "Extract" schema.
# This has historically been the default table name and schema for extracts created by Tableau
extract_table = TableDefinition(
    table_name=TableName("Extract", "Extract"),
    columns=[
        TableDefinition.Column(name='Customer ID', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Customer Name', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Loyalty Reward Points', type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Segment', type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)


def run_insert_data_into_single_table():
    """
    An example demonstrating a simple single-table Hyper file including table creation and data insertion with different types
    """
    print("EXAMPLE - Insert data into a single table within a new Hyper file")
    path_to_database = Path("customer.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=extract_table)

            # The rows to insert into the "Extract"."Extract" table.
            data_to_insert = [
                ["DK-13375", "Dennis Kane", 518, "Consumer"],
                ["EB-13705", "Ed Braxton", 815, "Corporate"]
            ]

            with Inserter(connection, extract_table) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()

            # The table names in the "Extract" schema (the default schema).
            table_names = connection.catalog.get_table_names("Extract")
            print(f"Tables available in {path_to_database} are: {table_names}")

            # Number of rows in the "Extract"."Extract" table.
            # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
            row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {extract_table.table_name}")
            print(f"The number of rows in table {extract_table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_data_into_single_table()
    except HyperException as ex:
        print(ex)
        exit(1)
