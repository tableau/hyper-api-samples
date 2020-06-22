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
    TableName, Name, \
    HyperException

# The table is called "Extract" and will be created in the "Extract" schema.
# This has historically been the default table name and schema for extracts created by Tableau
extract_table = TableDefinition(
    table_name=TableName("Extract", "Extract"),
    columns=[
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Location', type=SqlType.geography(), nullability=NOT_NULLABLE)
    ]
)


def run_insert_spatial_data_to_a_hyper_file():
    """
    An example of how to add spatial data to a Hyper file.
    """
    print("EXAMPLE - Add spatial data to a Hyper file ")
    path_to_database = Path("spatial_data.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "spatial_data.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=extract_table)

            # Hyper API's Inserter allows users to transform data during insertion.
            # To make use of data transformation during insertion, the inserter requires the following inputs
            #   1. The connection to the Hyper instance containing the table.
            #   2. The table name or table defintion into which data is inserted.
            #   3. List of Inserter.ColumnMapping.
            #       This list informs the inserter how each column in the target table is tranformed.
            #       The list must contain all the columns into which data is inserted.
            #       "Inserter.ColumnMapping" maps a valid SQL expression (if any) to a column in the target table.
            #       For example Inserter.ColumnMapping('target_column_name', f'{escape_name("colA")}*{escape_name("colB")}')
            #       The column "target_column" contains the product of "colA" and "colB" after successful insertion.
            #       SQL expression string is optional in Inserter.ColumnMapping.
            #       For a column without any transformation only the column name is required.
            #       For example Inserter.ColumnMapping('no_data_transformation_column')
            #   4. The Column Definition of all input values provided to the Inserter

            # Inserter definition contains the column definition for the values that are inserted
            # The data input has two text values Name and Location_as_text
            inserter_definition = [
                TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name='Location_as_text', type=SqlType.text(), nullability=NOT_NULLABLE)]

            # Column 'Name' is inserted into "Extract"."Extract" as-is.
            # Column 'Location' in "Extract"."Extract" of geography type is computed from Column 'Location_as_text' of text type
            # using the expression 'CAST("Location_as_text") AS GEOGRAPHY'.
            # Inserter.ColumnMapping is used for mapping the CAST expression to Column 'Location'.
            column_mappings = [
                'Name',
                Inserter.ColumnMapping('Location', f'CAST({escape_name("Location_as_text")} AS GEOGRAPHY)')
            ]

            # Data to be inserted.
            data_to_insert = [
                ['Seattle', "point(-122.338083 47.647528)"],
                ['Munich', "point(11.584329 48.139257)"]
            ]

            # Insert data into "Extract"."Extract" table with CAST expression.
            with Inserter(connection, extract_table, column_mappings, inserter_definition=inserter_definition) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()
            print("The data was added to the table.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_spatial_data_to_a_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
