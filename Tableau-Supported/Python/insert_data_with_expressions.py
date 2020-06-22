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
        TableDefinition.Column(name='Order ID', type=SqlType.int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Ship Timestamp', type=SqlType.timestamp(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Ship Mode', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Ship Priority', type=SqlType.int(), nullability=NOT_NULLABLE)
    ]
)


def run_insert_data_with_expressions():
    """
    An example of how to push down computations to Hyper during insertion with expressions.
    """
    print("EXAMPLE - Push down computations to Hyper during insertion with expressions")
    path_to_database = Path("orders.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "orders.hyper".
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
            inserter_definition = [
                TableDefinition.Column(name='Order ID', type=SqlType.int(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name='Ship Timestamp Text', type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name='Ship Mode', type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name='Ship Priority Text', type=SqlType.text(), nullability=NOT_NULLABLE)]

            # Column 'Order Id' is inserted into "Extract"."Extract" as-is
            # Column 'Ship Timestamp' in "Extract"."Extract" of timestamp type is computed from Column 'Ship Timestamp Text' of text type using 'to_timestamp()'
            # Column 'Ship Mode' is inserted into "Extract"."Extract" as-is
            # Column 'Ship Priority' is "Extract"."Extract" of integer type is computed from Colum 'Ship Priority Text' of text type using 'CASE' statement
            shipPriorityAsIntCaseExpression = f'CASE {escape_name("Ship Priority Text")} ' \
                f'WHEN {escape_string_literal("Urgent")} THEN 1 ' \
                f'WHEN {escape_string_literal("Medium")} THEN 2 ' \
                f'WHEN {escape_string_literal("Low")} THEN 3 END'

            column_mappings = [
                'Order ID',
                Inserter.ColumnMapping(
                    'Ship Timestamp', f'to_timestamp({escape_name("Ship Timestamp Text")}, {escape_string_literal("YYYY-MM-DD HH24:MI:SS")})'),
                'Ship Mode',
                Inserter.ColumnMapping('Ship Priority', shipPriorityAsIntCaseExpression)
            ]

            # Data to be inserted
            data_to_insert = [
                [399, '2012-09-13 10:00:00', 'Express Class', 'Urgent'],
                [530, '2012-07-12 14:00:00', 'Standard Class', 'Low']
            ]

            # Insert data into "Extract"."Extract" table with expressions
            with Inserter(connection, extract_table, column_mappings, inserter_definition=inserter_definition) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()
            print("The data was added to the table.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_data_with_expressions()
    except HyperException as ex:
        print(ex)
        exit(1)
