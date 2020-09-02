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
import pandas as pd
import pantab
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException

# An example of how to turn a .hyper file into a csv to fit within potiential ETL workflows.

"""
Note: you need to follow the pantab documentation to make sure columns line up with the
appropriate datatypes. For example, my first try at this failed because my text() columns
were NOT_NULLABLE, which conflicts with what pantab expects.
More here:
https://pantab.readthedocs.io/en/latest/caveats.html

"""

# Change these to match your use case.
hyper_name = "hyper_for_csv.hyper"
my_table = TableName("Extract", "Extract")
output_name = "output.csv"

path_to_database = Path(hyper_name)


# The table is called "Extract" and will be created in the "Extract" schema
# and contains four columns.
extract_table = TableDefinition(
    table_name=my_table,
    columns=[
        TableDefinition.Column(name='Customer ID', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Customer Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Loyalty Reward Points', type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Segment', type=SqlType.text(), nullability=NULLABLE)
    ]
)


def insert_data():
    """
    Creates a simple .hyper file. For more on this, see the below example:
    https://github.com/tableau/hyper-api-samples/blob/main/Tableau-Supported/Python/insert_data_into_single_table.py
    """
    print("Creating single table for conversion.")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            # Creates schema and table with table_definition.
            connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=extract_table)

            # The rows to insert into the "Extract"."Extract" table.
            data_to_insert = [
                ["DK-13375", "Dennis Kane", 685, "Consumer"],
                ["EB-13705", "Ed Braxton", 815, "Corporate"]
            ]

            # Insert the data.
            with Inserter(connection, extract_table) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


def convert_to_csv():
    """
    Leverages pantab and pandas to convert a .hyper file to a df, and then convert
    the df to a csv file.
    """

    # Uses pantab to convert the hyper file to a df.
    df = pantab.frame_from_hyper(hyper_name, table=my_table)
    print("Printing table:")
    print(df)
    
    print("Converting to CSV...")

    # Simple pandas->csv operation.
    df.to_csv(output_name)


# Run it!
if __name__ == '__main__':
    insert_data()
    convert_to_csv()
    print("All done!")
