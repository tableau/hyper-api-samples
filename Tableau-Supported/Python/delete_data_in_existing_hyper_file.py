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
    HyperException


def run_delete_data_in_existing_hyper_file():
    """
    An example of how to delete data in an existing Hyper file.
    """
    print("EXAMPLE - Delete data from an existing Hyper file")

    # Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table.
    # See "insert_data_into_multiple_tables.py" for an example that works with the complete schema.
    path_to_source_database = Path(__file__).parent / "data" / "superstore_sample.hyper"

    # Make a copy of the superstore example Hyper file.
    path_to_database = Path(shutil.copy(path_to_source_database, "superstore_sample_delete.hyper")).resolve()

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "superstore_sample_delete.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            print(f"Delete all rows from customer with the name 'Dennis Kane' from table {escape_name('Orders')}.")
            # `execute_command` executes a SQL statement and returns the impacted row count.
            row_count = connection.execute_command(
                command=f"DELETE FROM {escape_name('Orders')} "
                f"WHERE {escape_name('Customer ID')} = ANY("
                f"SELECT {escape_name('Customer ID')} FROM {escape_name('Customer')} "
                f"WHERE {escape_name('Customer Name')} = {escape_string_literal('Dennis Kane')})")

            print(f"The number of deleted rows in table {escape_name('Orders')} "
                  f"is {row_count}.\n")

            print(f"Delete all rows from customer with the name 'Dennis Kane' from table {escape_name('Customer')}.")
            row_count = connection.execute_command(
                command=f"DELETE FROM {escape_name('Customer')} "
                f"WHERE {escape_name('Customer Name')} = {escape_string_literal('Dennis Kane')}")

            print(f"The number of deleted rows in table Customer is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_delete_data_in_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
