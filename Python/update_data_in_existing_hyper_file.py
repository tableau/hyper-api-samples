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


def run_update_data_in_existing_hyper_file():
    """
    An example of how to update data in an existing Hyper file.
    """
    print("EXAMPLE - Update existing data in an Hyper file")

    # Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table.
    # See "insert_data_into_multiple_tables.py" for an example that works with the complete schema.
    path_to_source_database = Path(__file__).parent / "data" / "superstore_sample.hyper"

    # Make a copy of the superstore sample Hyper file.
    path_to_database = Path(shutil.copy(path_to_source_database, "superstore_sample_update.hyper")).resolve()

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "superstore_sample_update.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            rows_pre_update = connection.execute_list_query(
                query=f"SELECT {escape_name('Loyalty Reward Points')}, {escape_name('Segment')}"
                f"FROM {escape_name('Customer')}")
            print(f"Pre-Update: Individual rows showing 'Loyalty Reward Points' and 'Segment' "
                  f"columns: {rows_pre_update}\n")

            print("Update 'Customers' table by adding 50 Loyalty Reward Points to all Corporate Customers.")
            row_count = connection.execute_command(
                command=f"UPDATE {escape_name('Customer')} "
                f"SET {escape_name('Loyalty Reward Points')} = {escape_name('Loyalty Reward Points')} + 50 "
                f"WHERE {escape_name('Segment')} = {escape_string_literal('Corporate')}")

            print(f"The number of updated rows in table {escape_name('Customer')} is {row_count}")

            rows_post_update = connection.execute_list_query(
                query=f"SELECT {escape_name('Loyalty Reward Points')}, {escape_name('Segment')} "
                f"FROM {escape_name('Customer')}")
            print(f"Post-Update: Individual rows showing 'Loyalty Reward Points' and 'Segment' "
                  f"columns: {rows_post_update}")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_update_data_in_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
