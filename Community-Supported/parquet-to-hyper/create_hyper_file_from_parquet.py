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
from datetime import date
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException


def run_create_hyper_file_from_parquet(
        parquet_file_path: Path,
        table_definition: TableDefinition,
        hyper_database_path: Path):
    """
    An example demonstrating how to load rows from an Apache Parquet file (`parquet_file_path`)
    into a new Hyper file (`hyper_database_path`) using the COPY command. Currently the
    table definition of the data to copy needs to be known and explicitly specified.

    Reading Parquet data is analogous to reading CSV data. For more details, see:
    https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_insert_csv.html
    """

    # Start the Hyper process.
    #
    # * Since `FORMAT parquet` is an experimental feature, it must be explicitly enabled on startup.
    #   See also the "Experimental Settings" section in the Tableau Hyper documentation:
    #   https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/experimentalsettings.html
    # * Sending telemetry data to Tableau is encouraged when trying out an experimental feature.
    #   To opt out, simply set `telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU` below.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU,
                      parameters={"experimental_external_format_parquet": "1"}) as hyper:

        # Open a connection to the Hyper process. This will also create the new Hyper file.
        # The `CREATE_AND_REPLACE` mode causes the file to be replaced if it
        # already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=hyper_database_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            # Create the target table.
            connection.catalog.create_table(table_definition=table_definition)

            # Execute a COPY command to insert the data from the Parquet file.
            copy_command = f"COPY {table_definition.table_name} FROM {escape_string_literal(parquet_file_path)} WITH (FORMAT PARQUET)"
            print(copy_command)
            count_inserted = connection.execute_command(copy_command)
            print(f"-- {count_inserted} rows have been copied from '{parquet_file_path}' to the table {table_definition.table_name} in '{hyper_database_path}'.")


if __name__ == '__main__':
    try:
        # The `orders` table to read from the Parquet file.
        table_definition = TableDefinition(
            table_name="orders",
            columns=[
                TableDefinition.Column("o_orderkey", SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column("o_custkey", SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column("o_orderstatus", SqlType.text(), NOT_NULLABLE),
                TableDefinition.Column("o_totalprice", SqlType.numeric(8, 2), NOT_NULLABLE),
                TableDefinition.Column("o_orderdate", SqlType.date(), NOT_NULLABLE),
                TableDefinition.Column("o_orderpriority", SqlType.text(), NOT_NULLABLE),
                TableDefinition.Column("o_clerk", SqlType.text(), NOT_NULLABLE),
                TableDefinition.Column("o_shippriority", SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column("o_comment", SqlType.text(), NOT_NULLABLE),
            ]
        )

        run_create_hyper_file_from_parquet(
            "orders_10rows.parquet",
            table_definition,
            "orders.hyper")

    except HyperException as ex:
        print(ex)
        exit(1)
