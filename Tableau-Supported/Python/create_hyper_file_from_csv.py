from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException


customer_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Customer",
    columns=[
        TableDefinition.Column("Customer ID", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Customer Name", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Loyalty Reward Points", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("Segment", SqlType.text(), NOT_NULLABLE)
    ]
)


def run_create_hyper_file_from_csv():
    """
    An example demonstrating loading data from a csv into a new Hyper file
    For more details, see https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_insert_csv.html
    """
    print("EXAMPLE - Load data from CSV into table in new Hyper file")

    path_to_database = Path("customer.hyper")

    # Optional process parameters.
    # They are documented in the Tableau Hyper documentation, chapter "Process Settings"
    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
    process_parameters = {
        # Limits the number of Hyper event log files to two.
        "log_file_max_count": "2",
        # Limits the size of Hyper event log files to 100 megabytes.
        "log_file_size_limit": "100M"
    }

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:

        # Optional connection parameters.
        # They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
        # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
        connection_parameters = {"lc_time": "en_US"}

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameters=connection_parameters) as connection:

            connection.catalog.create_table(table_definition=customer_table)

            # Using path to current file, create a path that locates CSV file packaged with these examples.
            path_to_csv = str(Path(__file__).parent / "data" / "customers.csv")

            # Load all rows into "Customers" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.
            #
            # Note:
            # You might have to adjust the COPY parameters to the format of your specific csv file.
            # The example assumes that your columns are separated with the ',' character
            # and that NULL values are encoded via the string 'NULL'.
            # Also be aware that the `header` option is used in this example:
            # It treats the first line of the csv file as a header and does not import it.
            #
            # The parameters of the COPY command are documented in the Tableau Hyper SQL documentation
            # (https:#help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql-copy.html).
            print("Issuing the SQL COPY command to load the csv file into the table. Since the first line")
            print("of our csv file contains the column names, we use the `header` option to skip it.")
            count_in_customer_table = connection.execute_command(
                command=f"COPY {customer_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header)")

            print(f"The number of rows in table {customer_table.table_name} is {count_in_customer_table}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
