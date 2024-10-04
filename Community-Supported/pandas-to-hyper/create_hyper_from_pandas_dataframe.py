# Import necessary standard libraries
from pathlib import Path  # For file path manipulations

# Import pandas for DataFrame creation and manipulation
import pandas as pd

# Import necessary classes from the Tableau Hyper API
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    CreateMode,
    NOT_NULLABLE,
    NULLABLE,
    SqlType,
    TableDefinition,
    Inserter,
    HyperException,
)

def run_create_hyper_file_from_dataframe():
    """
    An example demonstrating loading data from a pandas DataFrame into a new Hyper file.
    """

    print("EXAMPLE - Load data from pandas DataFrame into table in new Hyper file")

    # Step 1: Create a sample pandas DataFrame.
    data = {
        "Customer ID": ["DK-13375", "EB-13705", "JH-13600"],
        "Customer Name": ["John Doe", "Jane Smith", "Alice Johnson"],
        "Loyalty Reward Points": [100, 200, 300],
        "Segment": ["Consumer", "Corporate", "Home Office"],
    }
    df = pd.DataFrame(data)

    # Step 2: Define the path where the Hyper file will be saved.
    path_to_database = Path("customer.hyper")

    # Step 3: Optional process parameters.
    # These settings limit the number of log files and their size.
    process_parameters = {
        "log_file_max_count": "2",  # Limit the number of log files to 2
        "log_file_size_limit": "100M",  # Limit the log file size to 100 megabytes
    }

    # Step 4: Start the Hyper Process.
    # Telemetry is set to send usage data to Tableau.
    with HyperProcess(
        telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters
    ) as hyper:

        # Step 5: Optional connection parameters.
        # This sets the locale for time formats to 'en_US'.
        connection_parameters = {"lc_time": "en_US"}

        # Step 6: Create a connection to the Hyper file.
        # If the file exists, it will be replaced.
        with Connection(
            endpoint=hyper.endpoint,
            database=path_to_database,
            create_mode=CreateMode.CREATE_AND_REPLACE,
            parameters=connection_parameters,
        ) as connection:

            # Step 7: Define the table schema.
            customer_table = TableDefinition(
                table_name="Customer",  # Name of the table
                columns=[
                    TableDefinition.Column(
                        "Customer ID", SqlType.text(), NOT_NULLABLE
                    ),
                    TableDefinition.Column(
                        "Customer Name", SqlType.text(), NOT_NULLABLE
                    ),
                    TableDefinition.Column(
                        "Loyalty Reward Points", SqlType.big_int(), NOT_NULLABLE
                    ),
                    TableDefinition.Column("Segment", SqlType.text(), NOT_NULLABLE),
                ],
            )

            # Step 8: Create the table in the Hyper file.
            connection.catalog.create_table(table_definition=customer_table)

            # Step 9: Use the Inserter to insert data into the table.
            with Inserter(connection, customer_table) as inserter:
                # Iterate over the DataFrame rows as tuples.
                # 'itertuples' returns an iterator yielding named tuples.
                for row in df.itertuples(index=False, name=None):
                    inserter.add_row(row)  # Add each row to the inserter
                inserter.execute()  # Execute the insertion into the Hyper file

            # Step 10: Verify the number of rows inserted.
            row_count = connection.execute_scalar_query(
                f"SELECT COUNT(*) FROM {customer_table.table_name}"
            )
            print(
                f"The number of rows in table {customer_table.table_name} is {row_count}."
            )
            print("Data has been successfully inserted into the Hyper file.")

        # The connection is automatically closed when exiting the 'with' block.
        print("The connection to the Hyper file has been closed.")

    # The Hyper process is automatically shut down when exiting the 'with' block.
    print("The Hyper process has been shut down.")


if __name__ == "__main__":
    try:
        run_create_hyper_file_from_dataframe()
    except HyperException as ex:
        print(ex)
        exit(1)
