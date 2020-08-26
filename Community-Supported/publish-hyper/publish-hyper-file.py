from pathlib import Path
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException


# Configure for TSC to publish
# Note: Do not store creds/tokens in plaintext, please use env vars :)
hyper_name = 'customer.hyper'
server_address = 'https://10ax.online.tableau.com/'
site_name = 'mysitename'
project_name = 'myproject'
token_name = 'mytokenname'
token_value = 'tokenherebutpleaseconsidersecuritypolicies'
# For more on tokens, head here:
# https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm

path_to_database = Path(hyper_name)

# The table is called "Extract" and will be created in the "Extract" schema
# and contains four columns.
extract_table = TableDefinition(
    table_name=TableName("Extract", "Extract"),
    columns=[
        TableDefinition.Column(name='Customer ID', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Customer Name', type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Loyalty Reward Points', type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name='Segment', type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)


def insert_data():
    """
    An example demonstrating a simple single-table Hyper file including table creation and data insertion with different types
    This code is lifted from the below example:
    https://github.com/tableau/hyper-api-samples/blob/main/Tableau-Supported/Python/insert_data_into_single_table.py
    """
    print("Creating single table for publishing.")

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
                ["DK-13375", "Dennis Kane", 685, "Consumer"],
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


def publish_hyper():
    """
    Shows how to leverage the Tableau Server Client (TSC) to sign in and publish an extract directly to Tableau Online/Server
    """

    # Sign in to server
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name=token_name, personal_access_token=token_value, site_id=site_name)
    server = TSC.Server(server_address, use_server_version=True)
    
    print(f"Signing into {site_name} at {server_address}")
    with server.auth.sign_in(tableau_auth):
        # Define publish mode - Overwrite, Append, or CreateNew
        publish_mode = TSC.Server.PublishMode.Overwrite
        
        # Get project_id from project_name
        all_projects, pagination_item = server.projects.get()
        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id
    
        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(project_id)
        
        print(f"Publishing {hyper_name} to {project_name}...")
        # Publish datasource
        datasource = server.datasources.publish(datasource, path_to_database, publish_mode)
        print("Datasource published. Datasource ID: {0}".format(datasource.id))


if __name__ == '__main__':
    insert_data()
    publish_hyper()
