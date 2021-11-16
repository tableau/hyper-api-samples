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

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter
import tableauserverclient as TSC
from pathlib import Path
from datetime import datetime
import os, json, sys

orders_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Orders",
    columns=[
        TableDefinition.Column(name="Address ID", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order Date", type=SqlType.date(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Ship Date", type=SqlType.date(), nullability=NULLABLE),
        TableDefinition.Column(name="Ship Mode", type=SqlType.text(), nullability=NULLABLE)
    ]
)

customer_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Customer",
    columns=[
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer Name", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Loyalty Reward Points", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Segment", type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)

def create_hyper_file_and_insert_data(path_to_database):
    """
    Shows how to create a hyper file with multiple tables and how to add assumed constraints such that Tableau Server 
    can infer which data model to analyze in the data source.
    """
    print("Creating hyper file for publishing.")
    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Creates new Hyper file. Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            # Create multiple tables.
            connection.catalog.create_table(table_definition=orders_table)
            connection.catalog.create_table(table_definition=customer_table)
            # Create assumed primary key for the customer table and a foreign key referencing the "Customer ID" in the orders table.
            # This enables Tableau Server to infer which data model to analyze in the data source.
            connection.execute_command(f'ALTER TABLE {customer_table.table_name} ADD ASSUMED PRIMARY KEY ("Customer ID")')
            connection.execute_command(f'''ALTER TABLE {orders_table.table_name} ADD ASSUMED FOREIGN KEY 
                ("Customer ID") REFERENCES  {customer_table.table_name} ( "Customer ID" )''')

            # Insert data into Orders table.
            orders_data_to_insert = [
                [399, "DK-13375", datetime(2012, 9, 7), "CA-2011-100006", datetime(2012, 9, 13), "Standard Class"],
                [530, "EB-13705", datetime(2012, 7, 8), "CA-2011-100090", datetime(2012, 7, 12), "Standard Class"]
            ]

            with Inserter(connection, orders_table) as inserter:
                inserter.add_rows(rows=orders_data_to_insert)
                inserter.execute()

            # Insert data into Customers table.
            customer_data_to_insert = [
                ["DK-13375", "Dennis Kane", 518, "Consumer"],
                ["EB-13705", "Ed Braxton", 815, "Corporate"]
            ]

            with Inserter(connection, customer_table) as inserter:
                inserter.add_rows(rows=customer_data_to_insert)
                inserter.execute()

def publish_hyper(token_name, token_value, site_name, server_address, project_name, path_to_database):
    """
    Shows how to leverage the Tableau Server Client (TSC) to sign in and publish the hyper file directly to Tableau Online/Server.
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
        
        print(f"Publishing {path_to_database} to {project_name}...")
        # Publish datasource
        datasource = server.datasources.publish(datasource, path_to_database, publish_mode)
        print("Datasource published. Datasource ID: {0}".format(datasource.id))

def load_config():
    """
    Loads a config file in the current directory called config.json.
    """
    # Opens the config file and loads as a dictionary.    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            print("Config file loaded.")
            return config
    except:
        message = 'Could not read config file.'
        print("Unexpected error: ", sys.exc_info()[0])
        sys.exit(message)


if __name__ == '__main__':
    config = load_config()
    path_to_database = Path(config['hyper_name'])
    create_hyper_file_and_insert_data(path_to_database)
    publish_hyper(config['tableau_token_name'], config['tableau_token'], config['site_name'], config['server_address'], config['project_name'], path_to_database)
