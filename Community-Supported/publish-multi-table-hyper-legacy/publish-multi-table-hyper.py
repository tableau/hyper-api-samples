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

from tableauhyperapi import *
from tableau_tools import *
from tableau_tools.tableau_documents import *
import tableauserverclient as TSC
import os, json, sys

def get_data():
    '''This function is responsible for returning the two tables as nested arrays, as shown with the example below.'''

    # Sample data held as arrays. Column order must be consistent and match the table definitions defined below.
    table_one = [[123, 40, 'John', 'Order#1'], [123, 90, 'Jane', 'Order#2'], [456, 110, 'John', 'Order#3'], [456, 80, 'Jane', 'Order#4']]
    table_two = [[123, 'Lemonade', 'Beverage'], [456, 'Cookie', 'Food']]
    
    # Create an array of the data_tables to pass to Hyper
    table_data = [table_one, table_two]
    
    return table_data

def build_tables():
    '''Builds the two tables for the multitable extract.'''
    # Since the table names are not prefixed with an explicit schema name, the tables will reside in the default "public" namespace.
    # It is important to match the order of the table definitions with the data tables returned in get_data()
    table_one = TableDefinition(
        table_name="sales", 
        columns=[
            TableDefinition.Column("Product Key", SqlType.int()),
            TableDefinition.Column("Sales", SqlType.int()),
            TableDefinition.Column("Customer", SqlType.text()),
            TableDefinition.Column("Order ID", SqlType.text())
        ]
    )
    table_two = TableDefinition(
        table_name="products", 
        columns=[
            TableDefinition.Column("Product Key", SqlType.int()),
            TableDefinition.Column("Product Name", SqlType.text()),
            TableDefinition.Column("Category", SqlType.text())
        ]
    )
    table_definitions = [table_one, table_two]
    return table_definitions


def load_config():
    '''Loads a config file in the current directory called config.json.'''
    
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


def add_to_hyper(table_data, table_definitions, hyper_name):
    '''Uses the Hyper API to build and insert data into the Hyper file.'''

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.   
    print("Starting Hyper process.")
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "[hyper_name].hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        print("Opening connection to Hyper file.")
        with Connection(endpoint=hyper.endpoint, database=hyper_name, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            
            # Creates multiple tables.
            for data, definition in zip(table_data, table_definitions):
                connection.catalog.create_table(definition)
                print(f"Creating table {definition.table_name} in Hyper...")
                
                # Inserts data into table.
                with Inserter(connection, definition) as inserter:
                    print(f"Instering {len(data)} rows into table {definition.table_name}...")
                    inserter.add_rows(data)
                    inserter.execute()

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


def swap_hyper(hyper_name, tdsx_name, logger_obj=None):
    '''Uses tableau_tools to open a local .tdsx file and replace the hyperfile.'''
    
    # Checks to see if TDSX exists, otherwise, as a one-time step, user will need to create using Desktop.
    if os.path.exists(tdsx_name):
        print("Found TDSX file.")
    else:
        message = "--Could not find existing TDSX file. Please use Desktop to create one from the newly created hyper file or update the config file.--"
        sys.exit(message)
    
    # Uses tableau_tools to replace the hyper file in the TDSX.
    try:
        local_tds = TableauFileManager.open(filename=tdsx_name, logger_obj=logger_obj)
    except TableauException as e:
        sys.exit(e)
    filenames = local_tds.get_filenames_in_package()
    for filename in filenames:
        if filename.find('.hyper') != -1:
            print("Overwritting Hyper in original TDSX...")
            local_tds.set_file_for_replacement(filename_in_package=filename,
                                            replacement_filname_on_disk=hyper_name)
            break
    
    # Overwrites the original TDSX file locally.
    tdsx_name_before_extension, tdsx_name_extension = os.path.splitext(tdsx_name)
    tdsx_updated_name = tdsx_name_before_extension + '_updated' + tdsx_name_extension
    local_tds.save_new_file(new_filename_no_extension=tdsx_updated_name)
    os.remove(tdsx_name)
    os.rename(tdsx_updated_name, tdsx_name)


def publish_to_server(site_name, server_address, project_name, tdsx_name, tableau_token_name, tableau_token):
    '''Publishes updated, local .tdsx to Tableau, overwriting the original file.'''
    
    # Creates the auth object based on the config file.
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=tableau_token_name, personal_access_token=tableau_token, site_id=site_name)
    server = TSC.Server(server_address)
    print(f"Signing into to site: {site_name}.")

    # Signs in and find the specified project.
    with server.auth.sign_in(tableau_auth):
        all_projects, pagination_item = server.projects.get()
        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id
        if project_id == None:
            message = "Could not find project. Please update the config file."
            sys.exit(message)
        print(f"Publishing to {project_name}.")
        
        # Publishes the data source.
        overwrite_true = TSC.Server.PublishMode.Overwrite
        datasource = TSC.DatasourceItem(project_id)
        file_path = os.path.join(os.getcwd(), tdsx_name)
        datasource = server.datasources.publish(
            datasource, file_path, overwrite_true)
        print(f"Publishing of datasource '{tdsx_name}' complete.")


# Run
if __name__ == '__main__':
    config = load_config() 
    
    try:
        add_to_hyper(get_data(), build_tables(), config['hyper_name'])
    except HyperException as e:
        sys.exit(e)

    swap_hyper(config['hyper_name'], config['tdsx_name'])
    publish_to_server(config['site_name'], config['server_address'], config['project_name'],
        config['tdsx_name'], config['tableau_token_name'], config['tableau_token'])