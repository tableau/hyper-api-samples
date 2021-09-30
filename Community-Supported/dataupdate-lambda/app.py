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

import requests, os, json, sys, datetime, uuid
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException



# This is basically so that Lambda knows what to do.
def handler(event, context):
    print("Handler...")
    os.chdir("/tmp")

    # Call main function
    main()
    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({"message": "Lambda evoked",
                            "event": event})
    }


def main():
    # # # # # # # # # # # # # # # # # # # # # # # #
    #   Ensure this hyper file perfectly matches  #
    #       the columns/schema of the target      #
    # # # # # # # # # # # # # # # # # # # # # # # #

    schema = TableDefinition(TableName('Extract', 'Extract'), [
        TableDefinition.Column('Name', SqlType.text()),
        TableDefinition.Column('Symbol', SqlType.text()),
        TableDefinition.Column('Price', SqlType.double()),
        TableDefinition.Column('Volume', SqlType.double()),
        TableDefinition.Column('Market Cap', SqlType.double()),
        TableDefinition.Column('Date Time', SqlType.timestamp())
    ])

    # Set the .hyper file name
    hyper_file = "changeset.hyper"


    # # # # # # # # # # # # # # # # # # # # #
    #  Below is where you will modify the   #
    # sample to fit your specific use case. #
    # # # # # # # # # # # # # # # # # # # # #

    # Get data from API (or wherever!)
    url = "https://api.coinstats.app/public/v1/coins?skip=0&limit=5&currency=USD"
    response = requests.get(url)
    

    # Parse data & append to nested list
    data = json.loads(response.text)
    new_data = []
    dt = datetime.datetime.now()
    
    # Create list of new rows
    for coin in data['coins']:
        row = [coin['name'], coin['symbol'], coin['price'], coin['volume'], coin['marketCap'], dt]
        new_data.append(row)



    # Create changeset .hyper file
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, hyper_file, CreateMode.CREATE_AND_REPLACE) as connection:
            # Create a schema called 'Extract'
            connection.catalog.create_schema('Extract')
            
            # Create a table called 'new rows' for the rows to be inserted, and insert the data
            table_def = TableDefinition(TableName('Extract', 'new_rows'), schema.columns)
            connection.catalog.create_table(table_def)
            
            with Inserter(connection, table_def) as inserter:
                for d in new_data:
                    inserter.add_row(d)
                inserter.execute()
            


    # Use our change-set to update our data source via 'actions'
    # INSERT latest prices from 'new_rows' to 'Extract'
    actions = [
        {
            "action": "insert", 
            "source-schema":"Extract", "source-table": "new_rows",
            "target-schema": "Extract", "target-table": "Extract"
        }
    ]


    # Set up environmental variables for publishing
    # These will be set from the AWS Environmental Variables config
    ds_name = os.environ['DATASOURCE'] # ex. 'CryptoPrices'
    server_address = os.environ['SERVER'] # ex. 'https://10ax.online.tableau.com/'
    tableau_token_name = os.environ['TOKEN_NAME'] # ex. 'user@gmail.com'
    tableau_token = os.environ['TOKEN']
    site_name = os.environ['SITE'] # ex. 'sales'
    project_name = os.environ['PROJECT']


    # Creates the auth object.
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=tableau_token_name, personal_access_token=tableau_token, site_id=site_name)
    server = TSC.Server(server_address)
    server.use_server_version()

    # Signs in and finds the specified project.
    with server.auth.sign_in(tableau_auth):
        project_id = None
        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id
        if project_id == None:
            message = "Could not find project."
            sys.exit(message)
        
        # Set options to filter to datasource name
        options = TSC.RequestOptions()
        options.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals, 
                                     ds_name))

        # Set datasource object
        ds_list, _ = server.datasources.get(req_options=options)
        for ds in ds_list:
            if ds.name == ds_name:
                datasource = ds
        
        # Create random request ID
        request_id = str(uuid.uuid4())
        
        # Run PATCH request!
        job = server.datasources.update_data(datasource.id, request_id=request_id, actions=actions, payload=hyper_file)
        
        # Log on console and clean up
        print(job)
        os.remove(hyper_file)