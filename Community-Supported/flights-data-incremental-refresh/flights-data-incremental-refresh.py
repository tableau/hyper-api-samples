from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, TableName, CreateMode, SqlType, Nullability, Inserter
from opensky_api import OpenSkyApi
import tableauserverclient as TSC
import uuid

def create_hyper_database_with_flights_data(database_path):
    """
    Leverages the OpenSkyAPI (https://github.com/openskynetwork/opensky-api) to create a 
    Hyper database with flights data.  
    """
    # Create an instance of the opensky api to retrieve data from OpenSky via HTTP.
    opensky = OpenSkyApi()
    # Get the most recent state vector. Note that we can only call this method every 
    # 10 seconds as we are using the free version of the API.
    states = opensky.get_states(bbox=(45.8389, 47.8229, 5.9962, 10.5226))

    # Start up a local Hyper process. 
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Create a connection to the Hyper process and connect to a hyper file 
        # (create the file and replace if it exists).
        with Connection(endpoint=hyper.endpoint, database=database_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            # Create a table definition with table name "flights" in the "public" schema 
            # and columns for airport data.
            table_definition = TableDefinition(
                table_name=TableName("public", "flights"),
                columns=[
                    TableDefinition.Column('baro_altitude', SqlType.double(), Nullability.NULLABLE),
                    TableDefinition.Column('callsign', SqlType.text(), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('latitude', SqlType.double(), Nullability.NULLABLE),
                    TableDefinition.Column('longitude', SqlType.double(), Nullability.NULLABLE),
                    TableDefinition.Column('on_ground', SqlType.bool(), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('origin_country', SqlType.text(), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('time_position', SqlType.int(), Nullability.NULLABLE),
                    TableDefinition.Column('velocity', SqlType.double(), Nullability.NULLABLE),
                ])
            # Create the flights table.
            connection.catalog.create_table(table_definition)

            # Insert each of the states into the table.
            with Inserter(connection, table_definition) as inserter:
                for s in states.states:
                    inserter.add_row([s.baro_altitude, s.callsign, s.latitude, s.longitude, s.on_ground, s.origin_country, s.time_position, s.velocity])
                inserter.execute()

            num_flights = connection.execute_scalar_query(query=f"SELECT COUNT(*) from {table_definition.table_name}")
            print(f"Inserted {num_flights} flights into {database_path}.")

def publish_to_server(server_url, tableau_auth, project_name, database_path, datasource_name_on_server):
    """
    Creates the datasource on Tableau Server if it has not yet been created. Otherwise, uses the 
    Hyper Update REST API (https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_how_to_update_data_to_hyper.htm) to append the data to the datasource.
    """
    # Create a tableuserverclient object to interact with Tableau Server.
    server = TSC.Server(server_url, use_server_version=True)
    # Sign into Tableau Server with the above authentication information.
    with server.auth.sign_in(tableau_auth):
        # Get project_id from project_name.
        matching_projects = server.projects.filter(name=project_name)
        project_id = next((project.id for project in matching_projects if project.name == project_name), None)
        if project_id is None:
            print(f"Publish failed. The specified project '{project_name}' does not exist.")
            exit()

        # Get the datasource from Server (if it exists).
        matching_datasources = server.datasources.filter(name=datasource_name_on_server)
        datasource = next((ds for ds in matching_datasources), None)

        if datasource is None:
            # If the datasource does not exist on server, publish the datasource. 
            publish_mode = TSC.Server.PublishMode.CreateNew
            datasource = TSC.DatasourceItem(project_id)
            # Set the name of the datasource such that it can be easily identified.
            datasource.name = datasource_name_on_server
            datasource = server.datasources.publish(datasource, database_path, publish_mode)
            print(f"New datasource published: (id : {datasource.id}, name: {datasource.name}).")
        else:
            # If the datasource already exists on Tableau Server, use the Hyper Update REST API 
            # to send the delta to Tableau Server and insert the data into the respective table
            # in the datasource. 

            # Create a new random request id. 
            request_id = str(uuid.uuid4())

            # Create one action that inserts from the new table into the existing table.
            actions = [
                {
                    "action": "insert",
                    "source-schema": "public",
                    "source-table": "flights",
                    "target-schema": "public",
                    "target-table": "flights",
                }
            ]

            # Start the update job on Server.
            job = server.datasources.update_hyper_data(datasource.id, request_id=request_id, actions=actions, payload=database_path)
            print(f"Update job posted (ID: {job.id}). Waiting for the job to complete...")

            # Wait for the job to finish.
            job = server.jobs.wait_for_job(job)
            print("Job finished successfully")  
       
        
if __name__ == '__main__':
    # First create the hyper database with flights data.
    database_path = "flights.hyper"
    create_hyper_database_with_flights_data(database_path)

    # Then publish the data to server.
    server_url = ''
    site_name = ''
    project_name = ''
    token_name = ''
    token_value = ''
    datasource_name_on_server = 'flights_data_set'
    # Create credentials to sign into Tableau Server.
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_name)
    publish_to_server(server_url, tableau_auth, project_name, database_path, datasource_name_on_server)