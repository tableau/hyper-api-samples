# flights-data-incremental-refresh
## __Incremental Refresh using the OpenSkyApi__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This sample is based on the content the Hyper team presented in the Hands on Training session "Hands-on: Leverage the Hyper Update API and Hyper API to Keep Your Data Fresh on Tableau Server" at Tableau Conference 2022 ([slides available here](https://mkt.tableau.com/tc22/sessions/live/430-HOT-D1_Hands-onLeverageTheHyperUpdate.pdf)).

This script pulls down flights data from the [OpenSkyAPI](https://github.com/openskynetwork/opensky-api), creates a hyper database with this data and uses the [Hyper Update API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_how_to_update_data_to_hyper.htm) to implement an incremental refresh on your Tableau Server/Cloud. The first time this script is executed, the database file is simply published. 

# Get started

## __Prerequisites__
To run the script, you will need:
- Windows, Linux, or Mac
- Python 3
- Run `pip install -r requirements.txt`
- Tableau Server Credentials, see below. 

## Tableau Server Credentials
To run this sample with your Tableau Server/Cloud, you first need to get the following information:
- Tableau Server Url, e.g. 'https://us-west-2a.online.tableau.com'
- Site name, e.g., use 'default' for your default site (note that you cannot use 'default' in Tableau Cloud but must use the site name)
- Project name, e.g., use an empty string ('') for your default project 
- [Token Name and Token Value](https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm) 

Ensure that you have installed the requirements and then just run the sample Python file with the information from above. The syntax for running the script is:
   
   **python flights-data-incremental-refresh.py [-h] server_url site_name project_name token_name token_value**

# Incremental Refresh using the OpenSkyApi
The script consists of two parts: first it creates a Hyper database with flights data and then publishes the database to Tableau Server/Cloud.

## Create a database with flights data
The `create_hyper_database_with_flights_data` method creates an instance of the `OpenSkyAPI` and then pulls down states within a specific bounding box. This example just uses a subset of the available data as we are using the free version of the OpenSkyApi. 

Then, a Hyper database is created with a table with name `TableName("public", "flights")`. Finally, an inserter is used to insert the flights data. 

## Publish the hyper database to Tableau Server / Cloud
The `publish_to_server` method first signs into Tableau Server / Cloud. Then, it finds the respective project to which the database should be published to. 

There are two cases for publishing the database to Server: 
- No datasource with name `datasource_name_on_server` exists on Tableau Server. In this case, the script simply creates the initial datasource on Tableau server. This datasource is needed for the subsequent incremental refreshes as the data will be added to this datasource. 
- The datasource with name `datasource_name_on_server` already exists on Tableau Server. In this case, the script uses the Hyper Update REST API to insert the data from the database into the respective table in the datasource on Tableau Server/Cloud.

## __Resources__
Check out these resources to learn more:
- [Hyper API documentation](https://tableau.github.io/hyper-db)
- [Hyper Update API documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_how_to_update_data_to_hyper.htm)
- [Tableau Server Client Docs](https://tableau.github.io/server-client-python/docs/)
- [REST API documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
- [Tableau Tools](https://github.com/bryantbhowell/tableau_tools)
