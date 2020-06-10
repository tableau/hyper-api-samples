# s3-to-hyper
## __Dynamically Creating and Publishing Hyper Files from S3__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 0.1

This sample demonstrates how to, with little modification, leverage the Hyper API, Tableau Server Client Library, Tableau Tools, and Boto3 to do the following:
- Download a header file and un-unioned CSVs to pass to hyper
- Create an empty hyper file with columns defined in the header file
- Copies the CSVs to the hyper file based on a wildcard pattern match
- Swaps the newly created extract into a Packaged Data Source file (.tdsx)
- Publishes the data source to a specified project on Tableau Online/Server

It should serve as a starting point for anyone looking to automate the publishing process of datasources based on contents of S3 buckets. The advantage of leveraging this sample is that an end user should not need to open the python script and edit the table configuration; the code handles that automatically. In that way, this is intended to be 'point and shoot'.


# Get started

## __Prerequisites__
To run the script, you will need:
- Windows or Mac
- Tableau Desktop v10.5 or higher
- Python 3.7
- Run `pip install -r requirements.txt`
- Tableau Online/Server credentials or personal access token
- AWS Credentials File

## __Configuration File__
You will need to modify `config.json` and add fields that include:
- `.hyper` file name, `.tdsx` file name
- Tableau Online/Server address, site, project, and auth fields
- Header file and wildcard name format
- S3 bucket name and AWS credential profile name


## __Creating the .tdsx File__
As mentioned, one key step needed for the automatic publishing of multi-table hyper files is a Packaged Data Source, or .tdsx. As of now, this is a step that must be completed manually as a part of the setup process. _You will only need to do this once_. If a .tdsx is not present in the directory, the script will prompt you to create one.

Packaged Data Sources contain important metadata needed for Tableau Desktop and Server/Online. This includes things like definted joins and join clauses, relationships, calculated fields, and more.

To create the .tdsx, [follow these steps](https://help.tableau.com/current/pro/desktop/en-us/export_connection.htm):
- Run the script without a data source present to create the initial hyper file
- Double-click the hyper file to open it in Tableau Desktop
- Click and drag the relevant tables and create the joins or relationships
- Head to 'Sheet 1'
- In the top-left corner, right-click on the data source and select 'Add to Saved Data Sources...'
- Name the file to match the value in `config.json`
- Select 'Tableau __Packaged__ Data Source (*.tdsx)' from the dropdown
- Save it in the directory with the script and hyper file

Now you are free to rerun the script and validate the swapping and publishing process. Unless you change how the hyper file is being created (schema, column names, joins, etc.), you will not need to remake the .tdsx again.


Leverage the [official Hyper API samples](https://github.com/tableau/hyper-api-samples/tree/master/Python) to learn more about what's possible.


## __Resources__
Check out these resources to learn more:
- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)
- [TSC docs](https://tableau.github.io/server-client-python/docs/)
- [REST API docs](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
- [Tableau Tools](https://github.com/bryantbhowell/tableau_tools)
- [Publishing Data Sources](https://help.tableau.com/current/pro/desktop/en-us/export_connection.htm)