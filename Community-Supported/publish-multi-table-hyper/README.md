# publish-multi-table-hyper
## __Publishing a Multi-Table Hyper File to Tableau Online/Server__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This sample demonstrates how to leverage the Hyper API, Tableau Server Client Library, and Tableau Tools to do the following:
- Creates a multi-table .hyper file
- Swaps the newly created extract into an existing Packaged Data Source file (.tdsx)
- Publishes the data source to a specified project on Tableau Online/Server

It should serve as a starting point for anyone looking to automate the publishing process of multi-table extracts and data sources to Tableau. The Tableau Server Client library (TSC) is able to publish single-table hyper files directly to Online/Server, but this is not currently the case for any extract with multiple tables. Because of this, there is an important additional step of swapping the newly built hyper file into an existing packaged data source.


# Get started

## __Prerequisites__
To run the script, you will need:
- Windows or Mac
- Tableau Desktop v10.5 or higher
- Python 3.6 - 3.7
- Run `pip install -r requirements.txt`
- Tableau Online/Server credentials or Personal Access Token

## __Configuration File__
Modify `config.json` and add the following fields:
- Name of the .hyper file
- Name of the .tdsx file
- Server/Online url
- Site name
- Project name
- Auth fields

## __Data and Table Definitions__
If you want to simply run the sample to test the publishing process, you won't need to make any changes to the hyper_to_tdsx.py file. Ensure that you have installed the requirements and execute the python file.

Once you're ready to use your own data, you'll need to change the `get_data()` function to return the two lists of data to the `add_to_hyper()` function. It could be a part of an existing ETL workflow, grab the data from an API request, or pull CSVs from cloud storage like AWS, Azure, or GCP. In any case, writing that code is up to you. You can [check out this doc](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/tableauhyperapi.html?tableauhyperapi.Inserter) for more on how to pass data to Hyper's `inserter()` method.

The next step is to modify the `build_tables()` function to return the proper TableDefinition's to hyper. Note that the ordering of the columns must match how the data is being passed in. [Check out this doc](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/tableauhyperapi.html?tableauhyperapi.SqlType) for more information on the the Hyper API's SqlType class.

__Note:__ The current example features two tables, but in theory, this could support as many as you'd like. Just be sure to add the proper table definitions and make sure that the order in the list of table data and table definitions properly match.

## __Creating the .tdsx File__
As mentioned, one key step needed for the automatic publishing of multi-table hyper files is a Packaged Data Source, or .tdsx. As of now, this is a step that must be completed manually as a part of the setup process. _You will only need to do this once_. If a .tdsx is not present in the directory, the script will prompt you to create one. At this point, you should have entered the required config fields and have run the python script once to create the multi-table .hyper file.

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

## __Additional Customization__
If you end up needing to change more about how the extract is built (ex. insterting directly from a CSV file) then you will need to also change the `add_to_hyper()` function, but most likely nothing else.

Leverage the [official Hyper API samples](https://github.com/tableau/hyper-api-samples/tree/master/Python) to learn more about what's possible.


## __Resources__
Check out these resources to learn more:
- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)
- [TSC Docs](https://tableau.github.io/server-client-python/docs/)
- [REST API docs](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
- [Tableau Tools](https://github.com/bryantbhowell/tableau_tools)