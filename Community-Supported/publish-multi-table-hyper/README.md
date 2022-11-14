# publish-multi-table-hyper
## __Publishing a Multi-Table Hyper File to Tableau Online/Server (for Tableau Server versions 2021.4+)__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

In contrast to single-table `.hyper` files, with multi-table `.hyper` files it is not obvious which data model you want to analyze in the data source on Tableau Server. Thus, when publishing to Tableau Server < 2021.4, you can only publish single-table `.hyper` files or need to wrap the `.hyper` file into a Packaged Data Source (.tdsx). Starting with version 2021.4, you can now publish multi-table `.hyper` files to Tableau Server and Tableau Online; the data model will be automatically inferred (as specified by assumed table constraints). 

This sample demonstrates how to create a multi-table `.hyper` file with constraints such that Tableau Server (2021.4+) can infer the data model. If you are looking for how to wrap a multi-table `.hyper` file into a Packaged Data Source (.tdsx), have a look at [this sample](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/publish-multi-table-hyper-legacy).

It should serve as a starting point for anyone looking to automate the publishing process of multi-table extracts and data sources to Tableau. 

# Get started

## __Prerequisites__
To run the script, you will need:
- Windows, Linux, or Mac
- Python 3
- Run `pip install -r requirements.txt`
- Tableau Online/Server credentials or Personal Access Token

## __Configuration File__
Modify `config.json` and add the following fields:
- Name of the `.hyper` file
- Server/Online url
- Site name
- Project name
- Authentication information

## __Data and Table Definitions__
If you want to simply run the sample to test the publishing process, you do not need to make any changes to the python file. Ensure that you have installed the requirements, update the config file with authentication information and execute the python file.

Once you are ready to use your own data, you will need to change the `create_hyper_file_and_insert_data()` function. This function could be a part of an existing ETL workflow, grab the data from an API request, or pull CSVs from cloud storage like AWS, Azure, or GCP. In any case, writing that code is up to you. You can [check out this doc](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/tableauhyperapi.html?tableauhyperapi.Inserter) for more information on how to pass data to Hyper's `inserter()` method and [this doc](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/tableauhyperapi.html?tableauhyperapi.SqlType) for more information on the the Hyper API's SqlType class.

__Note:__ The current example features two tables, but in theory, this could support as many as you'd like. Just be sure to add the proper table definitions and make sure that the order in the list of table data and table definitions properly match.

## __How Does Tableau Server infer the data model for the data source?__
On Tableau Server, the data model for the data source is generated from the foreign keys in the `.hyper` file. In particular, a relationship between two tables is generated whenever they are connected with a foreign key (the resulting relationship may be a multi-expression if multiple columns are involved). No validation on foreign keys will be performed, for example, referential integrity is not enforced. Only simple relationship trees which span all tables in the database are supported; publishing will fail if this is violated, e.g., if there are multiple tables which do not have incoming foreign keys (multiple fact tables).

The resulting data source will have a single connection to the Hyper file, with a set of objects which correspond to individual tables in the Hyper file. For compatibility and to avoid surprises this will be done only for multi-table Hyper files, i.e. no changes for publishing single-table Hyper files.
## __Additional Customization__
If you end up needing to change more about how the extract is built (e.g., inserting directly from a CSV file) then you will need to also change the `create_hyper_file_and_insert_data()` function, but most likely nothing else.

Leverage the [official Hyper API samples](https://github.com/tableau/hyper-api-samples/tree/master/Python) to learn more about what's possible.


## __Resources__
Check out these resources to learn more:
- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)
- [TSC Docs](https://tableau.github.io/server-client-python/docs/)
- [REST API docs](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
- [Tableau Tools](https://github.com/bryantbhowell/tableau_tools)
- [Another multi-table example](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/git-to-hyper)