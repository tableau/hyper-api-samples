# clouddb-extractor
## __clouddb-extractor__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

Cloud Database Extractor Utility - This sample shows how to extract data from a cloud database to a published hyper extract and append/update/delete rows to keep up to date.

# Overview
This package defines a standard Extractor Interface which is extended by specific implementations
to support specific cloud databases.  For most use cases you will probably only ever call the
following methods:
* __load_sample__ - Loads a sample of rows from source_table to Tableau Server
* __export_load__ - Bulk export the contents of source_table and load to a Tableau Server
* __append_to_datasource__ - Appends the result of sql_query to a datasource on Tableau Server
* __update_datasource__ - Updates a datasource on Tableau Server with the changeset from sql_query
* __delete_from_datasource__ - Delete rows matching the changeset from a datasource on Tableau Server.  Simple delete by condition when sql_query is None

For a full list of methods and args see the docstrings in the BaseExtractor class.

## Contents
* __base_extractor.py__ - provides an Abstract Base Class with some utility methods to extract from cloud databases to "live to hyper" Tableau Datasources. Database specific Extractor classes extend this to manage queries, exports and schema discovery via the database vendor supplied client libraries.
* __bigquery_extractor.py__ - Google BigQuery implementation of Base Hyper Extractor ABC
* __restapi_helpers.py__ - The helper functions in this module are only used when REST API functionality is not yet available in the standard tableauserverclient libraries. (e.g. PATCH for update/upsert. Once these get added to the standard client libraries then this module will be refactored out.
* __extractor_cli.py__ - Simple CLI Wrapper around Extractor Classes
* __requirements.txt__ - List of third party python library dependencies - install with "pip install -r requirements.txt"

## CLI Utility
We suggest that you import one of the Extractor implementations and call this directly however we've included a command line utility to illustrate the key functionality:

```console
$ python3 extractor_cli.py --help
  usage: extractor_cli.py [-h]
   {load_sample,export_load,append,update,delete}
   [--extractor {bigquery}]
   [--source_table_id SOURCE_TABLE_ID]
   [--tableau_project TABLEAU_PROJECT]
   --tableau_datasource TABLEAU_DATASOURCE
   [--tableau_hostname TABLEAU_HOSTNAME]
   [--tableau_site_id TABLEAU_SITE_ID] [--bucket BUCKET]
   [--sample_rows SAMPLE_ROWS] [--sql SQL]
   [--sqlfile SQLFILE]
   [--match_columns MATCH_COLUMNS MATCH_COLUMNS]
   [--match_conditions_json MATCH_CONDITIONS_JSON]
   [--tableau_username TABLEAU_USERNAME]
   [--tableau_token_name TABLEAU_TOKEN_NAME]
   [--tableau_token_secretfile TABLEAU_TOKEN_SECRETFILE]

 Utilities to build Hyper Extracts from Cloud Databases
  - load_sample: Load sample rows of data to new Tableau datasource
  - export_load: Bulk export and load to new Tableau datasource
  - append: Append the results of a query to an existing Tableau datasource
  - update: Update an existing Tableau datasource with the changeset from a query
  - delete: Delete rows from a Tableau datasource that match key columns in a changeset from a query
```


# Get started

## Prerequisites

To run the script, you will need:

- a Linux server
- Python 3.6 or 3.7
- [Hyper API for Python](https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_installing.html#install-the-hyper-api-for-python-36-and-37)
- [Tableau Server Client Libraries](https://tableau.github.io/server-client-python/docs/)
- BigQuery Extractor: The `Google Cloud SDK` and `Python libraries for Cloud Storage and BigQuery`
