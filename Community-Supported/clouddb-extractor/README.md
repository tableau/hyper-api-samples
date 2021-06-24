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
* __config.yml__ - Defines site defaults for extractor_cli utility
* __extractor_cli.py__ - Simple CLI Wrapper around Extractor Classes
* __requirements.txt__ - List of third party python library dependencies
* __restapi_helpers.py__ - Helper functions for REST operations that are not yet available in the standard tableauserverclient libraries (e.g. PATCH for update/upsert).  Once these get added to the standard client libraries then this module will be refactored out.

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


# Installation

## Prerequisites
To use these utilities you will need:

- a Linux server (tested on ubuntu-1804-bionic-v20210504)
- Python 3.6 or above (tested on Python 3.6.9)

## Get the latest version of the Hyper API Samples
```console
git clone https://github.com/tableau/hyper-api-samples.git
```

## Create a new virtual environment
```console
python3 -m venv env
source env/bin/activate
```

## Install Hyper API Client Libraries
For latest instructions refer to: [Hyper API for Python](https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_installing.html#install-the-hyper-api-for-python-36-and-37)

```console
pip install --upgrade pip
pip install tableauhyperapi
```

## Install Tableau Server Client Libraries
For latest instructions refer to: [Tableau Server Client Libraries](https://tableau.github.io/server-client-python/docs/)

```console
pip install tableauserverclient
```

## Install and configure Google Cloud SDK and BigQuery Client Libraries
### Google Cloud SDK Configuration Notes
Install Google Cloud SDK using these instructions: https://cloud.google.com/sdk/docs/install#deb

At the end of the installation process above you will need to run `gcloud init` to configure accont credentials and cloud environment defaults.  Ensure that credentials and default compute zone/regions are defined correctly by reviewing the output or using `gcloud auth list` and `gcloud config configurations list`.

Sample output:
```console
(env) myuser@emea-client-linux:~$ gcloud init
Welcome! This command will take you through the configuration of gcloud.
....
Your Google Cloud SDK is configured and ready to use!
* Commands that require authentication will use XXX061130084-compute@developer.gserviceaccount.com by default
* Commands will reference project `pre-sales-demo` by default
* Compute Engine commands will use region `europe-west4` by default
* Compute Engine commands will use zone `europe-west4-a` by default
Run `gcloud help config` to learn how to change individual settings
This gcloud configuration is called [default]. You can create additional configurations if you work with multiple accounts and/or projects.
Run `gcloud topic configurations` to learn more.
Some things to try next:
* Run `gcloud --help` to see the Cloud Platform services you can interact with. And run `gcloud help COMMAND` to get help on any gcloud command.
* Run `gcloud topic --help` to learn about advanced features of the SDK like arg files and output formatting
(env) myuser@emea-client-linux:~$ gcloud auth list
                  Credentialed Accounts
ACTIVE  ACCOUNT
*       XXX061130084-compute@developer.gserviceaccount.com
To set the active account, run:
    $ gcloud config set account `ACCOUNT`
(env) myuser@emea-client-linux:~$ gcloud config configurations list
NAME     IS_ACTIVE  ACCOUNT                                             PROJECT         COMPUTE_DEFAULT_ZONE  COMPUTE_DEFAULT_REGION
default  True       XXX061130084-compute@developer.gserviceaccount.com  pre-sales-demo  europe-west4-a        europe-west4
```
### BigQuery Client Libraries
Install BigQuery Python Client Libraries using these instructions: https://github.com/googleapis/python-bigquery

```console
pip install google-cloud-bigquery
```

### Cloud API Access
In testing we used service account credentials in a GCP Compute Engine VM to invoke all required cloud service APIs.  In order for these utilities to work you will need to enable the following API Access Scopes for your VM:
- BigQuery - Enabled
- Storage - Read Write

For more details refer to: https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances#changeserviceaccountandscopes

Alternatively, a best practice is to set the cloud-platform access scope on the instance, then securely limit the service account's API access with IAM roles.

## Install third party python library dependencies
From the directory where you extracted the hyper api samples execute the following:
```console
cd hyper-api-samples/Community-Supported/clouddb-extractor
pip install -r requirements.txt
```
## Configure cloud and tableau environment defaults
Edit `config.yml` to define your environment defaults
