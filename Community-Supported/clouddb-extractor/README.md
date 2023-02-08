# clouddb-extractor
## __clouddb-extractor__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

Cloud Database Extractor Utility - This sample shows how to extract data from a cloud database to a published hyper extract and append/update/delete rows to keep up to date.

A detailed article about this utility is availabe at: https://www.tableau.com/developer/learning/how-synchronize-your-cloud-data-tableau-extracts-scale

# Overview
This package defines a standard Extractor Interface which is extended by specific implementations
to support specific cloud databases.  For most use cases you will probably only ever call the
following methods:
* __load_sample__ - Used during testing - extract a sample of rows from the source table to a new published datasource
* __export_load__ - Used for initial load - full extract of source table to a new published datasource
* __append_to_datasource__ - Append rows from a query or table to an existing published datasource
* __update_datasource__ - Updates an existing published datasource with the changeset from a query or table
* __delete_from_datasource__ - Delete rows from a published datasource that match a condition and/or that match the primary keys in the changeset from a query or table

For a full list of methods and args see the docstrings in the BaseExtractor class.

## Contents
* __azuresql_extractor.py__ - Azure SQL Database implementation of Base Hyper Extractor ABC
* __base_extractor.py__ - provides an Abstract Base Class with some utility methods to extract from cloud databases to "live to hyper" Tableau Datasources. Database specific Extractor classes extend this to manage connections and schema discovery
and may override the generic query processing methods based on DBAPIv2 standards with database specific optimizations.
* __bigquery_extractor.py__ - Google BigQuery implementation of Base Hyper Extractor ABC
* __extractor_cli.py__ - Simple CLI Wrapper around Extractor Classes
* __mysql_extractor.py__ - MySQL implementation of Base Hyper Extractor ABC
* __postgres_extractor.py__ - PostgreSQL implementation of Base Hyper Extractor ABC
* __README.md__ - This file
* __redshift_extractor.py__ - AWS Redshift implementation of Base Hyper Extractor ABC
* __requirements.txt__ - List of third party python library dependencies
* __tableau_restapi_helpers.py__ - Helper functions for REST operations that are not yet available in the standard tableauserverclient libraries (e.g. PATCH for update/upsert).  Once these get added to the standard client libraries then this module will be refactored out.
* __SAMPLE-config.yml__ - Template configuration file.  Copy this to config.yml and customize with your site defaults.
* __SAMPLE-delete_and_append_script.py__ - Sample script file shows how to execute multiple commands in a single batch operation (queries records for the most recent N days from source database to changeset hyper file, deletes the most recent N days from target datasource, appends changeset to target datasource)
* __SAMPLE-upsert_script.py__ - Sample script performs the same logic as above but applies changes using the upsert action instead of delete and insert.

## CLI Utility
We suggest that you import one of the Extractor implementations and call this directly however we've included a command line utility to illustrate the key functionality:

```console
$ python3 extractor_cli.py --help

  usage: extractor_cli.py [-h]
   {load_sample,export_load,append,update,upsert,delete}
   [--extractor {bigquery}]
   [--source_table_id SOURCE_TABLE_ID]
   [--overwrite]
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
  - upsert: Upsert changeset to an existing published datasource (update if rows match the specified primary keys else insert)
  - delete: Delete rows from a Tableau datasource that match key columns in a changeset from a query
```

### Sample Usage
Before use you should modify the file config.yml with your tableau and database settings.

__Load Sample:__ Load a sample (default=1000 lines) from test_table to sample_extract in test_project:
```console
python3 extractor_cli.py load_sample --source_table_id test_table --tableau_project test_project --tableau_datasource sample_extract
```

__Full Export:__ Load a full extract from test_table to full_extract in test_project:
```console
python extractor_cli.py export_load --source_table_id "test_table" --tableau_project "test_project" --tableau_datasource "test_datasource"
 ```

__Append:__ Execute new_rows.sql to retrieve a changeset and append to test_datasource:
```console
# new_rows.sql:
SELECT * FROM staging_table

python extractor_cli.py update --sqlfile new_rows.sql --tableau_project "test_project" --tableau_datasource "test_datasource"
```

__Update:__ Execute updated_rows.sql to retrieve a changeset and update test_datasource where primary key columns in changeset (METRIC_ID and METRIC_DATE) match corresponding columns in target datasource:
```console
# updated_rows.sql:
SELECT * FROM source_table WHERE LOAD_TIMESTAMP<UPDATE_TIMESTAMP

python extractor_cli.py update --sqlfile updated_rows.sql --tableau_project "test_project" --tableau_datasource "test_datasource" \
  --match_columns METRIC_ID METRIC_ID --match_columns METRIC_DATE METRIC_DATE
```

__Upsert:__ Execute upsert_rows.sql to retrieve a changeset and upsert to test_datasource (i.e. update where primary key columns in changeset (METRIC_ID and METRIC_DATE) match corresponding columns in target datasource, else insert):
```console
# upsert_rows.sql:
SELECT * FROM source_table WHERE LOAD_TIMESTAMP<UPDATE_TIMESTAMP OR LOAD_TIMESTAMP>'2022-08-01'

python extractor_cli.py upsert --sqlfile updated_rows.sql --tableau_project "test_project" --tableau_datasource "test_datasource" \
  --match_columns METRIC_ID METRIC_ID --match_columns METRIC_DATE METRIC_DATE
```

__Delete:__ Execute deleted_rows.sql to retrieve a changeset containing the primary key columns that identify which rows have been deleted.  a list of deleted rows.  Delete  full_extract where METRIC_ID and METRIC_DATE in changeset match corresponding columns in target datasource:
```console
# deleted_rows.sql:
SELECT METRIC_ID, METRIC_DATE FROM source_table_deleted_rows

python extractor_cli.py delete --sqlfile deleted_rows.sql --tableau_project "test_project" --tableau_datasource "full_extract" \
  --match_columns METRIC_ID METRIC_ID --match_columns METRIC_DATE METRIC_DATE
```

__Conditional Delete:__ In this example no changeset is provided - records to be deleted are identified using the conditions specified in delete_conditions.json
```console
# delete_conditions.json
{
        "op": "lt",
        "target-col": "ORDER_DATE",
        "const": {"type": "datetime", "v": "2018-02-01T00:00:00Z"}
}

python extractor_cli.py delete --tableau_project "test_project" --tableau_datasource "full_extract" \
  --match_conditions_json=delete_conditions.json
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

## Install third party python library dependencies
From the directory where you extracted the hyper api samples execute the following:
```console
cd hyper-api-samples/Community-Supported/clouddb-extractor
pip install -r requirements.txt
```

## Azure SQL Database Configuration
The following steps are required if using azuresql_extractor.
 - Install ODBC Drivers and Azure utilities for your platform using the following instructions: https://github.com/Azure-Samples/AzureSqlGettingStartedSamples/tree/master/python/Unix-based
 
## Google BigQuery Configuration
The following steps are required if using bigquery_extractor.

### Google Cloud SDK
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
### (Optional) Install additional Google libraries if USE_DBAPI=True
By default we will use bigquery.table.RowIterator to handle queries however
bigquery_extractor can be configured to use the DBAPIv2 libraries and you may find that this gives performance advantages for some datasets as it can take advantage of the Storage Read API.

To use this option you will need to install the following additional libraries:

```console
pip install google-cloud-bigquery-storage
pip install pyarrow
```

### Cloud API Access
In testing we used service account credentials in a GCP Compute Engine VM to invoke all required cloud service APIs.  In order for these utilities to work you will need to enable the following API Access Scopes for your VM:
- BigQuery - Enabled
- Storage - Read Write

For more details refer to: https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances#changeserviceaccountandscopes

Alternatively, a best practice is to set the cloud-platform access scope on the instance, then securely limit the service account's API access with IAM roles.

## AWS Redshift Configuration
The following steps are required if using redshift_extractor

### Install and configure AWS Cloud SDK
Install Boto3 using these instructions: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

```console
pip install boto3
```

Install the AWS CLI using these instructions: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

Configure access credentials and defaults

```console
aws configure
```
### Install Redshift Client libraries
Install redshift connector using these instructions:
https://github.com/aws/amazon-redshift-python-driver

```console
pip install redshift_connector
```

### Authentication and Database configuration
All connection parameters are defined in the redshift.connection section in `config.yml`, for example:

```console
redshift: #Redshift configuration defualts
  connection:
    host : 'redshift-cluster-1.xxxxxxxxx.eu-west-1.redshift.amazonaws.com'
    database : 'dev'
    user : 'db_username'
    password : 'db_password'
```

If you are using IAM for authentication instead of username/password then you should follow the instructions here:
- [Options for providing IAM credentials](https://docs.aws.amazon.com/redshift/latest/mgmt/options-for-providing-iam-credentials.html)
- [Redshift Connector Python Tutorial](https://github.com/aws/amazon-redshift-python-driver/blob/master/tutorials/001%20-%20Connecting%20to%20Amazon%20Redshift.ipynb)

After you have configured your IAM roles etc. in AWS Management Console you will need to specify additional parameters in the redshift.connection section in `config.yml`, i.e.:

```console
# Connects to Redshift cluster using IAM credentials from default profile defined in ~/.aws/credentials
redshift: #Redshift configuration defualts
  connection:
    iam : True,
    database : 'dev',
    db_user : 'awsuser',
    password : '',
    user : '',
    cluster_identifier : 'examplecluster',
    profile : 'default'
```

Other options for federated API access using external identity providers are discussed in the following blog: - https://aws.amazon.com/blogs/big-data/federated-api-access-to-amazon-redshift-using-an-amazon-redshift-connector-for-python/

## MySQL Configuration

### Install MySQL Client Libraries:
Install MySQL connector using these instructions: https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html

```console
pip install mysql-connector-python
```

### Authentication and Database configuration
All connection parameters are defined in the mysql.connection section in `config.yml`, for example:

```console
mysql: #Mysql configuration defaults
  connection:
    host : "mysql.test"
    database : "dev"
    port : 3306
    username : "test"
    password : "password"
    raise_on_warnings : True
```
Database connection configuration options are documented here: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html

## PostgreSQL Configuration

### Install PostgreSQL Client Libraries:
Install Psycopg using hese instructions: https://www.psycopg.org/docs/install.html

```console
pip install psycopg2-binary
```

### Authentication and Database configuration
All connection parameters are defined in the postgres.connection section in `config.yml`, for example:

```console
postgres: #PostgreSQL configuration defaults
  connection:
    dbname : "dev"
    username : "test"
    password : "password"
    host : "postgres.test"
    port : 5432
```
Database connection configuration options are documented here:
- https://www.psycopg.org/docs/module.html?highlight=connect#psycopg2.connect
- https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

# Configuration
All configuration defaults are loaded from the config.yml file.
