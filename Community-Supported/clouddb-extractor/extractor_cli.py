""" Hyper Extractor CLI utility

Tableau Community supported Hyper API sample

Command Line Wrapper around Extractor Class Implentations
The Extractor utilities provide convenience functions to load, append, update and delete
Tableau datasources from a Cloud Database table or query result

-----------------------------------------------------------------------------

This file is the copyrighted property of Tableau Software and is protected
by registered patents and other applicable U.S. and international laws and
regulations.

You may adapt this file and modify it to fit into your context and use it
as a template to start your own projects.

-----------------------------------------------------------------------------
"""
import logging
import argparse
import getpass
import json

import bigquery_extractor

EXTRACTORS = {"bigquery": bigquery_extractor.BigQueryExtractor}
DEFAULT_EXTRACTOR = "bigquery"
MAX_QUERY_SIZE = 100 * 1024 * 1024  # 100MB
SAMPLE_ROWS = 1000
TABLEAU_PROJECT = "HyperAPITests"
TABLEAU_DATASOURCE_NAME = "Orders"
TABLEAU_HOSTNAME = "http://localhost"
DEFAULT_SITE_ID = ""
BUCKET_NAME = "emea_se"


def exclusive_args(args, *arg_names, required=True, message=None):
    count_args = 0
    for arg_name in arg_names:
        if bool(vars(args).get(arg_name)):
            count_args += 1
    if required:
        if count_args != 1:
            if message is None:
                raise argparse.ArgumentError(
                    "Must specify one of {}".format(",".join(arg_names))
                )
            else:
                raise argparse.ArgumentError(message)
    else:
        if count_args > 1:
            if message is None:
                raise argparse.ArgumentError(
                    "Can only specify one of {}".format(",".join(arg_names))
                )
            else:
                raise argparse.ArgumentError(message)


def required_arg(args, arg_name, message=None):
    if not bool(vars(args).get(arg_name)):
        if message is None:
            raise argparse.ArgumentError(
                "Missing required argument:{}".format(arg_name)
            )
        else:
            raise argparse.ArgumentError(message)


#
# Initialize logging
#
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s %(funcName)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), consoleHandler],
)

#
# Parse Command Line Args
#
parser = argparse.ArgumentParser(
    description="""Utilities to build Hyper Extracts from Cloud Databases
    - load_sample: Load sample rows of data to new Tableau datasource
    - export_load: Bulk export and load to new Tableau datasource
    - append: Append the results of a query to an existing Tableau datasource
    - update: Update an existing Tableau datasource with the changeset from a query
    - delete: Delete rows from a Tableau datasource that match key columns in a changeset from a query""",
)
parser.add_argument(
    "command",
    choices=["load_sample", "export_load", "append", "update", "delete"],
    help="Select the utility function to call",
)
parser.add_argument(
    "--extractor",
    choices=EXTRACTORS.keys(),
    default=DEFAULT_EXTRACTOR,
    help="Select the extractor implementation that matches your cloud database",
)
parser.add_argument(
    "--source_table_id", help="Source table ID",
)
parser.add_argument(
    "--tableau_project",
    "-P",
    default=TABLEAU_PROJECT,
    help="Target project name (default={})".format(TABLEAU_PROJECT),
)
parser.add_argument(
    "--tableau_datasource", required=True, help="Target datasource name",
)
parser.add_argument(
    "--tableau_hostname",
    "-H",
    default=TABLEAU_HOSTNAME,
    help="Tableau connection string (default={})".format(TABLEAU_HOSTNAME),
)
parser.add_argument(
    "--tableau_site_id",
    "-S",
    default=DEFAULT_SITE_ID,
    help="Tableau site id (default={})".format(DEFAULT_SITE_ID),
)
parser.add_argument(
    "--bucket",
    default=BUCKET_NAME,
    help="Bucket used for extract staging storage (default={})".format(BUCKET_NAME),
)
parser.add_argument(
    "--sample_rows",
    default=SAMPLE_ROWS,
    help="Defines the number of rows to use with LIMIT when command=load_sample (default={})".format(
        SAMPLE_ROWS
    ),
)
parser.add_argument(
    "--sql",
    help="The query string used to generate the changeset when command=[append|update|merge]",
)
parser.add_argument(
    "--sqlfile",
    help="File containing the query string used to generate the changeset when command=[append|update|delete]",
)
parser.add_argument(
    "--match_columns",
    action="append",
    nargs=2,
    help="Define conditions for matching source and target key columns "
    "to use when command=[update|delete].  Specify one or more column pairs "
    "in the format: --match_columns [source_col] [target_col]",
)
parser.add_argument(
    "--match_conditions_json",
    help="Define conditions for matching rows in json format when command=[update|delete]."
    "See Hyper API guide for details. ",
)
# TODO: Add option to authenticate with api token
parser.add_argument(
    "--tableau_username", "-U", help="Tableau user name",
)
parser.add_argument(
    "--tableau_token_name", help="Personal access token name",
)
parser.add_argument(
    "--tableau_token_secretfile", help="File containing personal access token secret",
)

args = parser.parse_args()
selected_command = args.command

#
# Initialize Extractor Implementation
#
TABLEAU_HOSTNAME = args.tableau_hostname
TABLEAU_PROJECT = args.tableau_project
TABLEAU_SITE_ID = args.tableau_site_id
extractor_class = EXTRACTORS.get(args.extractor)
extractor = {}
exclusive_args(
    args,
    "tableau_token_name",
    "tableau_username",
    required=True,
    message="Specify either tableau_token_name OR tableau_username",
)
if args.tableau_token_name:
    required_arg(
        args,
        "tableau_token_secretfile",
        "Must specify tableau_token_secretfile with tableau_token_name",
    )
    tableau_token_secret = ""
    with open(args.tableau_token_secretfile, "r") as myfile:
        tableau_token_secret = myfile.read().strip()

    extractor = extractor_class(
        tableau_hostname=TABLEAU_HOSTNAME,
        tableau_project=TABLEAU_PROJECT,
        tableau_site_id=TABLEAU_SITE_ID,
        staging_bucket=BUCKET_NAME,
        tableau_token_name=args.tableau_token_name,
        tableau_token_secret=tableau_token_secret,
    )
else:
    TABLEAU_USERNAME = args.tableau_username
    TABLEAU_PASSWORD = getpass.getpass("Password: ")
    extractor = extractor_class(
        tableau_hostname=TABLEAU_HOSTNAME,
        tableau_project=TABLEAU_PROJECT,
        tableau_site_id=TABLEAU_SITE_ID,
        staging_bucket=BUCKET_NAME,
        tableau_username=TABLEAU_USERNAME,
        tableau_password=TABLEAU_PASSWORD,
    )

#
# Implement TABLE level commands here
#
if selected_command in ("load_sample", "export_load"):
    required_arg(
        args,
        "source_table_id",
        "Must specify source_table_id when command is load_sample or export_load",
    )
    if selected_command == "load_sample":
        extractor.load_sample(
            source_table=args.source_table_id,
            tab_ds_name=args.tableau_datasource,
            sample_rows=args.sample_rows,
        )
    if selected_command == "export_load":
        extractor.export_load(
            source_table=args.source_table_id, tab_ds_name=args.tableau_datasource,
        )

#
# Implement QUERY processing commands here
#
if selected_command in ("append", "update", "delete"):
    exclusive_args(
        args,
        "sql",
        "sqlfile",
        "source_table_id",
        required=(selected_command != "delete"),
        message="Specify either sql OR sqlfile OR source_table_id",
    )
    sql_string = args.sql
    if args.sqlfile:
        with open(args.sqlfile, "r") as myfile:
            sql_string = myfile.read()

    if selected_command == "append":
        extractor.append_to_datasource(
            sql_query=sql_string,
            source_table=args.source_table_id,
            tab_ds_name=args.tableau_datasource,
        )
    else:
        #
        #  Implement update, delete, merge commands here
        #
        exclusive_args(
            args,
            "match_columns",
            "match_conditions_json",
            required=True,
            message="Must specify either match_columns OR match_conditions_json when command is update,delete,merge",
        )
        match_conditions_json = None
        if args.match_conditions_json:
            match_conditions_json = json.loads(args.match_conditions_json)
        if selected_command == "update":
            extractor.update_datasource(
                sql_query=sql_string,
                source_table=args.source_table_id,
                tab_ds_name=args.tableau_datasource,
                match_columns=args.match_columns,
                match_conditions_json=match_conditions_json,
            )
        if selected_command == "delete":
            extractor.delete_from_datasource(
                sql_query=sql_string,
                source_table=args.source_table_id,
                tab_ds_name=args.tableau_datasource,
                match_columns=args.match_columns,
                match_conditions_json=match_conditions_json,
            )
