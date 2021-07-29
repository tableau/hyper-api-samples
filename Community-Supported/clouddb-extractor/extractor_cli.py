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
import importlib
import logging
import argparse
import getpass
import json
import yaml

# Globals
EXTRACTORS = {
    "bigquery": "bigquery_extractor.BigQueryExtractor",
    "redshift": "redshift_extractor.RedshiftExtractor",
    "mysql": "mysql_extractor.MySQLExtractor",
    "postgres": "postgres_extractor.PostgresExtractor",
}
CONFIGURATION_FILE = "config.yml"

#
# Initialize logging
#
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s %(funcName)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), consoleHandler],
)


class IllegalArgumentError(ValueError):
    pass


def exclusive_args(args, *arg_names, required=True, message=None):
    count_args = 0
    for arg_name in arg_names:
        if bool(vars(args).get(arg_name)):
            count_args += 1
    if required:
        if count_args != 1:
            if message is None:
                raise IllegalArgumentError(message="Must specify one of {}".format(",".join(arg_names)))
            else:
                raise IllegalArgumentError(message)
    else:
        if count_args > 1:
            if message is None:
                raise IllegalArgumentError("Can only specify one of {}".format(",".join(arg_names)))
            else:
                raise IllegalArgumentError(message)


def required_arg(args, arg_name, message=None):
    if not bool(vars(args).get(arg_name)):
        if message is None:
            raise IllegalArgumentError("Missing required argument:{}".format(arg_name))
        else:
            raise IllegalArgumentError(message)


def main():
    # Load defaults
    config = yaml.safe_load(open("config.yml"))
    default_extractor = config.get("default_extractor")
    tableau_env = config.get("tableau_env")
    db_env = config.get(default_extractor)

    # Define Command Line Args
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
        default=default_extractor,
        help=f"Select the extractor implementation that matches your cloud database (default={default_extractor})",
    )
    parser.add_argument(
        "--source_table_id",
        help="Fully qualified table identifier from source database",
    )
    parser.add_argument(
        "--tableau_project",
        "-P",
        default=tableau_env.get("project"),
        help="Target project name (default={})".format(tableau_env.get("project")),
    )
    parser.add_argument(
        "--tableau_datasource",
        required=True,
        help="Target datasource name",
    )
    parser.add_argument(
        "--tableau_hostname",
        "-H",
        default=tableau_env.get("server_address"),
        help="Tableau connection string (default={})".format(tableau_env.get("server_address")),
    )
    parser.add_argument(
        "--tableau_site_id",
        "-S",
        default=tableau_env.get("site_id"),
        help="Tableau site id (default={})".format(tableau_env.get("site_id")),
    )
    parser.add_argument(
        "--sample_rows",
        default=config.get("sample_rows"),
        help="Defines the number of rows to use with LIMIT when command=load_sample(default={})".format(config.get("sample_rows")),
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
        help="Define conditions for matching rows in json format when command=[update|delete]." "See Hyper API guide for details. ",
    )
    parser.add_argument(
        "--tableau_username",
        "-U",
        help="Tableau user name",
    )
    parser.add_argument(
        "--tableau_token_name",
        help="Personal access token name",
    )
    parser.add_argument(
        "--tableau_token_secretfile",
        help="File containing personal access token secret",
    )

    # Parse Args
    args = parser.parse_args()
    selected_command = args.command
    selected_extractor = args.extractor
    db_env = config.get(selected_extractor)

    # Check for conflicting args
    exclusive_args(
        args,
        "tableau_token_name",
        "tableau_username",
        required=True,
        message="Specify either tableau_token_name OR tableau_username",
    )
    exclusive_args(
        args,
        "sql",
        "sqlfile",
        "source_table_id",
        required=(selected_command != "delete"),
        message="Specify either sql OR sqlfile OR source_table_id",
    )

    # Load sqlfile to sql_string if used
    sql_string = args.sql
    if args.sqlfile:
        with open(args.sqlfile, "r") as myfile:
            sql_string = myfile.read()

    # Initialize Extractor Implementation
    # These are loaded on demand so that you don't have to install
    # client libraries for all source database implementations
    extractor_class_str = EXTRACTORS.get(selected_extractor)
    extractor_module_str = extractor_class_str.split(".")[0]
    extractor_class_str = extractor_class_str.split(".")[1]
    extractor_module = importlib.import_module(extractor_module_str)
    extractor_class = getattr(extractor_module, extractor_class_str)

    # Tableau Authentication can be by token or username/password (prompt)
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
            source_database_config=db_env,
            tableau_hostname=args.tableau_hostname,
            tableau_project=args.tableau_project,
            tableau_site_id=args.tableau_site_id,
            tableau_token_name=args.tableau_token_name,
            tableau_token_secret=tableau_token_secret,
        )
    else:
        tableau_password = getpass.getpass("Tableau Password: ")
        extractor = extractor_class(
            source_database_config=db_env,
            tableau_hostname=args.tableau_hostname,
            tableau_project=args.tableau_project,
            tableau_site_id=args.tableau_site_id,
            tableau_username=args.tableau_username,
            tableau_password=tableau_password,
        )

    if selected_command == "load_sample":
        extractor.load_sample(
            sql_query=sql_string,
            source_table=args.source_table_id,
            tab_ds_name=args.tableau_datasource,
            sample_rows=args.sample_rows,
        )

    if selected_command == "export_load":
        extractor.export_load(
            sql_query=sql_string,
            source_table=args.source_table_id,
            tab_ds_name=args.tableau_datasource,
        )

    if selected_command == "append":
        extractor.append_to_datasource(
            sql_query=sql_string,
            source_table=args.source_table_id,
            tab_ds_name=args.tableau_datasource,
        )

    if selected_command in ("update", "delete"):
        exclusive_args(
            args,
            "match_columns",
            "match_conditions_json",
            required=True,
            message="Must specify either match_columns OR match_conditions_json when command is update or delete",
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


if __name__ == "__main__":
    main()
