# -*- coding: utf-8 -*-
"""
Tableau Community supported Hyper API sample

This module provies an Abstract Base Class with some utility methods to extract
from cloud databases to "live to hyper" Tableau Datasources.  This implements a
basic set of query and table extract functions based on the Python DBAPI standard.

The simplest way to add an implementation for a specific database is to extend this
with a database specific class that implements the following abstract methods:
- source_cursor() - Handles authentication and other implementation specific
 dependencies or optimizations (e.g. arraysize) and returns a DBAPI Cursor
 to the source database
- hyper_sql_type(source_column) - Finds the corresponding Hyper column type for
 the specified source column
- hyper_table_definition(source_table, hyper_table_name) - Runs introspection
 against the named source table and returns a Hyper table definition

Database specific Extractor classes may override the basic DBAPIv2 query handing
methods to add optimisations for exports via cloud storage etc based on additional
functionality offered by the database vendor supplied client libraries.

-----------------------------------------------------------------------------

This file is the copyrighted property of Tableau Software and is protected
by registered patents and other applicable U.S. and international laws and
regulations.

You may adapt this file and modify it to fit into your context and use it
as a template to start your own projects.

-----------------------------------------------------------------------------
"""


from abc import ABC, abstractmethod
import logging
from pathlib import Path
import os
import time
import uuid
from filelock import FileLock
from typing import Dict, Optional, Any, Union, List, Iterable, Generator

import tableauserverclient as TSC
import tableau_restapi_helpers as REST
from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    Inserter,
    CreateMode,
    TableDefinition,
    SqlType,
    escape_string_literal,
)

logger = logging.getLogger("hyper_samples.extractor.base")


def log_execution_time(method):
    # Decorator used during debugging to time execution
    def execution_timer(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logging.info("{0!r} completed in {1:2.4F} ms".format(
            method.__name__, (te - ts) * 1000))
        return result
    return execution_timer


TELEMETRY: Telemetry = Telemetry.SEND_USAGE_DATA_TO_TABLEAU
"""
    TELEMETRY: Send usage data to Tableau
        Set to Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU to disable
"""

TEMP_DIR: str = "/tmp"
"""
    TEMP_DIR (str): Local staging directory for hyper files, database exports etc.
"""

SAMPLE_ROWS: int = 1000
"""
    SAMPLE_ROWS (int): Default number of rows for LIMIT when using load_sample
"""

ASYNC_JOB_POLL_INTERVAL: int = 5
"""
    ASYNC_JOB_POLL_INTERVAL (int): How many seconds to wait between polls for Asynchronous Job completion
"""

DATASOURCE_LOCK_TIMEOUT: int = 60
"""
    DATASOURCE_LOCK_TIMEOUT (int): This  prevent multiple requests from being run against the
    same Tableau datasource at the same time.  This only guards against simultaneous jobs on the
    same host so you will need to implement this check in your scheduler if running accross
    multiple hosts
"""

DATASOURCE_LOCKFILE_PREFIX: str = "/var/lock/tableau_extractor"
"""
    DATASOURCE_LOCK_TIMEOUT (str): Defines the location of lockfiles
"""

DEFAULT_SITE_ID: str = ""
"""
    DEFAULT_SITE_ID (str): Default site ID
"""

HYPER_CONNECTION_PARAMETERS: Dict[str, str] = {
    "lc_time": "en_GB", "date_style": "YMD"}
"""
    HYPER_CONNECTION_PARAMETERS (dict): Options are documented in the Tableau Hyper API
    documentation, chapter “Connection Settings

    - lc_time - Controls the Locale setting that is used for dates. A Locale controls
        which cultural preferences the application should apply. For example, the literal
        Januar 1. 2002 can be converted to a date with the German locale de but not with the
        English locale en_US.

        Default value: en_US
        Allowed values start with a two-letter ISO-639 language code and an optional two-letter
            ISO-3166 country code. If a country code is used, an underscore has to be used to
            separate it from the language code. Some examples are: en_US (English: United States),
            en_GB (English: Great Britain), de (German), de_AT (German: Austria).

    - date_style - Controls how date strings are interpreted. Y, M and D stand for Year,
        Month, and Day respectively.

        Default value: MDY
        Accepted values: MDY, DMY, YMD, YDM
"""

DBAPI_BATCHSIZE: int = 1000
"""
    DBAPI_BATCHSIZE (int): Window size for query execution via DBAPI Cursor
    Defines how many lines are fetched for each call to fetchmany().
"""


class TableauJobError(Exception):
    """Exception: Tableau Job Failed"""

    pass


class TableauResourceNotFoundError(Exception):
    """Exception: Tableau Resource not found"""

    pass


class HyperSQLTypeMappingError(Exception):
    """Exception: Could not identify a target Hyper field type for source database field"""

    pass


def tempfile_name(prefix: str = "", suffix: str = "") -> str:
    return "{}/tableau_extractor_{}{}{}".format(
        TEMP_DIR, prefix, uuid.uuid4().hex, suffix
    )


class BaseExtractor(ABC):
    """
    Abstract Base Class defining the standard Extractor Interface

    Authentication to Tableau Server can be either by Personal Access Token or
     Username and Password.

    Constructor Args:
    - tableau_hostname (string): URL for Tableau Server, e.g. "http://localhost"
    - tableau_site_id (string): Tableau site identifier - if default use ""
    - tableau_project (string): Tableau project identifier
    - staging_bucket (string): Cloud storage bucket used for db extracts
    - tableau_token_name (string): PAT name
    - tableau_token_secret (string): PAT secret
    - tableau_username (string): Tableau username
    - tableau_password (string): Tableau password
    NOTE: Authentication to Tableau Server can be either by Personal Access Token or
     Username and Password.  If both are specified then token takes precedence.
    """

    def __init__(
        self,
        tableau_hostname: str,
        tableau_project: str,
        tableau_site_id: str = DEFAULT_SITE_ID,
        staging_bucket: Optional[str] = None,
        tableau_token_name: Optional[str] = None,
        tableau_token_secret: Optional[str] = None,
        tableau_username: Optional[str] = None,
        tableau_password: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.tableau_site_id = tableau_site_id
        self.tableau_hostname = tableau_hostname
        if tableau_token_name:
            self.tableau_auth = TSC.PersonalAccessTokenAuth(
                token_name=tableau_token_name,
                personal_access_token=tableau_token_secret,
                site_id=tableau_site_id,
            )
        else:
            self.tableau_auth = TSC.TableauAuth(
                username=tableau_username,
                password=tableau_password,
                site_id=tableau_site_id,
            )
        self.tableau_server = TSC.Server(
            tableau_hostname, use_server_version=True)
        self.tableau_server.auth.sign_in(self.tableau_auth)
        self.tableau_project_name = tableau_project
        self.tableau_project_id = self._get_project_id(tableau_project)
        self.staging_bucket = staging_bucket
        self.dbapi_batchsize = DBAPI_BATCHSIZE

    def _datasource_lock(self, tab_ds_name: str) -> FileLock:
        """
        Returns a posix lock for the named datasource.
        NOTE: Exclusive lock is not actually acquired until you call "with lock:" or "lock.acquire():
        e.g.
            lock=self._datasource_lock(tab_ds_name)
            with lock:
                #exclusive lock active for datasource here
            #exclusive lock released for datasource here
        """
        lock_path = "{}.{}.{}.lock".format(
            DATASOURCE_LOCKFILE_PREFIX, self.tableau_project_id, tab_ds_name
        )
        return FileLock(lock_path, timeout=DATASOURCE_LOCK_TIMEOUT)

    def _get_project_id(self, tab_project: str) -> str:
        """
        Return project_id for tab_project
        """
        all_projects, pagination_item = self.tableau_server.projects.get()

        for project in all_projects:
            if project.name == tab_project:
                return project.id

        logger.error("No project found for:{}".format(tab_project))
        raise TableauResourceNotFoundError(
            "No project found for:{}".format(tab_project)
        )

    def _get_datasource_id(self, tab_datasource: str) -> str:
        """
        Return id for tab_datasource
        """
        # Get project_id from project_name

        all_datasources, pagination_item = self.tableau_server.datasources.get()
        for datasource in all_datasources:
            if datasource.name == tab_datasource:
                return datasource.id

        raise TableauResourceNotFoundError(
            "No datasource found for:{}".format(tab_datasource)
        )

    def _wait_for_async_job(self, async_job_id: str) -> int:
        """
        Waits for async job to complete and returns finish_code
        """

        completed_at = None
        finish_code = None
        jobinfo = None
        while completed_at is None:
            time.sleep(ASYNC_JOB_POLL_INTERVAL)
            jobinfo = self.tableau_server.jobs.get_by_id(async_job_id)
            completed_at = jobinfo.completed_at
            finish_code = jobinfo.finish_code
            logger.info(
                "Job {} ... progress={} finishCode={}".format(
                    async_job_id, jobinfo.progress, finish_code
                )
            )
        if finish_code == "0":
            logger.info(
                "Job {} Completed: Finish Code: {}".format(
                    async_job_id, finish_code)
            )
        else:
            full_job_details = REST.get_job_details(
                self.tableau_hostname,
                self.tableau_server.auth_token,
                self.tableau_server.site_id,
                async_job_id,
            )
            logger.error(
                "Job {} Completed with non-zero Finish Code: {} : {}".format(
                    async_job_id, finish_code, full_job_details
                )
            )
            logger.error(
                "Check jobs pane in Tableau for detailed failure information")

        return finish_code

    def query_result_to_hyper_file(
        self,
        target_table_def: TableDefinition,
        cursor: Any = None,
        query_result_iter: Iterable[Iterable[object]] = None,
    ) -> Path:
        """
        Writes query output to a Hyper file
        Returns Path to hyper file

        target_table_def (TableDefinition): Schema for target extract table
        cursor : A Python DBAPI v2 compliant Cursor object
        query_result_iter : Iterator containing result rows

        Must specify either cursor or query_result_iter, Error if both are specified
        """

        if not (bool(cursor) ^ bool(query_result_iter)):
            raise Exception("Must specify either cursor OR query_result_iter")

        path_to_database = Path(tempfile_name(prefix="temp_", suffix=".hyper"))

        with HyperProcess(telemetry=TELEMETRY) as hyper:

            # Creates new Hyper extract file
            # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
            with Connection(
                endpoint=hyper.endpoint,
                database=path_to_database,
                create_mode=CreateMode.CREATE_AND_REPLACE,
            ) as connection:

                connection.catalog.create_schema(
                    schema=target_table_def.table_name.schema_name
                )
                connection.catalog.create_table(
                    table_definition=target_table_def)
                with Inserter(connection, target_table_def) as inserter:
                    if query_result_iter is not None:
                        inserter.add_rows(query_result_iter)
                        inserter.execute()
                    else:
                        while True:
                            rows = cursor.fetchmany(size=self.dbapi_batchsize)
                            if rows:
                                inserter.add_rows(rows)
                            else:
                                break
                        inserter.execute()

                row_count = connection.execute_scalar_query(
                    query=f"SELECT COUNT(*) FROM {target_table_def.table_name}"
                )
                logger.info(
                    f"The number of rows in table {target_table_def.table_name} is {row_count}."
                )

            logger.info("The connection to the Hyper file has been closed.")
        logger.info("The Hyper process has been shut down.")
        return path_to_database

    def csv_to_hyper_file(
        self,
        path_to_csv: str,
        target_table_def: TableDefinition,
        csv_format_options: str = """NULL 'NULL', delimiter ',', header FALSE""",
    ) -> Path:
        """
        Writes csv to a Hyper files
        Returns Path to hyper file

        path_to_csv (str): CSV file containing result rows
        target_table_def (TableDefinition): Schema for target extract table
        csv_format_options (str): Specify csv file format options for COPY command
            default csv format options: "NULL 'NULL', delimiter ',', header FALSE"
        """

        path_to_database = Path(tempfile_name(prefix="temp_", suffix=".hyper"))
        with HyperProcess(telemetry=TELEMETRY) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=path_to_database,
                create_mode=CreateMode.CREATE_AND_REPLACE,
                parameters=HYPER_CONNECTION_PARAMETERS,
            ) as connection:

                connection.catalog.create_schema(
                    schema=target_table_def.table_name.schema_name
                )
                connection.catalog.create_table(
                    table_definition=target_table_def)

                count_rows = connection.execute_command(
                    command=f"COPY {target_table_def.table_name} from {escape_string_literal(path_to_csv)} "
                    f"with (format csv, {csv_format_options})"
                )
                logger.info(
                    f"Inserted {count_rows} into table {target_table_def.table_name}"
                )
            logger.debug("The connection to the Hyper file has been closed.")
        logger.debug("The Hyper process has been shut down.")
        return path_to_database

    def publish_hyper_file(
        self,
        path_to_database: Path,
        tab_ds_name: str,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> str:
        """
        Publishes a Hyper file to Tableau Server

        path_to_database (string): Hyper file to publish
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)

        Returns the Datasource ID
        """
        # This is used to create new datasources and to implement the APPEND action
        #
        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(
            self.tableau_project_id, name=tab_ds_name)

        logger.info(f"Publishing {path_to_database} to {tab_ds_name}...")
        # Publish datasource
        lock = self._datasource_lock(tab_ds_name)
        with lock:
            datasource = self.tableau_server.datasources.publish(
                datasource, path_to_database, publish_mode
            )
        logger.info(
            "Datasource published. Datasource ID: {0}".format(datasource.id))
        return datasource.id

    def update_datasource_from_hyper_file(
        self,
        path_to_database: Optional[Path],
        tab_ds_name: str,
        match_columns: Union[List[str], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: Optional[str] = "updated_rows",
        action: str = "UPDATE",
    ):
        """
        Updates a datasource on Tableau Server with a changeset from a hyper file

        path_to_database (string): The hyper file containing the changeset
        tab_ds_name (string): Target Tableau datasource
        match_columns (array of tuples): Array of (source_col, target_col) pairs
        match_conditions_json (string): Define conditions for matching rows in json format.  See Hyper API guide for details.
        changeset_table_name (string): The name of the table in the hyper file that contains the changest (default="updated_rows")
        action (string): One of "INSERT", "UPDATE" or "DELETE" (Default="UPDATE")

        NOTES:
        - match_columns overrides match_conditions_json if both are specified
        - set path_to_database to None if conditional delete
            (e.g. json_request="condition": { "op": "<", "target-col": "col1", "const": {"type": "datetime", "v": "2020-06-00"}})
        - When action is DELETE, it is an error if the source table contains any additional columns not referenced by the condition. Those columns are pointless and we want to let the user know, so they can fix their scripts accordingly.
        """
        action = action.upper()
        match_conditions_args = []
        if match_columns is not None:
            for match_pair in match_columns:
                match_conditions_args.append(
                    {
                        "op": "=",
                        "source-col": match_pair[0],
                        "target-col": match_pair[1],
                    }
                )
            if len(match_conditions_args) > 1:
                match_conditions_json = {
                    "op": "and", "args": match_conditions_args}
            else:
                match_conditions_json = match_conditions_args[0]

        if action == "UPDATE":
            # Update action
            # The Update operation updates existing tuples inside the target table.
            # It uses a `condition` to decide which tuples (rows) to update.
            #
            # Example
            #
            # {"action": "update",
            #  "target-table": "my_data",
            #  "source-table": "uploaded_table",
            #  "condition": {"op": "=", "target-col": "row_id", "source-col": "update_row_id"}
            # }
            # Parameters:
            #
            # `target-table` (string; required): the table name inside the target database into which we will insert data
            # `target-schema` (string; required): the schema name inside the target database; default: the one, unique schema name inside the target database in case the target db has only one schema; error otherwise
            # `source-table` (string; required): the table name inside the source database from which the data will be inserted
            # `source-schema` (string; required): analogous to target-schema, but for the source table
            # `condition` (condition-specification; required): specifies the condition used to select the columns to be updated
            # To determine the updated columns, we will use the following default algorithm:
            #
            # We will map columns from the the source table onto the target table, based on their column name. This mapping will not consider the order of columns inside the tables, but will be solely based on column names. The same rules as for insert apply for this column mapping.
            # If any column from the source table does not have a corresponding column in the target table and is not referenced by the `condition` either, we will raise an error
            # This algorithm ensures that:
            #
            # we update all columns if they have a matching name
            # we bring mismatching columns to the users attention (e.g., if his column name is “userid” instead of “user_id”)
            # The update action is mapped to a SQL query of the form
            #
            # UPDATE target_db.<target-schema>.<target>
            # SET
            #    <target column 1> = <source column 1>,
            #    <target column 2> = <source column 2>,
            #    ...
            # FROM <source>
            # WHERE <condition>
            # -------
            json_request = {
                "actions": [
                    # UPDATE action
                    {
                        "action": "update",
                        "source-schema": "Extract",
                        "source-table": changeset_table_name,
                        "target-schema": "Extract",
                        "target-table": "Extract",
                        "condition": match_conditions_json,
                    },
                ]
            }
        elif action == "DELETE":
            # # The Delete operation deletes tuples from its target table.
            # It uses its `condition` to determine which tuples to delete.
            #
            # Example 1
            #
            # {"action": "delete",
            #  "target-table": "my_extract_table",
            #  "condition": {
            #    "op": "<",
            #    "target-col": "col1",
            #    "const": {"type": "datetime", "v": "2020-06-00"}}
            # }
            # Example 2
            #
            # {"action": "delete",
            #  "target-table": "my_extract_table",
            #  "source-table": "deleted_row_id_table",
            #  "condition": {"op": "=", "target-col": "id", "source-col": "deleted_id"}
            # }
            # Parameters:
            #
            # `target-table` (string; required): the table name inside the source database from which we will insert data
            # `target-schema` (string; required): analogous to source-schema, but for the source table
            # `source-table` (string; optional): the table name inside the target database into which the data will be inserted
            # `source-schema` (string; optional): the schema name inside the target database; default: the one, unique schema name inside the target database in case the target db has only one schema; error otherwise
            # `condition` (condition-specification; required): specifies the condition used to select the columns for deletion
            #
            # See separate section for the definition of `condition`s.
            #
            # If no `source` table is specified, the delete action will be translated to
            #
            # DELETE FROM target_db.<target-schema>.<target>
            # WHERE <condition>
            # This variant will be useful for “sliding window” extract, as described below in the examples section
            #
            # If a `source` table is specified, the delete action will be translated to
            #
            # DELETE FROM target_db.<target-schema>.<target>
            # WHERE EXISTS (
            #    SELECT * FROM <source-db>.<source-schema>.<source>
            #    WHERE <condition>
            # )
            # This variant is useful to delete many tuples, e.g., based on their row ID
            #
            # It is an error if the source table contains any additional columns not referenced by the condition. Those columns are pointless and we want to let the user know, so they can fix their scripts accordingly.
            if path_to_database is None:
                json_request = {
                    "actions": [
                        # UPDATE action
                        {
                            "action": "delete",
                            "target-schema": "Extract",
                            "target-table": "Extract",
                            "condition": match_conditions_json,
                        },
                    ]
                }
            else:
                json_request = {
                    "actions": [
                        # UPDATE action
                        {
                            "action": "delete",
                            "source-schema": "Extract",
                            "source-table": changeset_table_name,
                            "target-schema": "Extract",
                            "target-table": "Extract",
                            "condition": match_conditions_json,
                        },
                    ]
                }
        elif action == "INSERT":
            # The "insert" operation appends one or more rows from a table inside the uploaded Hyper file into the updated Hyper file on the server.
            #
            # Example
            #
            # {"action": "insert", "source-table": "added_users", "target-table": "current_users"}
            # Parameters:
            #
            # `target-table` (string; required): the table name inside the target database from which we will insert data
            # `target-schema` (string; required): analogous to target-schema, but for the source table
            # `source-table` (string; required): the table name inside the source database into which the data will be inserted
            # `source-schema` (string; required): the schema name inside the source database; default: the one, unique schema name inside the target database in case the target db has only one schema; error otherwise
            json_request = {
                "actions": [
                    # INSERT action
                    {
                        "action": "insert",
                        "source-schema": "Extract",
                        "source-table": changeset_table_name,
                        "target-schema": "Extract",
                        "target-table": "Extract",
                    },
                ]
            }
        else:
            raise Exception(
                "Unknown action {} specified for _update_datasource_from_hyper_file".format(
                    action
                )
            )
        file_upload_id = None
        if path_to_database is not None:
            # Update or delete by row_id
            file_upload_id = REST.upload_file(
                path_to_database,
                self.tableau_hostname,
                self.tableau_server.auth_token,
                self.tableau_server.site_id,
            )
        ds_id = self._get_datasource_id(tab_ds_name)
        lock = self._datasource_lock(tab_ds_name)
        with lock:
            async_job_id = REST.patch_datasource(
                server=self.tableau_hostname,
                auth_token=self.tableau_server.auth_token,
                site_id=self.tableau_server.site_id,
                datasource_id=ds_id,
                file_upload_id=file_upload_id,
                request_json=json_request,
            )
            finish_code = self._wait_for_async_job(async_job_id)
            if finish_code != "0":
                raise TableauJobError(
                    "Patch job {} terminated with non-zero return code:{}".format(
                        async_job_id, finish_code
                    )
                )

    @abstractmethod
    def new_source_cursor(self) -> Any:
        """
        Returns a DBAPI Cursor to the source database
        """

    @abstractmethod
    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the corresponding Hyper column type for source_column

        source_column (obj): Source column descriptor (e.g. DBAPI Column description tuple)

        Returns a tableauhyperapi.SqlType Object
        """

    @abstractmethod
    def hyper_table_definition(
        self, source_table: Any, hyper_table_name: str = "Extract"
    ) -> TableDefinition:
        """
        Build a hyper table definition from source_table

        source_table (obj): Source table or query resultset descriptor
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """

    def query_to_hyper_files(
        self,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        hyper_table_name: str = "Extract",
    ) -> Generator[Path, None, None]:
        """
        Executes sql_query or exports rows from source_table and writes output
        to one or more hyper files.  This base implementation uses the standard
        DBAPIv2 cursor methods and this should be overwritten if your native
        database client libraries include more efficient export etc. routines.

        Returns Iterable of Paths to hyper files

        sql_query (string): SQL to pass to the source database
        source_table (string): Source table ref ("project ID.dataset ID.table ID")
        hyper_table_name (string): Name of the target Hyper table, default=Extract

        NOTES:
        - Specify either sql_query OR source_table, error if both specified
        """

        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")

        if source_table:
            sql_query = "SELECT * from {}".format(source_table)

        cursor = self.new_source_cursor()
        cursor.execute(sql_query)
        target_table_def = self.hyper_table_definition(
            source_table=cursor.description, hyper_table_name=hyper_table_name
        )
        path_to_database = self.query_result_to_hyper_file(target_table_def=target_table_def,
                                                           cursor=cursor)
        yield path_to_database
        return

    @log_execution_time
    def load_sample(
        self,
        source_table: str,
        tab_ds_name: str,
        sample_rows: int = SAMPLE_ROWS,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> None:
        """
        Loads a sample of rows from source_table to Tableau Server

        source_table (string): Source table ref ("project ID.dataset ID.table ID")
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)
        sample_rows (int): How many rows to include in the sample (default=SAMPLE_ROWS)
        """

        # loads in the first sample_rows of a bigquery table
        # set publish_mode=TSC.Server.PublishMode.Overwrite to refresh a sample
        sql_query = "SELECT * FROM `{}` LIMIT {}".format(
            source_table, sample_rows)
        first_chunk = True
        for path_to_database in self.query_to_hyper_files(sql_query=sql_query):
            if first_chunk:
                self.publish_hyper_file(
                    path_to_database, tab_ds_name, publish_mode)
                first_chunk = False
            else:
                self.update_datasource_from_hyper_file(
                    path_to_database=path_to_database,
                    tab_ds_name=tab_ds_name,
                    action="INSERT",
                )
            os.remove(path_to_database)

    @log_execution_time
    def export_load(
        self,
        tab_ds_name,
        source_table: Optional[str] = None,
        sql_query: Optional[str] = None,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> None:
        """
        Bulk export the contents of source_table and load to Tableau Server

        source_table (string): Source table identifier
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)
        """

        first_chunk = True
        for path_to_database in self.query_to_hyper_files(source_table=source_table,sql_query=sql_query):
            if first_chunk:
                self.publish_hyper_file(
                    path_to_database, tab_ds_name, publish_mode)
                first_chunk = False
            else:
                self.update_datasource_from_hyper_file(
                    path_to_database=path_to_database,
                    tab_ds_name=tab_ds_name,
                    action="INSERT",
                )
            os.remove(path_to_database)

    @log_execution_time
    def append_to_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        changeset_table_name: str = "new_rows",
    ) -> None:
        """
        Appends the result of sql_query to a datasource on Tableau Server

        tab_ds_name (string): Target datasource name
        sql_query (string): The query string that generates the changeset
        source_table (string): Identifier for source table containing the changeset
        changeset_table_name (string): The name of the table in the hyper file that
            contains the changeset (default="new_rows")

        NOTES:
        - Must specify either sql_query OR source_table, error if both specified
        """

        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")

        for path_to_database in self.query_to_hyper_files(
            sql_query=sql_query,
            source_table=source_table,
            hyper_table_name=changeset_table_name,
        ):
            self.update_datasource_from_hyper_file(
                path_to_database=path_to_database,
                tab_ds_name=tab_ds_name,
                changeset_table_name=changeset_table_name,
                action="INSERT",
            )
            os.remove(path_to_database)

    @log_execution_time
    def update_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[str], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "updated_rows",
    ) -> None:
        """
        Updates a datasource on Tableau Server with the changeset from sql_query

        tab_ds_name (string): Target datasource name
        sql_query (string): The query string that generates the changeset
        source_table (string): Identifier for source table containing the changeset
        match_columns (array of tuples): Array of (source_col, target_col) pairs
        match_conditions_json (string): Define conditions for matching rows in json format.
            See Hyper API guide for details.
        changeset_table_name (string): The name of the table in the hyper file that contains
            the changeset (default="updated_rows")

        NOTES:
        - Specify either match_columns OR match_conditions_json, error if both specified
        - Specify either sql_query OR source_table, error if both specified
        """

        if not ((match_columns is None) ^ (match_conditions_json is None)):
            raise Exception(
                "Must specify either match_columns OR match_conditions_json"
            )
        if not ((sql_query is None) ^ (source_table is None)):
            raise Exception("Must specify either sql_query OR source_table")

        for path_to_database in self.query_to_hyper_files(
            sql_query=sql_query,
            source_table=source_table,
            hyper_table_name=changeset_table_name,
        ):
            self.update_datasource_from_hyper_file(
                path_to_database=path_to_database,
                tab_ds_name=tab_ds_name,
                match_columns=match_columns,
                match_conditions_json=match_conditions_json,
                changeset_table_name=changeset_table_name,
                action="UPDATE",
            )
            os.remove(path_to_database)

    @log_execution_time
    def delete_from_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[str], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "deleted_rowids",
    ) -> None:
        """
        Delete rows matching the changeset from sql_query from a datasource on Tableau Server
        Simple delete by condition when sql_query is None

        tab_ds_name (string): Target datasource name
        sql_query (string): The query string that generates the changeset
        source_table (string): Identifier for source table containing the changeset
        match_columns (array of tuples): Array of (source_col, target_col) pairs
        match_conditions_json (string): Define conditions for matching rows in json format.
            See Hyper API guide for details.
        changeset_table_name (string): The name of the table in the hyper file that contains
            the changeset (default="deleted_rowids")

        NOTES:
        - match_columns overrides match_conditions_json if both are specified
        - sql_query or source_table must only return columns referenced by the match condition
        - Specify either sql_query OR source_table, error if both specified
        - Set sql_query and source_table to None if conditional delete
          e.g. json_request="condition":
                { "op": "<", "target-col": "col1", "const": {"type": "datetime", "v": "2020-06-00"}})
        - Specify either match_columns OR match_conditions_json, error if both specified
        """
        if not ((match_columns is None) ^ (match_conditions_json is None)):
            raise Exception(
                "Must specify either match_columns OR match_conditions_json"
            )
        if not ((sql_query is None) ^ (source_table is None)):
            raise Exception("Must specify either sql_query OR source_table")

        if (sql_query is None) and (source_table is None):
            # Conditional delete
            if match_conditions_json is None:
                raise Exception(
                    "Must specify match_conditions_json if sql_query and source_table are both None"
                )
            self.update_datasource_from_hyper_file(
                path_to_database=None,
                tab_ds_name=tab_ds_name,
                match_columns=None,
                match_conditions_json=match_conditions_json,
                changeset_table_name=None,
            )
        else:
            for path_to_database in self.query_to_hyper_files(
                sql_query=sql_query,
                source_table=source_table,
                hyper_table_name=changeset_table_name,
            ):
                self.update_datasource_from_hyper_file(
                    path_to_database=path_to_database,
                    tab_ds_name=tab_ds_name,
                    match_columns=match_columns,
                    match_conditions_json=match_conditions_json,
                    changeset_table_name=changeset_table_name,
                    action="DELETE",
                )
                os.remove(path_to_database)
