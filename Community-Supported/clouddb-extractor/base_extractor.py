# -*- coding: utf-8 -*-
"""
Tableau Community supported Hyper API sample.

This module provies an Abstract Base Class with some utility methods to extract
from cloud databases to "live to hyper" Tableau Datasources.  This implements a
basic set of query and table extract functions based on the Python DBAPIv2 standard.

The simplest way to add an implementation for a specific database is to extend this
with a database specific class that implements the following abstract methods:
- hyper_sql_type(source_column) - Finds the corresponding Hyper column type for
 the specified source column
- hyper_table_definition(source_table, hyper_table_name) - Runs introspection
 against the named source table and returns a Hyper table definition
- source_database_cursor() - Handles authentication and other implementation specific
 dependencies or optimizations (e.g. arraysize) and returns a DBAPI Cursor
 to the source database

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
import functools
from pathlib import Path
import os
import time
import uuid
from filelock import FileLock
from typing import Dict, Optional, Any, Union, List, Iterable, Generator
import re

import tableauserverclient as TSC
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

# Globals
TELEMETRY: Telemetry = Telemetry.SEND_USAGE_DATA_TO_TABLEAU
"""
    TELEMETRY: Send usage data to Tableau
        Set to Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU to disable
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

DATASOURCE_LOCKFILE_PREFIX: str = "tableau_extractor"
"""
    DATASOURCE_LOCKFILE_PREFIX (str): Defines the naming convention for lockfiles
"""

TEMP_DIR: str = "/tmp"
"""
    TEMP_DIR (str): Local staging directory for hyper files, database exports etc.
"""
if os.name == 'nt':
    TEMP_DIR = os.environ.get('TEMP')

DEFAULT_SITE_ID: str = ""
"""
    DEFAULT_SITE_ID (str): Default site ID
"""

HYPER_CONNECTION_PARAMETERS: Dict[str, any] = {
    "lc_time": "en_GB", "date_style": "YMD",
}
"""
    HYPER_CONNECTION_PARAMETERS (dict): Options are documented in the Tableau Hyper API
    documentation, chapter “Connection Settings"

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

HYPER_DATABASE_PARAMETERS: Dict[str, any] = {
    "default_database_version": "2",
}
"""
    HYPER_DATABASE_PARAMETERS (dict): Options are documented in the Tableau Hyper API
    documentation, chapter "Database Settings"

    - default_database_version - Specifies the default database file format version that will 
      be used to create new database files.
"""

DBAPI_BATCHSIZE: int = 10000
"""
    DBAPI_BATCHSIZE (int): Window size for query execution via DBAPI Cursor
    Defines how many lines are fetched for each call to fetchmany().
"""

CONFIGURATION_FILE: str = "config.yml"
"""
    CONFIGURATION_FILE (str): Defines defaults for this utility
"""

SQL_IDENTIFIER_MAXLENGTH: int = 64
"""
    SQL_IDENTIFIER_MAXLENGTH (int): Returns error if table name exceeds defined length
"""

class TableauJobError(Exception):
    """Exception: Tableau Job Failed."""

    pass


class TableauResourceNotFoundError(Exception):
    """Exception: Tableau Resource not found."""

    pass


class HyperSQLTypeMappingError(Exception):
    """Exception: Could not identify a target Hyper field type for source database field."""

    pass


class ExtractorConfigurationError(Exception):
    """Exception: config.yml is missing required section(s) or argument(s)."""


def log_execution_time(func):
    """Decorator: Log function execution time."""

    def execution_timer(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        logging.info("{0!r} completed in {1:2.4F} ms".format(func.__name__, (te - ts) * 1000))
        return result

    return execution_timer


def debug(func):
    """Decorator: Log the function arguments and return value."""

    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]  # 1
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
        signature = ", ".join(args_repr + kwargs_repr)  # 3
        logger.debug(f"Calling {func.__name__}({signature})")

        value = func(*args, **kwargs)

        logger.debug(f"{func.__name__!r} returned {value!r}")  # 4

        return value

    return wrapper_debug


def tempfile_name(prefix: str = "", suffix: str = "") -> str:
    """Return a unique temporary file name."""
    return os.path.join(TEMP_DIR, "{}_tableau_extractor_{}{}".format(prefix, uuid.uuid4().hex, suffix))


class BaseExtractor(ABC):
    """
    Abstract Base Class defining the standard Extractor Interface.

    Authentication to Tableau Server can be either by Personal Access Token or
    Username and Password.

    Constructor Args:
    - source_database_config (dict): Source database parameters
    - tableau_hostname (string): URL for Tableau Server, e.g. "http://localhost"
    - tableau_site_id (string): Tableau site identifier - if default use ""
    - tableau_project (string): Tableau project identifier
    - tableau_token_name (string): PAT name
    - tableau_token_secret (string): PAT secret
    - tableau_username (string): Tableau username
    - tableau_password (string): Tableau password
    NOTE: Authentication to Tableau Server can be either by Personal Access Token or
     Username and Password.  If both are specified then token takes precedence.
    """

    def __init__(
        self,
        source_database_config: Dict[str, Any],
        tableau_hostname: str,
        tableau_project: str,
        tableau_site_id: str = DEFAULT_SITE_ID,
        tableau_token_name: Optional[str] = None,
        tableau_token_secret: Optional[str] = None,
        tableau_username: Optional[str] = None,
        tableau_password: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.source_database_config = source_database_config
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
        self.tableau_server = TSC.Server(tableau_hostname, use_server_version=True)
        self.tableau_server.auth.sign_in(self.tableau_auth)
        self.tableau_project_name = tableau_project
        self.tableau_project_id = self._get_project_id(tableau_project)
        self.dbapi_batchsize = DBAPI_BATCHSIZE
        self.sql_identifier_quote = """`"""
        self.sql_identifier_endquote = None

    @property
    def sql_identifier_quote(self):
        """
        Property defines how table identifiers etc. are quoted when SQL is generated.

        Default quote character is ` - i.e. `myschema.mytable`
        """
        return self.__sql_identifier_quote

    @sql_identifier_quote.setter
    def sql_identifier_quote(self, new_char):
        self.__sql_identifier_quote = new_char

    @property
    def sql_identifier_endquote(self):
        """
        Property defines how table identifiers etc. are quoted when SQL is generated.

        Default is None which uses the same as sql_identifier quote
        Set this is start and end quote character is different (e.g. "[" and "]")
        """
        return self.__sql_identifier_endquote

    @sql_identifier_endquote.setter
    def sql_identifier_endquote(self, new_char):
        self.__sql_identifier_endquote = new_char

    def quoted_sql_identifier(self, sql_identifier: str) -> str:
        """Parse a SQL Identifier (e.g. Table/Column Name) and return escaped and quoted version."""
        quote_start = self.sql_identifier_quote
        if self.__sql_identifier_endquote is None:
            quote_end = quote_start
        else:
            quote_end = self.sql_identifier_endquote

        sql_identifier = sql_identifier.strip()
        if sql_identifier is None:
            raise Exception("Expected SQL identifier (e.g. Table Name, Column Name) found None")

        if len(sql_identifier) > SQL_IDENTIFIER_MAXLENGTH:
            raise Exception("Invalid SQL identifier: {} - exceeded max allowed length: {}".format(sql_identifier, SQL_IDENTIFIER_MAXLENGTH))

        char_whitelist = re.compile(r"\A[\[\w\.\-\]]*\Z")
        if char_whitelist.match(sql_identifier) is None:
            raise Exception("Invalid SQL identifier: {} - found invalid characters".format(sql_identifier))

        if sql_identifier[0] == quote_start:
            logger.debug(f"SQL Identifier {sql_identifier} appears to be already quoted")
            return sql_identifier
        else:
            sql_identifier_parts = sql_identifier.split(".")
            join_str = "{}.{}".format(quote_end, quote_start)
            return "{}{}{}".format(quote_start, join_str.join(sql_identifier_parts), quote_end)

    @abstractmethod
    def source_database_cursor(self) -> Any:
        """Return a DBAPI Cursor to the source database."""

    @abstractmethod
    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Find the corresponding Hyper column type for source_column.

        source_column (obj): Source column descriptor (e.g. DBAPI Column description tuple)

        Returns a tableauhyperapi.SqlType Object
        """

    @abstractmethod
    def hyper_table_definition(self, source_table: Any, hyper_table_name: str = "Extract") -> TableDefinition:
        """
        Build a hyper table definition from source_table.

        source_table (obj): Source table or query resultset descriptor
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """

    def _datasource_lock(self, tab_ds_name: str) -> FileLock:
        """
        Return a posix lock for the named datasource.

        NOTE: Exclusive lock is not actually acquired until you call "with lock:" or "lock.acquire():
        e.g.
            lock=self._datasource_lock(tab_ds_name)
            with lock:
                #exclusive lock active for datasource here
            #exclusive lock released for datasource here
        """
        lock_path = os.path.join(TEMP_DIR, "{}.{}.{}.lock".format(DATASOURCE_LOCKFILE_PREFIX, self.tableau_project_id, tab_ds_name))
        return FileLock(lock_path, timeout=DATASOURCE_LOCK_TIMEOUT)

    def _get_project_id(self, tab_project: str) -> str:
        """Return project_id for tab_project."""

        for project in TSC.Pager(self.tableau_server.projects):
            if project.name == tab_project:
                logger.info(f"Found tableau project {project.name} : {project.id}")
                return project.id

        logger.error("No project found for:{}".format(tab_project))
        raise TableauResourceNotFoundError("No project found for:{}".format(tab_project))

    def _get_datasource_by_name(self, tab_datasource: str) -> str:
        """Return datasource object with name=tab_datasource."""

        for datasource in TSC.Pager(self.tableau_server.datasources):
            if datasource.name == tab_datasource and datasource.project_id == self.tableau_project_id:
                logger.info(f"Found tableau datasource {datasource.name} : {datasource.project_id}")
                return datasource

        raise TableauResourceNotFoundError("No datasource found for:{} with project id {}".format(tab_datasource, self.tableau_project_id))

    @log_execution_time
    def query_result_to_hyper_file(
        self,
        target_table_def: Optional[TableDefinition] = None,
        cursor: Any = None,
        query_result_iter: Optional[Iterable[Iterable[object]]] = None,
        hyper_table_name: str = "Extract",
    ) -> Path:
        """
        Write query output to a Hyper file.

        Returns Path to hyper file

        target_table_def (TableDefinition): Schema for target extract table
          (Required if using query_result_iter)
        cursor : A Python DBAPI v2 compliant Cursor object
        query_result_iter : Iterator containing result rows
        hyper_table_name (string): Name of the target Hyper table, default=Extract
          (Only used if target_table_def is None)

        Must specify either cursor or query_result_iter, Error if both are specified
        """
        rows = None

        if not (bool(cursor) ^ bool(query_result_iter)):
            raise Exception("Must specify either cursor OR query_result_iter")

        if target_table_def is None:
            if cursor is None:
                raise Exception("Must specify target_table_def when using query_result_iter")
            else:
                # If using a server side cursor then description may be None until first call
                if cursor.description is None:
                    rows = cursor.fetchmany(self.dbapi_batchsize)

                if cursor.description is None:
                    raise Exception("DBAPI Cursor did not return any schema description for query:{}".format(cursor.query))
                target_table_def = self.hyper_table_definition(source_table=cursor.description, hyper_table_name=hyper_table_name)
        assert target_table_def is not None

        path_to_database = Path(tempfile_name(prefix="temp_", suffix=".hyper"))
        with HyperProcess(telemetry=TELEMETRY, parameters=HYPER_DATABASE_PARAMETERS) as hyper:

            # Creates new Hyper extract file
            # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
            with Connection(
                endpoint=hyper.endpoint,
                database=path_to_database,
                create_mode=CreateMode.CREATE_AND_REPLACE,
                parameters=HYPER_CONNECTION_PARAMETERS,
            ) as connection:
                connection.catalog.create_schema(schema=target_table_def.table_name.schema_name)
                connection.catalog.create_table(table_definition=target_table_def)
                with Inserter(connection, target_table_def) as inserter:
                    if query_result_iter is not None:
                        assert cursor is None
                        inserter.add_rows(query_result_iter)
                        inserter.execute()
                    else:
                        assert cursor is not None
                        logger.info(f"Spooling cursor to hyper file, DBAPI_BATCHSIZE={self.dbapi_batchsize}")
                        batches = 0
                        if rows:
                            # We have rows in the buffer from where we determined the cursor.description for server side cursor
                            inserter.add_rows(rows)
                            batches += 1
                        while True:
                            rows = cursor.fetchmany(self.dbapi_batchsize)
                            if rows:
                                inserter.add_rows(rows)
                                batches += 1
                                if batches % 10 == 0:
                                    logger.info(f"Completed Batch {batches}")
                            else:
                                break
                        inserter.execute()

                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {target_table_def.table_name}")
                logger.info(f"The number of rows in table {target_table_def.table_name} is {row_count}.")

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
        Write csv to a Hyper file.

        Returns Path to hyper file

        path_to_csv (str): CSV file containing result rows
        target_table_def (TableDefinition): Schema for target extract table
        csv_format_options (str): Specify csv file format options for COPY command
            default csv format options: "NULL 'NULL', delimiter ',', header FALSE"
        """
        path_to_database = Path(tempfile_name(prefix="temp_", suffix=".hyper"))
        with HyperProcess(telemetry=TELEMETRY, parameters=HYPER_DATABASE_PARAMETERS) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=path_to_database,
                create_mode=CreateMode.CREATE_AND_REPLACE,
                parameters=HYPER_CONNECTION_PARAMETERS,
            ) as connection:

                connection.catalog.create_schema(schema=target_table_def.table_name.schema_name)
                connection.catalog.create_table(table_definition=target_table_def)

                count_rows = connection.execute_command(
                    command=f"COPY {target_table_def.table_name} from {escape_string_literal(path_to_csv)} " f"with (format csv, {csv_format_options})"
                )
                logger.info(f"Inserted {count_rows} into table {target_table_def.table_name}")
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
        Publish a Hyper file to Tableau Server.

        path_to_database (string): Hyper file to publish
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)

        Returns the Datasource ID
        """
        # This is used to create new datasources and to implement the APPEND action
        #
        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(self.tableau_project_id, name=tab_ds_name)

        logger.info(f"Publishing {path_to_database} to {tab_ds_name}...")
        # Publish datasource
        lock = self._datasource_lock(tab_ds_name)
        with lock:
            datasource = self.tableau_server.datasources.publish(datasource, path_to_database, publish_mode)
        logger.info("Datasource published. Datasource ID: {0}".format(datasource.id))
        return datasource.id

    @log_execution_time
    def patch_datasource(
        self,
        tab_ds_name: str,
        actions_json: List[Any],
        path_to_database: Optional[Path] = None,
    ):
        """
        Run a action batch against a datasource on Tableau Server

        tab_ds_name (string): Target Tableau datasource
        path_to_database (string): The hyper file containing the changeset
        match_columns (array of tuples): Array of (source_col, target_col) pairs
        match_conditions_json (string): Define conditions for matching rows in json format.  See Hyper API guide for details.
        changeset_table_name (string): The name of the table in the hyper file that contains the changest (default="updated_rows")
        actions_json (string): One of "INSERT", "UPDATE" or "DELETE" (Default="UPDATE")

        NOTES:
        - match_columns overrides match_conditions_json if both are specified
        - set path_to_database to None if conditional delete
            (e.g. json_request="condition": { "op": "<", "target-col": "col1", "const": {"type": "datetime", "v": "2020-06-00"}})
        - When action is DELETE, it is an error if the source table contains any additional columns not referenced by the condition. Those columns are pointless and we want to let the user know, so they can fix their scripts accordingly.
        """
        this_datasource = self._get_datasource_by_name(tab_ds_name)
        lock = self._datasource_lock(tab_ds_name)
        with lock:
            request_id = str(uuid.uuid4())
            logger.info("Executing batch with request_id={} changeset={} \nactions_json={}".format(request_id, path_to_database, actions_json))
            async_job = self.tableau_server.datasources.update_hyper_data(
                datasource_or_connection_item=this_datasource, request_id=request_id, actions=actions_json, payload=path_to_database
            )
            self.tableau_server.jobs.wait_for_job(async_job)

    def update_datasource_from_hyper_file(
        self,
        path_to_database: Optional[Path],
        tab_ds_name: str,
        match_columns: Union[List[List[str]], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: Optional[str] = "updated_rows",
        action: str = "UPDATE",
    ):
        """
        Update a datasource on Tableau Server with a changeset from a hyper file.

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
                        "op": "eq",
                        "source-col": match_pair[0],
                        "target-col": match_pair[1],
                    }
                )
            if len(match_conditions_args) > 1:
                match_conditions_json = {"op": "and", "args": match_conditions_args}
            else:
                match_conditions_json = match_conditions_args[0]

        if action == "UPDATE":
            actions_json = [
                {
                    "action": "update",
                    "source-schema": "Extract",
                    "source-table": changeset_table_name,
                    "target-schema": "Extract",
                    "target-table": "Extract",
                    "condition": match_conditions_json,
                },
            ]
        elif action == "DELETE":
            if path_to_database is None:
                actions_json = [
                    {
                        "action": "delete",
                        "target-schema": "Extract",
                        "target-table": "Extract",
                        "condition": match_conditions_json,
                    },
                ]
            else:
                actions_json = [
                    {
                        "action": "delete",
                        "source-schema": "Extract",
                        "source-table": changeset_table_name,
                        "target-schema": "Extract",
                        "target-table": "Extract",
                        "condition": match_conditions_json,
                    },
                ]
        elif action == "INSERT":
            actions_json = [
                {
                    "action": "insert",
                    "source-schema": "Extract",
                    "source-table": changeset_table_name,
                    "target-schema": "Extract",
                    "target-table": "Extract",
                },
            ]
        elif action == "UPSERT":
            actions_json = [
                {
                    "action": "upsert",
                    "source-schema": "Extract",
                    "source-table": changeset_table_name,
                    "target-schema": "Extract",
                    "target-table": "Extract",
                    "condition": match_conditions_json,
                },
            ]
        else:
            raise Exception("Unknown action {} specified for _update_datasource_from_hyper_file".format(action))

        this_datasource = self._get_datasource_by_name(tab_ds_name)
        lock = self._datasource_lock(tab_ds_name)
        with lock:
            request_id = str(uuid.uuid4())
            async_job = self.tableau_server.datasources.update_hyper_data(
                datasource_or_connection_item=this_datasource, request_id=request_id, actions=actions_json, payload=path_to_database
            )
            self.tableau_server.jobs.wait_for_job(async_job)

    def query_to_hyper_files(
        self,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        hyper_table_name: str = "Extract",
        multiple_hyper_files: bool = False,
    ) -> Generator[Path, None, None]:
        """
        Execute sql_query or export rows from source_table and write output to one or more hyper files.

        This base implementation uses the standard
        DBAPIv2 cursor methods and this should be overwritten if your native
        database client libraries include more efficient export etc. routines.

        Returns Iterable of Paths to hyper files

        sql_query (string): SQL to pass to the source database
        source_table (string): Source table ref ("project ID.dataset ID.table ID")
        hyper_table_name (string): Name of the target Hyper table, default=Extract
        multiple_hyper_files (boolean): When true some extractor implementations can process
         large extracts by uploading as a number of smaller hyper files.  This is not atomic
         so only used for intial full load.

        NOTES:
        - Specify either sql_query OR source_table, error if both specified
        """
        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")

        if source_table:
            sql_query = "SELECT * from {}".format(self.quoted_sql_identifier(source_table))

        cursor = self.source_database_cursor()
        logger.info(f"Execute SQL:{sql_query}")
        cursor.execute(sql_query)
        path_to_database = self.query_result_to_hyper_file(cursor=cursor, hyper_table_name=hyper_table_name)
        yield path_to_database
        return

    @log_execution_time
    def load_sample(
        self,
        tab_ds_name: str,
        source_table: Optional[str] = None,
        sql_query: Optional[str] = None,
        sample_rows: int = SAMPLE_ROWS,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> None:
        """
        Load a sample of rows from source_table to Tableau Server.

        tab_ds_name (string): Target datasource name
        source_table (string): Source table identifier
        sql_query (string): SQL to pass to the source database
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)
        sample_rows (int): How many rows to include in the sample (default=SAMPLE_ROWS)

        NOTES:
        - Specify either sql_query OR source_table, error if both specified
        """
        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")

        # loads in the first sample_rows of source_table
        # set publish_mode=TSC.Server.PublishMode.Overwrite to refresh a sample
        if sql_query:
            sql_query = "{} LIMIT {}".format(sql_query, sample_rows)
        else:
            assert source_table is not None
            sql_query = "SELECT * FROM {} LIMIT {}".format(self.quoted_sql_identifier(source_table), sample_rows)
        first_chunk = True
        for path_to_database in self.query_to_hyper_files(sql_query=sql_query):
            if first_chunk:
                self.publish_hyper_file(path_to_database, tab_ds_name, publish_mode)
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
        tab_ds_name: str,
        source_table: Optional[str] = None,
        sql_query: Optional[str] = None,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> None:
        """
        Bulk export the contents of source_table and load to Tableau Server.

        tab_ds_name (string): Target datasource name
        source_table (string): Source table identifier
        sql_query (string): SQL to pass to the source database
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)

        NOTES:
        - Specify either sql_query OR source_table, error if both specified
        """
        first_chunk = True
        for path_to_database in self.query_to_hyper_files(source_table=source_table, sql_query=sql_query, multiple_hyper_files=True):
            if first_chunk:
                self.publish_hyper_file(path_to_database, tab_ds_name, publish_mode)
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
        Append the result of sql_query to a datasource on Tableau Server.

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
        match_columns: Union[List[List[str]], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "updated_rows",
    ) -> None:
        """
        Update a datasource on Tableau Server with the changeset from sql_query.

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
            raise Exception("Must specify either match_columns OR match_conditions_json")
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
    def upsert_to_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[List[str]], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "updated_rows",
    ) -> None:
        """
        Upsert the changeset to a datasource on Tableau Server (i.e. Update rows if matched else insert)

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
            raise Exception("Must specify either match_columns OR match_conditions_json")
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
                action="UPSERT",
            )
            os.remove(path_to_database)

    @log_execution_time
    def delete_from_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[List[str]], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "deleted_rowids",
    ) -> None:
        """
        Delete rows from a datasource on Tableau Server.

        Delete rows matching the changeset from sql_query or simple delete by condition when sql_query is None

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
            raise Exception("Must specify either match_columns OR match_conditions_json")

        if (sql_query is None) and (source_table is None):
            # Conditional delete
            if match_conditions_json is None:
                raise Exception("Must specify match_conditions_json if sql_query and source_table are both None")
            self.update_datasource_from_hyper_file(
                path_to_database=None,
                tab_ds_name=tab_ds_name,
                match_columns=None,
                match_conditions_json=match_conditions_json,
                changeset_table_name=None,
                action="DELETE",
            )
        else:
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
                    action="DELETE",
                )
                os.remove(path_to_database)
