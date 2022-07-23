"""Azure SQL Database implementation of Base Hyper Extractor ABC

Tableau Community supported Hyper API sample

-----------------------------------------------------------------------------

This file is the copyrighted property of Tableau Software and is protected
by registered patents and other applicable U.S. and international laws and
regulations.

You may adapt this file and modify it to fit into your context and use it
as a template to start your own projects.

-----------------------------------------------------------------------------
"""
import logging
from typing import Any, Optional, Dict

import pyodbc
from tableauhyperapi import Nullability, SqlType, TableDefinition, TableName

from base_extractor import DEFAULT_SITE_ID, BaseExtractor, HyperSQLTypeMappingError

logger = logging.getLogger("hyper_samples.extractor.mySQL")

class QuerySizeLimitError(Exception):
    pass

class AzureSQLExtractor(BaseExtractor):
    """Azure SQL Database Implementation of Extractor Interface

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
        source_database_config: dict,
        tableau_hostname: str,
        tableau_project: str,
        tableau_site_id: str = DEFAULT_SITE_ID,
        tableau_token_name: Optional[str] = None,
        tableau_token_secret: Optional[str] = None,
        tableau_username: Optional[str] = None,
        tableau_password: Optional[str] = None,
    ) -> None:
        super().__init__(
            source_database_config=source_database_config,
            tableau_hostname=tableau_hostname,
            tableau_project=tableau_project,
            tableau_site_id=tableau_site_id,
            tableau_token_name=tableau_token_name,
            tableau_token_secret=tableau_token_secret,
            tableau_username=tableau_username,
            tableau_password=tableau_password,
        )
        self._source_database_connection = None
        self.sql_identifier_quote = ""

    def source_database_cursor(self) -> Any:
        """
        Returns a DBAPI Cursor to the source database
        """
        assert self.source_database_config is not None
        if self._source_database_connection is None:
            logger.info("Connecting to source Azure SQL Database Instance...")

            db_connection_args = self.source_database_config.get("connection")
            assert type(db_connection_args) is dict

            key_vault_url = db_connection_args.get("key_vault_url")
            secret_name = db_connection_args.get("secret_name")
            if key_vault_url is not None:
                #Recommended: Read password from keyvault
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient
                credential = DefaultAzureCredential()
                secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
                secret = secret_client.get_secret(secret_name)
                this_password = secret.value
            else:
                #Password is stored as plain text
                this_password = db_connection_args["password"]

            connection_str = "Driver={{ODBC Driver 17 for SQL Server}};Server={host},{port};Database={database};Uid={username};Pwd={password};{connect_str_suffix}".format(
                host=db_connection_args["host"],
                port=db_connection_args["port"],
                database=db_connection_args["database"],
                username=db_connection_args["username"],
                password=this_password,
                connect_str_suffix=db_connection_args["connect_str_suffix"]
            )
            self._source_database_connection = pyodbc.connect(connection_str)

        return self._source_database_connection.cursor()

    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the corresponding Hyper column type for source_column

        source_column (obj): Instance of DBAPI Column description tuple

        Returns a tableauhyperapi.SqlType Object
        """

        """
        Note: pyodbc returns a description which contains a tuple per column with the following fields
            0 column name (or alias, if specified in the SQL)
            1 type object
            2 display size (pyodbc does not set this value)
            3 internal size (in bytes)
            4 precision
            5 scale
            6 nullable (True/False)
            e.g. ('schema_id', <class 'int'>, None, 10, 10, 0, False)
        The mapping from SQL types to python types is defined in pyodbx.SQL_data_type_dict
        """
        source_column_type = source_column[1].__name__
        source_column_precision = source_column[4]
        source_column_scale = source_column[5]

        type_lookup = {
            "str": SqlType.text,
            "unicode": SqlType.text,
            "bytearray": SqlType.text,
            "bool": SqlType.bool,

            "int": SqlType.int,
            "float": SqlType.double,
            "long": SqlType.big_int,
            #"Decimal": SqlType.numeric,

            "date": SqlType.date,
            "time": SqlType.time,
            "datetime": SqlType.timestamp_tz,
        }

        if source_column_type == 'Decimal':
            return_sql_type = SqlType.numeric(source_column_precision, source_column_scale)
        else:
            return_sql_type = type_lookup.get(source_column_type)

            if return_sql_type is None:
                error_message = "No Hyper SqlType defined for MySQL source type: {}".format(source_column_type)
                logger.error(error_message)
                raise HyperSQLTypeMappingError(error_message)

            return_sql_type = return_sql_type()

        logger.debug("Translated source column type {} to Hyper SqlType {}".format(source_column_type, return_sql_type))
        return return_sql_type

    def hyper_table_definition(self, source_table: Any, hyper_table_name: str = "Extract") -> TableDefinition:
        """
        Build a hyper table definition from source_schema

        source_table (obj): Source table (Instance of DBAPI Cursor Description)
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """
        logger.debug(
            "Building Hyper TableDefinition for table {}".format(source_table)
        )
        target_cols = []
        for source_column in source_table:
            this_name = source_column[0]
            this_type = self.hyper_sql_type(source_column)
            if source_column[6] == False:
                this_col = TableDefinition.Column(this_name, this_type, Nullability.NOT_NULLABLE)
            else:
                this_col = TableDefinition.Column(name=this_name, type=this_type)
            target_cols.append(this_col)
            logger.info("..Column {} - Type {}".format(this_name, this_type))

        # Create the target schema for our Hyper File
        target_schema = TableDefinition(table_name=TableName("Extract", hyper_table_name), columns=target_cols)
        return target_schema

    def load_sample(
        self,
        tab_ds_name: str,
        source_table: Optional[str] = None,
        sql_query: Optional[str] = None,
        sample_rows: int = 0,
        publish_mode: Any = None,
    ) -> None:
        error_message = "METHOD load_sample is not implemented for SQL Server (Transact-SQL does not support the LIMIT statement)"
        logger.error(error_message)
        raise NotImplementedError(error_message)

def main():
    pass


if __name__ == "__main__":
    main()
