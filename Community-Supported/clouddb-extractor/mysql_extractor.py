"""MySQL implementation of Base Hyper Extractor ABC

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
from typing import Any, Optional

import mysql.connector
from mysql.connector import FieldType
from tableauhyperapi import Nullability, SqlType, TableDefinition, TableName

from base_extractor import DEFAULT_SITE_ID, BaseExtractor, HyperSQLTypeMappingError

logger = logging.getLogger("hyper_samples.extractor.mySQL")


class QuerySizeLimitError(Exception):
    pass


class MySQLExtractor(BaseExtractor):
    """MySQL Implementation of Extractor Interface

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
        self.sql_identifier_quote = "`"

    def source_database_cursor(self) -> Any:
        """
        Returns a DBAPI Cursor to the source database
        """
        if self._source_database_connection is None:
            db_connection_args = self.source_database_config.get("connection")
            logger.info("Connecting to source MySQL Instance...")
            self._source_database_connection = mysql.connector.connect(**db_connection_args)

        return self._source_database_connection.cursor()

    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the corresponding Hyper column type for source_column

        source_column (obj): Instance of DBAPI Column description tuple

        Returns a tableauhyperapi.SqlType Object
        """

        type_lookup = {
            "TINY": SqlType.bool(),
            "SHORT": SqlType.bytes(),
            "DATE": SqlType.date(),
            "DATETIME": SqlType.timestamp(),
            "INT24": SqlType.big_int(),
            "LONGLONG": SqlType.big_int(),
            "INTEGER": SqlType.int(),
            "DECIMAL": SqlType.numeric(18, 9),
            "DOUBLE": SqlType.double(),
            "FLOAT": SqlType.double(),
            "VAR_STRING": SqlType.text(),
            "TIME": SqlType.time(),
            "TIMESTAMP": SqlType.timestamp_tz(),
        }
        source_column_type = source_column
        return_sql_type = type_lookup.get(source_column_type)

        if return_sql_type is None:
            error_message = "No Hyper SqlType defined for MySQL source type: {}".format(source_column_type)
            logger.error(error_message)
            raise HyperSQLTypeMappingError(error_message)

        logger.debug("Translated source column type {} to Hyper SqlType {}".format(source_column_type, return_sql_type))
        return return_sql_type

    def hyper_table_definition(self, source_table: Any, hyper_table_name: str = "Extract") -> TableDefinition:
        """
        Build a hyper table definition from source_schema

        source_table (obj): Source table (Instance of DBAPI Cursor Description)
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """
        # logger.debug(
        #     "Building Hyper TableDefinition for table {}".format(source_table.dtypes)
        # )
        target_cols = []

        for source_field in source_table:
            this_name = source_field[0]
            this_type = self.hyper_sql_type(FieldType.get_info(source_field[1]))
            if source_field[6]:
                this_col = TableDefinition.Column(this_name, this_type, Nullability.NOT_NULLABLE)
            else:
                this_col = TableDefinition.Column(name=this_name, type=this_type)
            target_cols.append(this_col)
            logger.info("..Column {} - Type {}".format(this_name, this_type))

        # create the target schema for our Hyper File
        target_schema = TableDefinition(table_name=TableName("Extract", hyper_table_name), columns=target_cols)
        return target_schema


def main():
    pass


if __name__ == "__main__":
    main()
