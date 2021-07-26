"""
For MYSQL

"""
import logging
import os
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

import sqlalchemy
import tableauserverclient as TSC
from mysql.connector import FieldType
from tableauhyperapi import Nullability, SqlType, TableDefinition, TableName

from base_extractor import (DEFAULT_SITE_ID, SAMPLE_ROWS, BaseExtractor,
                            HyperSQLTypeMappingError, tempfile_name)

logger = logging.getLogger("hyper_samples.extractor.mySQL")


class QuerySizeLimitError(Exception):
    pass


class MySQLQueryExtractor(BaseExtractor):
    """ MySQL Implementation of Extractor Interface

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
        db_hostname: Optional[str] = None,
        db_port: Optional[str] = None,
        db_main_database: Optional[str] = None,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
    ) -> None:
        super().__init__(
            tableau_hostname=tableau_hostname,
            tableau_project=tableau_project,
            tableau_site_id=tableau_site_id,
            staging_bucket=staging_bucket,
            tableau_token_name=tableau_token_name,
            tableau_token_secret=tableau_token_secret,
            tableau_username=tableau_username,
            tableau_password=tableau_password,
        )
        self._db_hostname = db_hostname
        self._db_port =db_port
        self._db_main_database = db_main_database
        self._db_username = db_username
        self._db_password =db_password
        

    def new_source_cursor(self, sql_query) -> Any:
        """
        Returns a DBAPI Cursor to the source database

        We use SQLAlchemy to generalise as much as possible the creation of the connection and the retrieval of the cursor
        """
        sqlalchemy_database_uri =  "mysql+mysqlconnector://"+self._db_username+":"+self._db_password+"@"+self._db_hostname+":"+self._db_port+"/"+self._db_main_database
        engine = sqlalchemy.create_engine(sqlalchemy_database_uri)
        connection = engine.connect()
        rs = connection.execute(sql_query)
        return rs.cursor
        
    def hyper_sql_type(self,source_column) -> SqlType:
        """
        Finds the correct Hyper column type for source_column
        source_column (obj): Source column (Instance of )
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
            error_message = "No Hyper SqlType defined for MySQL source type: {}".format(
                source_column_type
            )
            logger.error(error_message)
            raise LookupError(error_message)

        logger.debug(
            "Translated source column type {} to Hyper SqlType {}".format(
                source_column_type, return_sql_type
            )
        )
        return return_sql_type
        
    def hyper_table_definition(
            self,
            source_table: Any, hyper_table_name: str = "Extract"
        ) -> TableDefinition:
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
                    this_col = TableDefinition.Column(
                        this_name, this_type, Nullability.NOT_NULLABLE)
                else:
                    this_col = TableDefinition.Column(
                        name=this_name, type=this_type
                    )
                target_cols.append(this_col)
                logger.info(
                    "..Column {} - Type {}".format(this_name, this_type))
            
            # create the target schema for our Hyper File
            target_schema = TableDefinition(
                table_name=TableName("Extract", hyper_table_name), columns=target_cols
            )

            return target_schema

    def query_to_hyper_files(
            self,
            sql_query: Optional[str] = None,
            source_table: Optional[str] = None,
            hyper_table_name: str = "Extract",
        ) -> Generator[Path, None, None]:
            """
            Executes sql_query or exports rows from source_table and writes output
            to one or more hyper files. 

            Returns Iterable of Paths to hyper files

            sql_query (string): SQL to pass to the source database
            source_table (string): Source table ref ("project ID.dataset ID.table ID")
            hyper_table_name (string): Name of the target Hyper table, default=Extract

            NOTES:
            - Specify either sql_query OR source_table, error if both specified
             - This method is needed here to handle the cursor exeution as well as MySQL dialect
            """
            target_table_def = None
            
            if not (bool(sql_query) ^ bool(source_table)):
                raise Exception("Must specify either sql_query OR source_table")

            if source_table:
                sql_query = "SELECT * from `{}`".format(source_table)

            # retrieve cursor from connection
            cursor = self.new_source_cursor(sql_query)
            
            # Determine table structure by passing the description of the cursor
            target_table_def = self.hyper_table_definition(
                cursor.description, hyper_table_name
            )
            path_to_database = self.query_result_to_hyper_file(
                target_table_def=target_table_def,
                cursor=cursor,
            )
            yield path_to_database
            return

def main():
    pass


if __name__ == "__main__":
    main()
