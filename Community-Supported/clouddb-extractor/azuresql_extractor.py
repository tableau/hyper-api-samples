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
from typing import Any, Optional

# Sample Code
# TODO: Add installation instructions from https://github.com/Azure-Samples/AzureSqlGettingStartedSamples/blob/master/python/Unix-based/Ubuntu_Setup.md
# TODO: Using pyodbc as this appears to be the most up to date from the following: 
# TODO: Using ODBC for now - Select one of the following DBAPI v2 compatible libraries: adodbapi, pymssql, mxODBC, pyodbc

# pyodbc source: https://pypi.org/project/pyodbc/#description
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

    #TODO: This uses an ODBC driver (which may be slow?) so consider adding a bulk export path via csv using bcp (use regular cursor with "select top 0" to determine schema)

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
        if self._source_database_connection is None:
            db_connection_args: Dict[str: str] = self.source_database_config.get("connection")
            logger.info("Connecting to source Azure SQL Database Instance...")

            '''TODO: Read password from keyvault instead of storing password in configuration
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient

                credential = DefaultAzureCredential()

                secret_client = SecretClient(vault_url="https://<your_key_vault_name>.vault.azure.net", credential=credential)

                # NOTE: please replace the ("<your-secret-name>") with the name of the secret in your vault
                secret = secret_client.get_secret("AppSecret")
                password = secret.value
            '''
            connection_str="Driver={{ODBC Driver 17 for SQL Server}};Server={host},{port};Database={database};Uid={username};Pwd={password};{connect_str_suffix}".format(
                host = db_connection_args["host"],
                port = db_connection_args["port"],
                database = db_connection_args["database"],
                username = db_connection_args["username"],
                password = db_connection_args["password"],
                connect_str_suffix = db_connection_args["connect_str_suffix"]
            )
            self._source_database_connection = pyodbc.connect(connection_str)
        
        return self._source_database_connection.cursor()

    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the corresponding Hyper column type for source_column

        source_column (obj): Instance of DBAPI Column description tuple

        Returns a tableauhyperapi.SqlType Object
        """

        #Populated this list using: "select system_type_id, name from sys.types;"
        type_lookup = {                                                                                                                                  
            #34 image                                                                                                                           
            35: SqlType.text(), # text                                                                                                                            
            36: SqlType.text(), # uniqueidentifier                                                                                                                
            40: SqlType.text(), # date                                                                                                                            
            41: SqlType.time(), # time                                                                                                                            
            42: SqlType.timestamp(), # datetime2                                                                                                                       
            # 43: datetimeoffset                                                                                                                  
            48: SqlType.small_int(), # tinyint                                                                                                                         
            52: SqlType.small_int(), # smallint                                                                                                                        
            56: SqlType.int(), # int                                                                                                                             
            58: SqlType.timestamp(), # smalldatetime                                                                                                                   
            59: SqlType.double(), # real                                                                                                                            
            60: SqlType.numeric(18,3), # money                                                                                                                           
            61: SqlType.timestamp(), # datetime                                                                                                                        
            62: SqlType.double(), # float                                                                                                                           
            #98: sql_variant                                                                                                                     
            99: SqlType.text(), # ntext                                                                                                                           
            104: SqlType.bool(), # bit                                                                                                                             
            106: SqlType.numeric(18, 9), # decimal                                                                                                                         
            108: SqlType.numeric(18, 9), # numeric                                                                                                                         
            122: SqlType.numeric(18, 9), # smallmoney                                                                                                                      
            127: SqlType.big_int(), # bigint                                                                                                                          
            #240:  hierarchyid                                                                                                                     
            #240:  geometry                                                                                                                        
            #240:  geography                                                                                                                       
            165: SqlType.text(), # varbinary                                                                                                                       
            167: SqlType.text(), # varchar                                                                                                                         
            173: SqlType.text(), # binary                                                                                                                          
            175: SqlType.text(), # char                                                                                                                            
            189: SqlType.time(), # timestamp                                                                                                                       
            231: SqlType.text(), # nvarchar                                                                                                                        
            239: SqlType.text(), # nchar                                                                                                                           
            # 241: SqlType. xml                                                                                                                             
            231: SqlType.text(), # sysname                                                                                                                         
            # 231:  AccountNumber                                                                                                                   
            # 104:  Flag                                                                                                                            
            # 231:  Name                                                                                                                            
            # 104:  NameStyle                                                                                                                       
            # 231:  OrderNumber                                                                                                                     
            231: SqlType.text(), # Phone                                              
            104: SqlType.bool(),
        }
        source_column_type = source_column[1]
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
        logger.debug(source_table)
    
        for source_column in source_table:
            this_name = source_column[0]
            this_type = self.hyper_sql_type(source_column)
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
