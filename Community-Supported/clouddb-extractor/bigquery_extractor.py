"""Google BigQuery implementation of Base Hyper Extractor ABC

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
import subprocess
import uuid
from pathlib import Path
import os
from tableauhyperapi import TableDefinition, Nullability, SqlType, TableName
from base_extractor import (
    BaseExtractor,
    HyperSQLTypeMappingError,
    DEFAULT_SITE_ID,
    tempfile_name,
)
from typing import Optional, Any, Dict, Generator

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.cloud import storage

logger = logging.getLogger("hyper_samples.extractor.bigquery")

bq_client = bigquery.Client()
storage_client = storage.Client()

MAX_QUERY_SIZE: int = 1024 * 1024 * 1024  # 1GB
"""
    MAX_QUERY_SIZE (int): Abort job if query returns more than specified number of bytes.
      Note: this is not enforced if extracting from a Table, only for SQL Query
"""

USE_DBAPI: bool = False
"""
    USE_DBAPI (bool): Controls if queries are executed via DBAPI Cursor (True) or
    the bigquery client native query methods.
"""

BLOBS_PER_HYPER_FILE: int = 5
"""
    BLOBS_PER_HYPER_FILE (int): Performance optimization - BigQuery splits extracts into 1G chunks
    but it is more efficient to update existing datasources in fewer, larger chunks
"""


class QuerySizeLimitError(Exception):
    pass


class BigQueryExtractor(BaseExtractor):
    """Google BigQuery Implementation of Extractor Interface

    Authentication to Tableau Server can be either by Personal Access Token or
     Username and Password.

    Constructor Args:
    - source_database_config (dict): Source database connection parameters
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
        source_database_config: Dict,
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
        self.sql_identifier_quote = """`"""
        self.staging_bucket = self.source_database_config.get("staging_bucket")

    def source_database_cursor(self) -> Any:
        """
        Returns a DBAPI Cursor to the source database
        """
        if self._source_database_connection is None:
            self._source_database_connection = dbapi.Connection(client=bq_client)

        return self._source_database_connection.cursor()

    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the corresponding Hyper column type for source_column

        source_column (obj): Instance of google.cloud.bigquery.schema.SchemaField or DBAPI Column description tuple

        Returns a tableauhyperapi.SqlType Object
        """

        type_lookup = {
            "BOOL": SqlType.bool(),
            "BYTES": SqlType.bytes(),
            "DATE": SqlType.date(),
            "DATETIME": SqlType.timestamp(),
            "INT64": SqlType.big_int(),
            "INTEGER": SqlType.int(),
            "NUMERIC": SqlType.numeric(18, 9),
            "FLOAT64": SqlType.double(),
            "STRING": SqlType.text(),
            "TIME": SqlType.time(),
            "TIMESTAMP": SqlType.timestamp_tz(),
        }

        if isinstance(source_column, bigquery.schema.SchemaField):
            source_column_type = source_column.field_type
        else:
            source_column_type = source_column.type_code

        return_sql_type = type_lookup.get(source_column_type)
        if return_sql_type is None:
            error_message = "No Hyper SqlType defined for BigQuery source type: {}".format(source_column_type)
            logger.error(error_message)
            raise HyperSQLTypeMappingError(error_message)

        logger.debug("Translated source column type {} to Hyper SqlType {}".format(source_column_type, return_sql_type))
        return return_sql_type

    def hyper_table_definition(self, source_table: Any, hyper_table_name: str = "Extract") -> TableDefinition:
        """
        Build a hyper table definition from source_table

        source_table (obj): Source table or query result description
          (Instance of google.cloud.bigquery.table.Table or dbapi.Cursor.description)
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """
        target_cols = []
        logger.info("Determine target Hyper table definition...")
        if isinstance(source_table, bigquery.table.Table):
            for source_field in source_table.schema:
                this_name = source_field.name
                this_type = self.hyper_sql_type(source_field)
                this_col = TableDefinition.Column(name=this_name, type=this_type)

                # Check for Nullability
                this_mode = source_field.mode
                if this_mode == "REPEATED":
                    raise (HyperSQLTypeMappingError("Field mode REPEATED is not implemented in Hyper"))
                if this_mode == "REQUIRED":
                    this_col = TableDefinition.Column(this_name, this_type, Nullability.NOT_NULLABLE)

                target_cols.append(this_col)
                logger.info("..Column {} - Type {}".format(this_name, this_type))
        else:
            for source_field in source_table:
                this_name = source_field.name
                this_type = self.hyper_sql_type(source_field)
                if source_field.null_ok:
                    this_col = TableDefinition.Column(name=this_name, type=this_type)
                else:
                    this_col = TableDefinition.Column(this_name, this_type, Nullability.NOT_NULLABLE)
                target_cols.append(this_col)
                logger.info("..Column {} - Type {}".format(this_name, this_type))

        target_schema = TableDefinition(table_name=TableName("Extract", hyper_table_name), columns=target_cols)
        return target_schema

    def _estimate_query_bytes(self, sql_query: str) -> int:
        """
        Dry run to estimate query result size
        """
        # Dry run to estimate result size
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dryrun_query_job = bq_client.query(sql_query, job_config=job_config)
        dryrun_bytes_estimate = dryrun_query_job.total_bytes_processed
        logger.info("This query will process {} bytes.".format(dryrun_bytes_estimate))

        if dryrun_bytes_estimate > MAX_QUERY_SIZE:
            raise QuerySizeLimitError("This query will return more than {MAX_QUERY_SIZE} bytes")
        return dryrun_bytes_estimate

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
        """
        extract_prefix = ""
        extract_destination_uri = ""
        use_extract = False
        target_table_def = None

        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")

        if sql_query:
            assert source_table is None
            self._estimate_query_bytes(sql_query)

            if USE_DBAPI:
                logging.info("Executing query using bigquery.dbapi.Cursor...")
                for path_to_database in super().query_to_hyper_files(
                    sql_query=sql_query,
                    hyper_table_name=hyper_table_name,
                ):
                    yield path_to_database
                return
            else:
                logging.info("Executing query using bigquery.table.RowIterator...")
                query_job = bq_client.query(sql_query)

                # Determine table structure
                target_table_def = self.hyper_table_definition(source_table=bq_client.get_table(query_job.destination), hyper_table_name=hyper_table_name)

                query_result_iter = bq_client.list_rows(query_job.destination)
                path_to_database = self.query_result_to_hyper_file(
                    target_table_def=target_table_def,
                    query_result_iter=query_result_iter,
                )
                yield path_to_database
                return
        else:
            logging.info("Exporting Table:{}...".format(source_table))
            use_extract = True
            extract_prefix = "staging/{}_{}".format(source_table, uuid.uuid4().hex)
            extract_destination_uri = "gs://{}/{}-*.csv.gz".format(self.staging_bucket, extract_prefix)
            source_table_ref = bq_client.get_table(source_table)
            target_table_def = self.hyper_table_definition(source_table_ref, hyper_table_name)
            extract_job_config = bigquery.ExtractJobConfig(compression="GZIP", destination_format="CSV", print_header=False)
            extract_job = bq_client.extract_table(source_table, extract_destination_uri, job_config=extract_job_config)
            extract_job.result()  # Waits for job to complete.
            logger.info("Exported {} to {}".format(source_table, extract_destination_uri))

        if use_extract:
            bucket = storage_client.bucket(self.staging_bucket)
            pending_blobs = 0
            batch_csv_filename = ""
            for this_blob in bucket.list_blobs(prefix=extract_prefix):
                logger.info("Downloading blob:{} ...".format(this_blob))
                # # TODO: better error checking here

                temp_csv_filename = tempfile_name(prefix="temp", suffix=".csv")
                temp_gzip_filename = "{}.gz".format(temp_csv_filename)
                this_blob.download_to_filename(temp_gzip_filename)
                logger.info("Unzipping {} ...".format(temp_gzip_filename))

                # Performance optimization: Concat smaller CSVs into a larger single hyper file
                if pending_blobs == 0:
                    # New batch
                    batch_csv_filename = temp_csv_filename
                    subprocess.run(
                        f"gunzip -c {temp_gzip_filename} > {batch_csv_filename}",
                        shell=True,
                        check=True,
                    )
                    os.remove(Path(temp_gzip_filename))

                else:
                    # Append to existing batch
                    subprocess.run(
                        f"gunzip -c {temp_gzip_filename} >> {batch_csv_filename}",
                        shell=True,
                        check=True,
                    )
                    os.remove(Path(temp_gzip_filename))
                pending_blobs += 1
                if pending_blobs == BLOBS_PER_HYPER_FILE:
                    path_to_database = self.csv_to_hyper_file(
                        path_to_csv=batch_csv_filename,
                        target_table_def=target_table_def,
                    )
                    pending_blobs = 0
                    os.remove(Path(batch_csv_filename))
                    yield path_to_database

            if pending_blobs:
                path_to_database = self.csv_to_hyper_file(path_to_csv=batch_csv_filename, target_table_def=target_table_def)
                os.remove(Path(batch_csv_filename))
                yield path_to_database


def main():
    pass


if __name__ == "__main__":
    main()
