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
import os
import uuid
from pathlib import Path
import tableauserverclient as TSC
from tableauhyperapi import TableDefinition, Nullability, SqlType, TableName
from base_extractor import (
    BaseExtractor,
    HyperSQLTypeMappingError,
    DEFAULT_SITE_ID,
    tempfile_name,
    SAMPLE_ROWS,
)
from typing import Dict, Optional, Any, Union, List, Generator

# from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud import bigquery
from google.cloud import storage

logger = logging.getLogger("hyper_samples.extractor.bigquery")

bq_client = bigquery.Client()

MAX_QUERY_SIZE: int = 1024 * 1024 * 1024  # 1GB
"""
    MAX_QUERY_SIZE (int): Abort job if query returns more than specified number of bytes.
      Note: this is not enforced if extracting from a Table, only for SQL Query
"""

# TODO: Implement query via cloud storage once parquet support is out of beta.
# No straight-forward way of determining schema today when using EXPORT DATA
#  QUERY_TO_CLOUDSTORAGE_THRESHOLD: int = 50 * 1024 * 1024  # 50MB
# """
#     QUERY_TO_CLOUDSTORAGE_THRESHOLD (int): Create extract via Cloud Storage if query result
#     exceeds specified number of bytes
# """

BLOBS_PER_HYPER_FILE: int = 5
"""
    BLOBS_PER_HYPER_FILE (int): Performance optimization - BigQuery splits extracts into 1G chunks
    but it is more efficient to update existing datasources in fewer, larger chunks
"""


class QuerySizeLimitError(Exception):
    pass


class BigQueryExtractor(BaseExtractor):
    """ Google BigQuery Implementation of Extractor Interface

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

    def hyper_sql_type(self, source_column: Any) -> SqlType:
        """
        Finds the correct Hyper column type for source_column

        source_column (obj): Source column (Instance of google.cloud.bigquery.schema.SchemaField)

        Returns a tableauhyperapi.SqlType Object
        """

        source_column_type = source_column.field_type
        return_sql_type = {
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
        }.get(source_column_type)

        if return_sql_type is None:
            error_message = "No Hyper SqlType defined for BigQuery source type: {}".format(
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
        self, source_table: Any, hyper_table_name: str = "Extract"
    ) -> TableDefinition:
        """
        Build a hyper table definition from source_schema

        source_table (obj): Source table (Instance of google.cloud.bigquery.table.Table)
        hyper_table_name (string): Name of the target Hyper table, default="Extract"

        Returns a tableauhyperapi.TableDefinition Object
        """

        logger.debug(
            "Building Hyper TableDefinition for table {}".format(source_table.reference)
        )
        target_cols = []
        for source_field in source_table.schema:
            this_name = source_field.name
            this_type = self.hyper_sql_type(source_field)
            this_col = TableDefinition.Column(name=this_name, type=this_type)

            # Check for Nullability
            this_mode = source_field.mode
            if this_mode == "REPEATED":
                raise (
                    HyperSQLTypeMappingError(
                        "Field mode REPEATED is not implemented in Hyper"
                    )
                )
            if this_mode == "REQUIRED":
                this_col = TableDefinition.Column(
                    this_name, this_type, Nullability.NOT_NULLABLE
                )

            target_cols.append(this_col)
            logger.debug("..Column {} - Type {}".format(this_name, this_type))

        target_schema = TableDefinition(
            table_name=TableName("Extract", hyper_table_name), columns=target_cols
        )

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
            raise QuerySizeLimitError(
                "This query will return more than {MAX_QUERY_SIZE} bytes"
            )
        return dryrun_bytes_estimate

    def query_to_hyper_files(
        self,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        hyper_table_name: str = "Extract",
    ) -> Generator[Path, None, None]:
        """
        Executes sql_query or exports rows from source_table and writes output
        to one or more hype files.

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
            dryrun_bytes_estimate = self._estimate_query_bytes(sql_query)

            # if dryrun_bytes_estimate < QUERY_TO_CLOUDSTORAGE_THRESHOLD:
            logging.info("Executing query using bigquery.table.RowIterator...")
            # 1: Smaller resultset - keep it simple and query direct via paging Api
            query_job = bq_client.query(sql_query)

            # Determine table structure
            query_temp_table = bq_client.get_table(query_job.destination)
            target_table_def = self.hyper_table_definition(
                query_temp_table, hyper_table_name
            )

            query_job_iter = bq_client.list_rows(query_job.destination)
            path_to_database = self.query_result_to_hyper_file(
                query_job_iter, target_table_def
            )
            yield path_to_database
            return
            # else:
            #     logging.info("Executing query using EXPORT DATA via cloud storage...")
            #     # 2: Larger resultset - expport via cloud storage
            #     use_extract = True
            #     extract_prefix = "staging/{}_{}".format("SQL", uuid.uuid4().hex)
            #     extract_destination_uri = "gs://{}/{}-*.csv.gz".format(
            #         self.staging_bucket, extract_prefix
            #     )
            #     sql_query = "EXPORT DATA OPTIONS( uri={}, format='CSV', overwrite=true, header=false, field_delimiter=',') AS {}".format(
            #         extract_destination_uri, sql_query
            #     )
            #     query_job = bq_client.query(sql_query)
            #
            #     # Determine table structure
            #     query_temp_table = bq_client.get_table(query_job.destination)
            #     target_table_def = self.hyper_table_definition(
            #         query_temp_table, hyper_table_name
            #     )
        else:
            logging.info("Exporting Table:{}...".format(source_table))
            use_extract = True
            extract_prefix = "staging/{}_{}".format(source_table, uuid.uuid4().hex)
            extract_destination_uri = "gs://{}/{}-*.csv.gz".format(
                self.staging_bucket, extract_prefix
            )
            source_table_ref = bq_client.get_table(source_table)
            target_table_def = self.hyper_table_definition(
                source_table_ref, hyper_table_name
            )
            extract_job_config = bigquery.ExtractJobConfig(
                compression="GZIP", destination_format="CSV", print_header=False
            )
            extract_job = bq_client.extract_table(
                source_table, extract_destination_uri, job_config=extract_job_config
            )
            extract_job.result()  # Waits for job to complete.
            logger.info(
                "Exported {} to {}".format(source_table, extract_destination_uri)
            )

        if use_extract:
            storage_client = storage.Client()
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
                path_to_database = self.csv_to_hyper_file(
                    path_to_csv=batch_csv_filename, target_table_def=target_table_def
                )
                os.remove(Path(batch_csv_filename))
                yield path_to_database

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
        sql_query = "SELECT * FROM `{}` LIMIT {}".format(source_table, sample_rows)
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

    def export_load(
        self,
        source_table: str,
        tab_ds_name: str,
        publish_mode: TSC.Server.PublishMode = TSC.Server.PublishMode.CreateNew,
    ) -> None:
        """
        Bulk export the contents of source_table and load to Tableau Server

        source_table (string): Source table identifier
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)
        """

        source_table_ref = bq_client.get_table(source_table)
        target_table_def = self.hyper_table_definition(source_table_ref)
        first_chunk = True
        for path_to_database in self.query_to_hyper_files(source_table=source_table):
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

    def update_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[str], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "updated_rows",
    ):
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

    def delete_from_datasource(
        self,
        tab_ds_name: str,
        sql_query: Optional[str] = None,
        source_table: Optional[str] = None,
        match_columns: Union[List[str], None] = None,
        match_conditions_json: Optional[object] = None,
        changeset_table_name: str = "deleted_rowids",
    ):
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
            (e.g. json_request="condition": { "op": "<", "target-col": "col1", "const": {"type": "datetime", "v": "2020-06-00"}})
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


def main():
    pass


if __name__ == "__main__":
    main()
