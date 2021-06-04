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
)

# from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud import bigquery
from google.cloud import storage

logger = logging.getLogger("hyper_samples.extractor.bigquery")

bq_client = bigquery.Client()

MAX_QUERY_SIZE = 100 * 1024 * 1024  # 100MB
SAMPLE_ROWS = 1000


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
        tableau_hostname,
        tableau_project,
        tableau_site_id=DEFAULT_SITE_ID,
        staging_bucket=None,
        tableau_token_name=None,
        tableau_token_secret=None,
        tableau_username=None,
        tableau_password=None,
    ):
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

    def _hyper_sql_type(self, source_column):
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

    def _hyper_table_definition(self, source_table, hyper_table_name="Extract"):
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
            this_type = self._hyper_sql_type(source_field)
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

    def _query_to_hyper_files(self, sql_query, hyper_table_name="Extract"):
        """
        Executes sql_query against the source database and writes the output to one or more Hyper files
        Returns a list of output Hyper files

        sql_query -- SQL string to pass to the source database
        hyper_table_name -- Name of the target Hyper table, default=Extract
        """

        # Dry run to estimate result size
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dryrun_query_job = bq_client.query(sql_query, job_config=job_config)
        dryrun_bytes_estimate = dryrun_query_job.total_bytes_processed
        logger.info("This query will process {} bytes.".format(dryrun_bytes_estimate))

        if dryrun_bytes_estimate > MAX_QUERY_SIZE:
            raise QuerySizeLimitError(
                "This query will return than {MAX_QUERY_SIZE} bytes"
            )

        query_job = bq_client.query(sql_query)

        # Determine table structure
        query_temp_table = bq_client.get_table(query_job.destination)
        target_table_def = self._hyper_table_definition(
            query_temp_table, hyper_table_name
        )

        def query_job_iter():
            return bq_client.list_rows(query_job.destination)

        return self._query_result_to_hyper_files(query_job_iter, target_table_def)

    def load_sample(
        self,
        source_table,
        tab_ds_name,
        sample_rows=SAMPLE_ROWS,
        publish_mode=TSC.Server.PublishMode.CreateNew,
    ):
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
        output_hyper_files = self._query_to_hyper_files(sql_query, tab_ds_name)
        first_chunk = True
        for path_to_database in output_hyper_files:
            if first_chunk:
                self._publish_hyper_file(path_to_database, tab_ds_name, publish_mode)
                first_chunk = False
            else:
                self._update_datasource_from_hyper_file(
                    path_to_database=path_to_database,
                    tab_ds_name=tab_ds_name,
                    action="INSERT",
                )
            self._publish_hyper_file(path_to_database, tab_ds_name, publish_mode)
            os.remove(path_to_database)

    def _extract_to_blobs(self, source_table):
        # Returns list of blobs
        # 1: EXTRACT
        extract_job_config = bigquery.ExtractJobConfig(
            compression="GZIP", destination_format="CSV"
        )
        extract_prefix = "staging/{}_{}".format(source_table, uuid.uuid4().hex)
        extract_destination_uri = "gs://{}/{}-*.csv.gz".format(
            self.staging_bucket, extract_prefix
        )
        extract_job = bq_client.extract_table(
            source_table, extract_destination_uri, job_config=extract_job_config
        )  # API request
        extract_job.result()  # Waits for job to complete.
        logger.info("Exported {} to {}".format(source_table, extract_destination_uri))

        # 2: LIST BLOBS
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.staging_bucket)
        return bucket.list_blobs(prefix=extract_prefix)

    def _download_blob(self, blob, local_filename):
        logger.info("Downloading blob:{}".format(blob))
        # # TODO: better error checking here
        blob.download_to_filename("{}.gz".format(local_filename))
        subprocess.run(["gunzip", "{}.gz".format(local_filename)], check=True)

    def export_load(
        self, source_table, tab_ds_name, publish_mode=TSC.Server.PublishMode.CreateNew
    ):
        """
        Bulk export the contents of source_table and load to a Tableau Server

        source_table (string): Source table ref ("project ID.dataset ID.table ID")
        tab_ds_name (string): Target datasource name
        publish_mode: One of TSC.Server.[Overwrite|CreateNew] (default=CreateNew)
        """
        # Uses the bigquery export api to split large table to csv for load
        source_table_ref = bq_client.get_table(source_table)
        target_table_def = self._hyper_table_definition(source_table_ref)
        first_chunk = True
        for blob in self._extract_to_blobs(source_table):
            temp_csv_filename = tempfile_name(prefix="temp", suffix=".csv")
            self._download_blob(blob, temp_csv_filename)
            output_hyper_files = self._csv_to_hyper_files(
                temp_csv_filename, target_table_def
            )
            for path_to_database in output_hyper_files:
                if first_chunk:
                    self._publish_hyper_file(
                        path_to_database, tab_ds_name, publish_mode
                    )
                    first_chunk = False
                else:
                    self._update_datasource_from_hyper_file(
                        path_to_database=path_to_database,
                        tab_ds_name=tab_ds_name,
                        action="INSERT",
                    )
                os.remove(path_to_database)
            os.remove(Path(temp_csv_filename))

    def append_to_datasource(
        self,
        tab_ds_name,
        sql_query=None,
        source_table=None,
        changeset_table_name="new_rows",
    ):
        """
        Appends the result of sql_query to a datasource on Tableau Server

        tab_ds_name (string): Target datasource name
        sql_query (string): The query string that generates the changeset
        source_table (string): Identifier for source table containing the changeset
        changeset_table_name (string): The name of the table in the hyper file that
            contains the changeset (default="new_rows")

        NOTES:
        - Specify either sql_query OR source_table, error if both specified
        """

        if not (bool(sql_query) ^ bool(source_table)):
            raise Exception("Must specify either sql_query OR source_table")
        if sql_query:
            # Execute query to generate append changeset - slower than bulk extract
            output_hyper_files = self._query_to_hyper_files(sql_query, tab_ds_name)

            for path_to_database in output_hyper_files:
                self._update_datasource_from_hyper_file(
                    path_to_database=path_to_database,
                    tab_ds_name=tab_ds_name,
                    action="INSERT",
                )
                os.remove(path_to_database)
        if source_table:
            # Bulk extract and append from a changeset that is stored in a bq table
            source_table_ref = bq_client.get_table(source_table)
            target_table_def = self._hyper_table_definition(source_table_ref)
            for blob in self._extract_to_blobs(source_table):
                temp_csv_filename = tempfile_name(prefix="temp", suffix=".csv")
                self._download_blob(blob, temp_csv_filename)
                output_hyper_files = self._csv_to_hyper_files(
                    temp_csv_filename, target_table_def
                )
                for path_to_database in output_hyper_files:
                    self._update_datasource_from_hyper_file(
                        path_to_database=path_to_database,
                        tab_ds_name=tab_ds_name,
                        action="INSERT",
                    )
                    os.remove(path_to_database)
                os.remove(Path(temp_csv_filename))

    def update_datasource(
        self,
        tab_ds_name,
        sql_query=None,
        source_table=None,
        match_columns=None,
        match_conditions_json=None,
        changeset_table_name="updated_rows",
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

        if sql_query:
            # Execute query to generate update changeset - slower than bulk extract
            output_hyper_files = self._query_to_hyper_files(
                sql_query, changeset_table_name
            )
            for path_to_database in output_hyper_files:
                self._update_datasource_from_hyper_file(
                    path_to_database,
                    tab_ds_name,
                    match_columns,
                    match_conditions_json,
                    changeset_table_name,
                )
                os.remove(path_to_database)
        if source_table:
            # Bulk extract and update from a changeset that is stored in a bq table
            source_table_ref = bq_client.get_table(source_table)
            target_table_def = self._hyper_table_definition(
                source_table_ref, hyper_table_name=changeset_table_name
            )
            for blob in self._extract_to_blobs(source_table):
                temp_csv_filename = tempfile_name(prefix="temp", suffix=".csv")
                self._download_blob(blob, temp_csv_filename)
                output_hyper_files = self._csv_to_hyper_files(
                    temp_csv_filename, target_table_def
                )
                for path_to_database in output_hyper_files:
                    self._update_datasource_from_hyper_file(
                        path_to_database,
                        tab_ds_name,
                        match_columns,
                        match_conditions_json,
                        changeset_table_name,
                    )
                    os.remove(path_to_database)
                os.remove(Path(temp_csv_filename))

    def delete_from_datasource(
        self,
        tab_ds_name,
        sql_query=None,
        source_table=None,
        match_columns=None,
        match_conditions_json=None,
        changeset_table_name="deleted_rowids",
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
            self._update_datasource_from_hyper_file(
                None, tab_ds_name, None, match_conditions_json, None,
            )
        else:
            if sql_query:
                # Execute query to generate update changeset - slower than bulk extract
                output_hyper_files = self._query_to_hyper_files(
                    sql_query, changeset_table_name
                )
                for path_to_database in output_hyper_files:
                    self._update_datasource_from_hyper_file(
                        path_to_database,
                        tab_ds_name,
                        match_columns,
                        match_conditions_json,
                        changeset_table_name,
                        action="DELETE",
                    )
                    os.remove(path_to_database)
            if source_table:
                # Bulk extract and update from a changeset that is stored in a bq table
                source_table_ref = bq_client.get_table(source_table)
                target_table_def = self._hyper_table_definition(
                    source_table_ref, hyper_table_name=changeset_table_name
                )
                for blob in self._extract_to_blobs(source_table):
                    temp_csv_filename = tempfile_name(prefix="temp", suffix=".csv")
                    self._download_blob(blob, temp_csv_filename)
                    output_hyper_files = self._csv_to_hyper_files(
                        temp_csv_filename, target_table_def
                    )
                    for path_to_database in output_hyper_files:
                        self._update_datasource_from_hyper_file(
                            path_to_database,
                            tab_ds_name,
                            match_columns,
                            match_conditions_json,
                            changeset_table_name,
                            action="DELETE",
                        )
                        os.remove(path_to_database)
                    os.remove(Path(temp_csv_filename))


def main():
    pass


if __name__ == "__main__":
    main()
