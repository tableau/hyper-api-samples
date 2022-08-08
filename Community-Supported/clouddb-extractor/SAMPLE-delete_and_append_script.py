""" Sample script file shows how to execute multiple commands in a single batch operation

Tableau Community supported Hyper API sample

This script uses the database_extractor utilities to:
 - 1: Query records for the most recent UPDATE_WINDOW_DAYS days from source database and 
   write to a changeset hyper file
 - 2: Delete the most recent N days from target datasource
 - 3: Append the changeset to target datasource

Update "#Batch Configuration" and "#Query and action settings" below and 
 the config.yml file with your site configuration before using.
 
-----------------------------------------------------------------------------

This file is the copyrighted property of Tableau Software and is protected
by registered patents and other applicable U.S. and international laws and
regulations.

You may adapt this file and modify it to fit into your context and use it
as a template to start your own projects.

-----------------------------------------------------------------------------
"""

from azuresql_extractor import AzureSQLExtractor
import os
import yaml
import logging
from datetime import datetime, timedelta


#Batch Configuration
CONFIG_FILE = "config.yml"
SOURCE_TABLE = "MY_SOURCE_TABLE"
TARGET_DATASOURCE = "MY_TARGET_DATASOURCE"
UPDATE_WINDOW_DAYS = 7
TARGET_DATE_COLUMN = "LOAD_DATE"

#Load Defaults from CONFIG_FILE
config = yaml.safe_load(open(CONFIG_FILE))
tableau_env = config.get("tableau_env")
db_env = config.get("azuresql")

#Calculated datetime fields
target_date_start = datetime.today() - timedelta(days=UPDATE_WINDOW_DAYS)

#Query and Action Settings
SOURCE_SQL = f"SELECT * from {SOURCE_TABLE} WHERE {TARGET_DATE_COLUMN} >= '{target_date_start.strftime('%Y-%m-%d')}'"
ACTION_BATCH_JSON = [
    {
        "action": "delete",
        "target-schema": "Extract",
        "target-table": "Extract",
        "condition": {
            "op": "gte",
            "target-col": TARGET_DATE_COLUMN,
            "const": {"type": "datetime", "v": target_date_start.astimezone().isoformat()}  # datetime is in ISO 8601 format e.g. 2007-12-03T10:15:30Z (for UTC time) or 2007-12-03T10:15:30+0100 (for a timezone UTC+1:00)
        },
    },
    {
        "action": "insert",
        "source-schema": "Extract",
        "source-table": "Extract",
        "target-schema": "Extract",
        "target-table": "Extract",
    },
]

# Initialize logging
consoleHandler = logging.StreamHandler()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s %(funcName)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), consoleHandler],
)
consoleHandler.setLevel(logging.INFO)

logger = logging.getLogger("hyper_samples.batch_script")

def main():
    logger.info(f"Load private access token")
    with open(tableau_env.get("token_secretfile"), "r") as myfile:
        tableau_token_secret = myfile.read().strip()

    logger.info(f"Initialize extractor")
    extractor = AzureSQLExtractor(
        source_database_config=db_env,
        tableau_hostname=tableau_env.get("server_address"),
        tableau_project=tableau_env.get("project"),
        tableau_site_id=tableau_env.get("site_id"),
        tableau_token_name=tableau_env.get("token_name"),
        tableau_token_secret=tableau_token_secret,
    )

    # Run Job
    logger.info(f"Connecting to source database")
    cursor = extractor.source_database_cursor()

    logger.info(f"Execute SQL:{SOURCE_SQL}")
    cursor.execute(SOURCE_SQL)

    logger.info(f"Extract to local changeset hyper file")
    path_to_database = extractor.query_result_to_hyper_file(cursor=cursor, hyper_table_name="Extract")

    logger.info(f"Transfer changeset hyper file {path_to_database} and execute batch {ACTION_BATCH_JSON}")
    extractor.patch_datasource(
        tab_ds_name=TARGET_DATASOURCE,
        actions_json=ACTION_BATCH_JSON,
        path_to_database=path_to_database,
    )

    logger.info(f"Remove local changeset hyper file {path_to_database}")
    os.remove(path_to_database)

if __name__ == "__main__":
    main()