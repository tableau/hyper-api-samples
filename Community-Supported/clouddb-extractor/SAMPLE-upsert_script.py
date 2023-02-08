""" Sample script file showing how to call clouddb-extractor extractor class directly

Tableau Community supported Hyper API sample

This script uses the database_extractor utilities to:
 - Query records for the most recent UPDATE_WINDOW_DAYS days from source database and 
   write to a changeset hyper file
 - Update TARGET_DATASOURCE where primary key columns identified by MATCHING_COLUMNS 
   in changeset match corresponding columns in target datasource, else insert)

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
MATCH_COLUMNS = [
    ['PK_COLUMN_A','PK_COLUMN_A'],
    ['PK_COLUMN_B','PK_COLUMN_B'],
    ['PK_COLUMN_C','PK_COLUMN_C'],
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
    logger.info(f"Run UPSERT action")
    extractor.upsert_to_datasource(
        tab_ds_name=TARGET_DATASOURCE,
        sql_query=SOURCE_SQL,
        match_columns=MATCH_COLUMNS,
    )

if __name__ == "__main__":
    main()