import json

from etl_service.src.db_utils import execute_sql_script
from etl_service.src.etl import etl_process

# Load configurations
CONFIG_FILE = "/app/etl_service/config/etl_config.json"
with open(CONFIG_FILE, "r") as config_file:
    CONFIG = json.load(config_file)

DB_CONFIG = CONFIG["database"]
DATA_FILES = CONFIG["data_files"]


def main():
    """
    Main pipeline orchestrator.
    """

    # Execute the SQL DDL script
    init_script_path = (
        "/app/db/init.sql"  # Path to the init.sql file inside the container
    )
    execute_sql_script(DB_CONFIG, init_script_path)

    # Run the ETL process
    etl_process(DB_CONFIG, DATA_FILES)


if __name__ == "__main__":
    main()
