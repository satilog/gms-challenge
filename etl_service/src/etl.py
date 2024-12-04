import glob
import json
import os

import pandas as pd
from psycopg2 import connect

from etl_service.src.clean_utils import (
    clean_gender,
    create_prefix_matcher,
    parse_mixed_dates,
    preprocess_dates,
)


# Helper to batch insert data
def load_data_to_database(df, table_name, conn, unique_key=None):
    """
    Loads a Pandas DataFrame into a PostgreSQL table, avoiding duplicates.
    """
    df = df.replace({pd.NA: None})  # Replace pd.NA with None
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            keys = ", ".join(row.index)
            placeholders = ", ".join(f"%s" for _ in row.index)
            updates = ", ".join(f"{col} = EXCLUDED.{col}" for col in row.index)
            query = f"""
                INSERT INTO {table_name} ({keys}) VALUES ({placeholders})
                ON CONFLICT ({unique_key}) DO UPDATE SET {updates};
            """
            cursor.execute(query, tuple(row))
    conn.commit()
    print(f"Data loaded into {table_name}.")


def set_foreign_keys_to_null(df, parent_table, foreign_key_column, conn):
    """
    Sets foreign keys to NULL in a DataFrame if the referenced record does not exist in the parent table.

    :param df: DataFrame containing the foreign key column.
    :param parent_table: Parent table to check for existing keys.
    :param foreign_key_column: Column in the DataFrame that is a foreign key.
    :param conn: Active database connection.
    """
    with conn.cursor() as cursor:
        parent_ids_query = f"SELECT DISTINCT ClaimID FROM {parent_table}"
        cursor.execute(parent_ids_query)
        parent_ids = {row[0] for row in cursor.fetchall()}

    df[foreign_key_column] = df[foreign_key_column].apply(
        lambda x: x if x in parent_ids else None
    )


# ETL Pipeline
def etl_process(db_config, data_files):
    """
    Main ETL pipeline to process and load data into the database.
    """
    # Connect to database
    conn = None
    try:
        # Claims Master
        conn = connect(**db_config)
        claims_files = glob.glob(data_files["claims_master"])
        claims_master_combined = pd.concat(
            [pd.read_csv(file) for file in claims_files], ignore_index=True
        )

        # Cleaning Claims Master
        claims_master_combined["ClaimDate"] = (
            claims_master_combined["ClaimDate"]
            .apply(preprocess_dates)
            .apply(parse_mixed_dates)
        )
        claims_master_combined["PolicyStartDate"] = (
            claims_master_combined["PolicyStartDate"]
            .apply(preprocess_dates)
            .apply(parse_mixed_dates)
        )
        claims_master_combined["PolicyEndDate"] = (
            claims_master_combined["PolicyEndDate"]
            .apply(preprocess_dates)
            .apply(parse_mixed_dates)
        )
        claims_master_combined["ClaimAmount"] = claims_master_combined[
            "ClaimAmount"
        ].round(2)

        clean_region = create_prefix_matcher(
            ["Europe", "Australia", "Asia", "North America"]
        )
        claims_master_combined["Region"] = claims_master_combined["Region"].apply(
            clean_region
        )
        claims_master_combined["CustomerGender"] = claims_master_combined[
            "CustomerGender"
        ].apply(clean_gender)

        load_data_to_database(
            claims_master_combined, "claims_master", conn, unique_key="ClaimID"
        )

        # Claims Payment
        claims_payment = pd.read_csv(data_files["claims_payment"])
        claims_payment["PaymentDate"] = (
            claims_payment["PaymentDate"]
            .apply(preprocess_dates)
            .apply(parse_mixed_dates)
        )
        claims_payment["PaymentAmount"] = claims_payment["PaymentAmount"].round(2)

        valid_payment_statuses = ["Paid", "Pending", "Rejected"]
        clean_payment_status = create_prefix_matcher(valid_payment_statuses)
        claims_payment["PaymentStatus"] = claims_payment["PaymentStatus"].apply(
            clean_payment_status
        )

        # Set foreign keys to NULL if parent records are missing
        # set_foreign_keys_to_null(claims_payment, "claims_master", "ClaimID", conn)

        load_data_to_database(
            claims_payment, "claims_payment", conn, unique_key="PaymentID"
        )

        # Policy Premium
        policy_premium = pd.read_csv(data_files["policy_premium"])
        policy_premium["LastPaymentDate"] = (
            policy_premium["LastPaymentDate"]
            .apply(preprocess_dates)
            .apply(parse_mixed_dates)
        )
        policy_premium["PremiumAmount"] = policy_premium["PremiumAmount"].round(2)

        load_data_to_database(
            policy_premium, "policy_premium", conn, unique_key="PolicyID"
        )

        # Claim Details and Procedure Codes
        json_files = glob.glob(data_files["json_directory"])
        claim_details_data = []
        procedure_codes_data = []

        for file_path in json_files:
            with open(file_path, "r") as json_file:
                data = json.load(json_file)
                # print(data)
                claim_id = data["ClaimID"]
                details = data["Details"]

                claim_details_data.append(
                    {
                        "ClaimID": claim_id,
                        "Diagnosis": details.get("Diagnosis", ""),
                        "ServiceDate": parse_mixed_dates(
                            preprocess_dates(details.get("ServiceDate", ""))
                        ),
                        "Provider": details.get("Provider", ""),
                        "Country": "USA",  # details.get("Country", ""),
                    }
                )

                for code in details.get("ProcedureCodes", []):
                    procedure_codes_data.append(
                        {
                            "ClaimID": claim_id,
                            "ProcedureCode": code,
                        }
                    )

        claim_details_df = pd.DataFrame(claim_details_data)
        procedure_codes_df = pd.DataFrame(procedure_codes_data)

        # Set foreign keys to NULL if parent records are missing
        # set_foreign_keys_to_null(claim_details_df, "claims_master", "ClaimID", conn)
        # set_foreign_keys_to_null(procedure_codes_df, "claim_details", "ClaimID", conn)

        load_data_to_database(
            claim_details_df, "claim_details", conn, unique_key="ClaimID"
        )
        load_data_to_database(
            procedure_codes_df, "procedure_codes", conn, unique_key="CodeID"
        )

    except Exception as e:
        print(f"Error during ETL process: {e}")
    finally:
        if conn:
            conn.close()


# Execute ETL
if __name__ == "__main__":
    CONFIG_FILE = "/app/etl_service/config/etl_config.json"
    with open(CONFIG_FILE, "r") as config_file:
        CONFIG = json.load(config_file)

    etl_process(CONFIG["database"], CONFIG["data_files"])
