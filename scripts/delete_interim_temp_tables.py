from google.cloud import bigquery
import os

def set_google_credentials(credential_path):
    """
    Set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
    
    Parameters:
    - credential_path (str): Path to the Google Cloud Service Account key.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

def initialize_bigquery_client():
    """
    Initialize and return a BigQuery client.
    """
    return bigquery.Client()

def list_tables_in_dataset(client, project_id, dataset_id):
    """
    List all tables in a given dataset.
    
    Parameters:
    - client: BigQuery client instance.
    - project_id (str): Project ID for the Google Cloud.
    - dataset_id (str): Dataset ID to list tables from.
    
    Returns:
    - list: List of tables in the dataset.
    """
    return list(client.list_tables(f"{project_id}.{dataset_id}"))

def delete_interim_and_temp_tables(client, dataset_id, tables):
    """
    Delete tables with names containing "interim" or "temp".
    
    Parameters:
    - client: BigQuery client instance.
    - dataset_id (str): Dataset ID where the tables reside.
    - tables (list): List of tables to inspect and potentially delete.
    """
    for table in tables:
        if "interim" in table.table_id or "temp" in table.table_id:
            table_ref = client.dataset(dataset_id).table(table.table_id)
            client.delete_table(table_ref)
            print(f"Deleted table: {table.table_id}")

if __name__ == "__main__":
    gc_key = '/Users/jurisildam/my_keys/ds-on-gcp-395110-36aa06996860.json'
    set_google_credentials(gc_key)
    
    client = initialize_bigquery_client()

    project_id = 'ds-on-gcp-395110'
    dataset_id = 'numerai_data'

    tables = list_tables_in_dataset(client, project_id, dataset_id)
    delete_interim_and_temp_tables(client, dataset_id, tables)

    print("Process completed.")
