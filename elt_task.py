
from config import PROJECT_ID, DATASET_ID, BUCKET_NAME, GC_KEY, DN
from config import DATA_TYPE
from data_utils import process_and_save_data, roundup, move_data_to_gcs
from bigquery_pipeline import BQPipeline
import os
from gcp_utils import set_google_credentials
import logging

bucket_name = BUCKET_NAME
dN = DN
gc_key = GC_KEY
project_id = PROJECT_ID
dataset_id = DATASET_ID
data_type = DATA_TYPE


def run_elt(data_type: str):
    try:

        # Set Google credentials
        set_google_credentials(gc_key)
        
        # Define the data path and table id based on the data_type
        data_path = f'v4.1/{data_type}.parquet'
        main_table_id = f'{data_type}_data'
        
        # Define the full path to the GCS bucket
        gcs_bucket_fullpath = f'gs://{bucket_name}/{main_table_id}'
        
        # Step 1: download numerai parquet files, break it into blocks and save them locally
        Nmax = process_and_save_data(main_table_id, data_path)
        N = roundup(Nmax, dN)

        # Step 2: Move local data directories to GCS
        move_data_to_gcs(os.getcwd(), gcs_bucket_fullpath)

        # Step 3: Launch BigQuery pipeline to merge data blocks and create a single table
        pipeline = BQPipeline(project_id, dataset_id, bucket_name, gc_key, N, main_table_id, dN)
        pipeline.merge_and_create_final_bq_table(main_table_id)    
    except Exception as e:
        logging.error(f"Failed to run ELT process. Error: {str(e)}")