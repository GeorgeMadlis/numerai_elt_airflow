import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import math
from numerapi import NumerAPI

import subprocess
import os
from google.cloud import bigquery
import logging
from config import DATASET_ID
dataset_id =  DATASET_ID

def save_column_names_to_bq(client, dataset_id, table_id, columns):
    """
    Save column names into a BigQuery table.
    
    :param client: BigQuery client instance
    :param dataset_id: ID of the BQ dataset
    :param table_id: ID of the BQ table (determined by type: training, validation, or live)
    :param columns: List of column names
    """
    # Create a temporary dataframe
    df = pd.DataFrame(columns=['column_name'], data=columns)

    # Load the dataframe into BQ
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [bigquery.SchemaField("column_name", "STRING")]
    
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()


def roundup(x, dN):
    """Rounds up a number based on dN."""
    return int(math.ceil(x / (dN*1.0))) * dN

def get_list_of_directories(in_path='', prefix='data_') -> list:
    """Retrieve directories in a specified directory with a given prefix."""
    in_path = in_path or os.getcwd()
    return [d for d in os.listdir(in_path) if os.path.isdir(os.path.join(in_path, d)) and d.startswith(prefix)]

def move_data_to_gcs(in_path: str, gcs_base_path: str):
    """Moves directories from local machine to Google Cloud Storage."""
    dirs2move = get_list_of_directories(in_path)
    for local_dir in dirs2move:
        gcs_dir_path = f"{gcs_base_path}/{local_dir}"
        # will ovewrite the file in gcs_dir_path
        #subprocess.run(["gsutil", "cp", "-r", local_dir, gcs_dir_path], check=True)
        # "-n" flag ensures that it will skip the file in gcs_dir_path if it already exists
        subprocess.run(["gsutil", "cp", "-n", "-r", local_dir, gcs_dir_path], check=True)


def break_into_blocks(numbers, M):
    """Break a list into blocks of size M and return start-end pairs for each block."""
    blocks = [numbers[i:i + M] for i in range(0, len(numbers), M)]
    pairs = [(str(i), str(i + M)) for i in range(0, len(numbers), M)]
    return blocks, pairs

def download_dataset_if_not_exists(data_path):
    """Download the dataset using napi if it doesn't already exist at the given path."""
    napi = NumerAPI()
    if not os.path.exists(data_path):
        # print('downloading:', data_path)
        logging.info('Downloading: %s', data_path)
        napi.download_dataset(data_path)
    else:
        #print(f"Dataset already exists at {data_path}")
        logging.info('Dataset already exists at: %s', data_path)


def process_and_save_data(data_type: str, data_path: str):
    napi = NumerAPI()
    current_round_number = napi.get_current_round()
    print(f'current round: {current_round_number}')

    download_dataset_if_not_exists(data_path)
    
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.parquet(data_path)

    rows, cols = data.count(), len(data.columns)
    # print(f"DataFrame Rows count: {rows},  DataFrame Columns count: {cols}")
    logging.info(f"DataFrame Rows count: {rows},  DataFrame Columns count: {cols}")

    feature_cols = [c for c in data.columns if c.startswith("feature_")]

    # Adjust data filtering based on data_type
    if data_type == "validation":
        data = data.filter(data["data_type"] == "validation")

    # Determine the table name
    cols_table_name = f"{data_type}_cols"
    bq_client = bigquery.Client()
    save_column_names_to_bq(bq_client, dataset_id, cols_table_name, feature_cols)
    
    data = data.withColumn('row_id', monotonically_increasing_id())
    feature_cols_list, start_end = break_into_blocks(feature_cols, 100)

    for cols_, start_end_ in zip(feature_cols_list, start_end):
        cols = ['era'] + cols_ + ['target'] + ['row_id']
        in_path = 'data_' + start_end_[0] + '_' + start_end_[1]
        if not os.path.exists(in_path): # NB!!!
            print(in_path)
            logging.info('Saving partitioned data: %s', in_path)
            # data.select(cols).write.mode('overwrite').parquet(in_path)
            data.select(cols).repartition(3).write.mode('overwrite').parquet(in_path)
        else:
            # print('data exists, skipping', in_path)
            logging.info('Data exists, skipping: %s', in_path)


    return len(feature_cols)