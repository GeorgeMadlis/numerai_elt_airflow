from google.cloud import bigquery
import uuid
import os
from data_utils import process_and_save_data, roundup, move_data_to_gcs
from google.cloud import storage
from gcp_utils import set_google_credentials


class BQPipeline:
    def __init__(self, project_id, dataset_id, bucket_name, 
                 credential_path, N, prefix='training_data', dN = 100):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.dN = dN
        self.N = N
        set_google_credentials(credential_path)

        self.client = bigquery.Client()
        self.storage_client = storage.Client()

    def _load_parquet_from_gcs_to_bq(self, table_id, gcs_path):
        """Upload parquet data from GCS to BQ."""
        dataset_ref = self.client.dataset(self.dataset_id)
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, autodetect=True)
        load_job = self.client.load_table_from_uri(gcs_path, dataset_ref.table(table_id), job_config=job_config)
        load_job.result()

    def merge_and_create_final_bq_table(self, main_table_id):
        """Merge data from GCS and create final BQ table."""
        bucket = self.storage_client.bucket(self.bucket_name)
        interim_tables = []

        for N1, N2 in zip(range(0, self.N, self.dN), range(self.dN, self.N + self.dN, self.dN)):
            blobs_list = list(bucket.list_blobs(prefix=f"{self.prefix}/data_{N1}_{N2}/", delimiter=None))
            if not blobs_list:
                print('empty', N1, N2)
                continue
            print(N1, N2)
            parquet_files = [blob.name for blob in blobs_list if blob.name.endswith(".parquet")]
            table_names = []

            for idx, file in enumerate(parquet_files):
                unique_id = str(uuid.uuid4()).replace('-', '')
                table_name = f"temp_table_{N1}_{N2}_{idx}_{unique_id}"
                self._load_parquet_from_gcs_to_bq(table_name, f"gs://{self.bucket_name}/{file}")
                table_names.append(table_name)

            interim_table_id = f"interim_{N1}_{N2}"
            interim_tables.append(interim_table_id)
            union_queries = [f"SELECT * FROM `{self.dataset_id}.{table}`" for table in table_names]
            merge_sql = f"CREATE OR REPLACE TABLE `{self.dataset_id}.{interim_table_id}` AS " + " UNION ALL ".join(union_queries)
            self.client.query(merge_sql).result()

        base_table = interim_tables.pop(0)
        merge_sql = f"CREATE OR REPLACE TABLE `{self.dataset_id}.{main_table_id}` AS SELECT * FROM `{self.dataset_id}.{base_table}` "
        for interim_table_id in interim_tables:
            merge_sql += f"LEFT JOIN `{self.dataset_id}.{interim_table_id}` USING(row_id, era, target) "
        self.client.query(merge_sql).result()



