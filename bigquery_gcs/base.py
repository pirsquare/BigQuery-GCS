import boto
from boto.gs.bucket import Bucket
from boto.gs.key import Key
from bigquery.client import get_client, JOB_WRITE_TRUNCATE, JOB_CREATE_IF_NEEDED
from bigquery.errors import BigQueryTimeoutException
from . import utils
from .exceptions import BadConfigurationException


CONTENT_TYPE_CSV = 'text/csv'


class Exporter(object):

    def __init__(self, config, *args, **kwargs):
        """
        Parameters
        ----------
        config: dict
            Dict containing all the configuration details
        """

        if 'GCS_ACCESS_KEY' not in config:
            raise BadConfigurationException("GCS_ACCESS_KEY needs to be specify in config")

        if 'GCS_SECRET_KEY' not in config:
            raise BadConfigurationException("GCS_SECRET_KEY needs to be specify in config")

        if 'GCS_BUCKET_NAME' not in config:
            raise BadConfigurationException("GCS_BUCKET_NAME needs to be specify in config")

        if 'BQ_PROJECT_ID' not in config:
            raise BadConfigurationException("BQ_PROJECT_ID needs to be specify in config")

        if 'BQ_SERVICE_ACCOUNT' not in config:
            raise BadConfigurationException("BQ_SERVICE_ACCOUNT needs to be specify in config")

        if 'BQ_PRIVATE_KEY_PATH' not in config:
            raise BadConfigurationException("BQ_PRIVATE_KEY_PATH needs to be specify in config")

        if 'BQ_DEFAULT_QUERY_TIMEOUT' not in config:
            raise BadConfigurationException("BQ_DEFAULT_QUERY_TIMEOUT needs to be specify in config")

        if 'BQ_DEFAULT_EXPORT_TIMEOUT' not in config:
            raise BadConfigurationException("BQ_DEFAULT_EXPORT_TIMEOUT needs to be specify in config")

        self.gcs_access_key = config['GCS_ACCESS_KEY']
        self.gcs_secret_key = config['GCS_SECRET_KEY']
        self.gcs_bucket_name = config['GCS_BUCKET_NAME']
        self.bq_project_id = config['BQ_PROJECT_ID']
        self.bq_service_account = config['BQ_SERVICE_ACCOUNT']
        self.bq_private_key_path = config['BQ_PRIVATE_KEY_PATH']
        self.bq_default_query_timeout = config['BQ_DEFAULT_QUERY_TIMEOUT']
        self.bq_default_export_timeout = config['BQ_DEFAULT_EXPORT_TIMEOUT']

    @property
    def bq_private_key(self):
        return self._get_file(self.bq_private_key_path)

    @property
    def gcs_client(self):
        if not hasattr(self, "_gcs_client"):
            self._gcs_client = boto.connect_gs(self.gcs_access_key, self.gcs_secret_key)
        return self._gcs_client

    @property
    def bq_client(self):
        if not hasattr(self, "_bq_client"):
            self._bq_client = get_client(self.bq_project_id, service_account=self.bq_service_account,
                                         private_key=self.bq_private_key, readonly=False)
        return self._bq_client

    @property
    def gcs_bucket(self):
        if not hasattr(self, "_gcs_bucket"):
            self._gcs_bucket = Bucket(self.gcs_client, self.gcs_bucket_name)
        return self._gcs_bucket

    @staticmethod
    def _get_file(filename):
        with open(filename) as f:
            return f.read()

    def _dataset_exist(self, dataset):
        """Given dataset name, check if dataset exist"""
        all_datasets = self.bq_client.get_datasets()
        if all_datasets:
            for row in all_datasets:
                if row["datasetReference"]["datasetId"] == dataset:
                    return True
        return False

    def _get_or_create_dataset(self, dataset):
        if not self._dataset_exist(dataset):
            self.bq_client.create_dataset(dataset)

    def _delete_table(self, dataset, table):
        self.bq_client.delete_table(dataset, table)

    def _write_to_table(self, dataset, table, query, write_disposition=JOB_WRITE_TRUNCATE, query_timeout=None):
        timeout = query_timeout or self.bq_default_query_timeout

        try:
            job = self.bq_client.write_to_table(query=query,
                                                dataset=dataset,
                                                table=table,
                                                create_disposition=JOB_CREATE_IF_NEEDED,
                                                write_disposition=write_disposition,
                                                allow_large_results=True)

            job_resource = self.bq_client.wait_for_job(job, timeout=timeout)

        # re-raise exceptions with details if job resource is still running after timeout
        except BigQueryTimeoutException:
            raise BigQueryTimeoutException('BigQuery Timeout. job="query" query="%s"' % query)

        dataset_id = job_resource["configuration"]["query"]["destinationTable"]["datasetId"]
        table_id = job_resource["configuration"]["query"]["destinationTable"]["tableId"]
        return (dataset_id, table_id)

    def _export_table_to_gcs(self, dataset, table, folder_name, file_name, export_timeout=None):
        timeout = export_timeout or self.bq_default_export_timeout
        gs_path = 'gs://%s/%s/%s.csv-parts-*' % (self.gcs_bucket_name, folder_name, file_name)

        try:
            job = self.bq_client.export_data_to_uris([gs_path], dataset, table, print_header=False)
            job_resource = self.bq_client.wait_for_job(job, timeout=timeout)

        # re-raise exceptions with details if job resource is still running after timeout
        except BigQueryTimeoutException:
            raise BigQueryTimeoutException('BigQuery Timeout. job="export" location="GCS"')

    def _delete_file(self, folder_name, file_name):
        file_path = '%s/%s.csv' % (folder_name, file_name)
        key = self.gcs_bucket.get_key(file_path)
        if key:
            self.gcs_bucket.delete_key(key.name)

    def _delete_file_parts(self, folder_name, file_name):
        parts_path = '%s/%s.csv-parts-' % (folder_name, file_name)
        parts_list = self.gcs_bucket.list(parts_path)
        for parts in parts_list:
            self.gcs_bucket.delete_key(parts.name)

    def _join_file_parts(self, folder_name, file_name):
        main_file = '%s.csv' % file_name
        main_file_path = '%s/%s' % (folder_name, main_file)
        bucket_path = '%s/%s' % (folder_name, file_name)
        bucket_list = self.gcs_bucket.list(bucket_path)

        # if main file doesnt exist, create an empty one. We need at least 2 files for composing
        if main_file_path not in [key.name for key in bucket_list]:
            contents = ''
            new_object = Key(self.gcs_bucket, main_file_path)
            new_object.set_contents_from_string(contents, {'content-type': CONTENT_TYPE_CSV}, replace=False)

        # use a list to store list of keys in groups of 20. Like [[1...20], [21...40], [41...60]]
        list_of_key_list = list(utils.split_every(20, bucket_list))

        # compose and join new file in groups of 20
        # Note: Compose will create a new object each time so we need to append existing base file into key_list for each loop.
        # You can think of it as overwriting the existing base file on each compose.
        for key_list in list_of_key_list:
            existing_main_file = self.gcs_bucket.get_key(main_file_path)
            updated_key_list = key_list + [existing_main_file]
            joined_object = Key(self.gcs_bucket, main_file_path)
            joined_object.compose(updated_key_list, content_type=CONTENT_TYPE_CSV)

        # after compose, remove all parts file
        self._delete_file_parts(folder_name, file_name)

    def _export(self, dataset_temp, table_temp, folder_name, file_name):
        # before export, check and delete any existing file parts in google cloud storage
        self._delete_file_parts(folder_name, file_name)

        # now export table result to google cloud storage
        self._export_table_to_gcs(dataset_temp, table_temp, folder_name, file_name)

        # before join, delete any existing main file so that we are not appending to it
        self._delete_file(folder_name, file_name)
        self._join_file_parts(folder_name, file_name)

    def query_and_export(self, query, dataset_temp, table_temp, folder_name, file_name, query_timeout=None, export_timeout=None):
        """Run query and export results to Google Cloud Storage

        Parameters
        ----------
        query: string
            Query that runs on bigquery in string

        dataset_temp: string
            Name of temporary dataset used to store query results

        table_temp: string
            Name of temporary table used to store query results

        folder_name: string
            Name of query results folder in GCS

        file_name: string
            Name of query results file in GCS

        query_timeout: int, optional
            Timeout for query job in seconds

        export_timeout: int, optional
            Timeout for export job in seconds
        """

        # get or create temp dataset
        self._get_or_create_dataset(dataset_temp)

        # write to table. We need to use a table since "allowLargeResults=True" is only available
        # when you write to a table. Write truncate to clear any existing data if there is any
        _dataset_id, _table_id = self._write_to_table(dataset_temp, table_temp, query, JOB_WRITE_TRUNCATE, query_timeout)

        # export temp table to GCS
        self._export(dataset_temp, table_temp, folder_name, file_name)

        # delete temp table in bigquery once everything is complete
        self._delete_table(dataset_temp, table_temp)
