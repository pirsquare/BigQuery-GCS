import unittest
import mock
from bigquery.client import JOB_WRITE_TRUNCATE, JOB_CREATE_IF_NEEDED, JOB_WRITE_APPEND
from bigquery_gcs.base import Exporter, CONTENT_TYPE_CSV


class TestExporter(unittest.TestCase):

    def setUp(self):
        self.gcs_access_key = "a"
        self.gcs_secret_key = "b"
        self.gcs_bucket_name = "c"
        self.bq_project_id = "d"
        self.bq_service_account = "e"
        self.bq_private_key_path = "f"
        self.bq_default_query_timeout = "g"
        self.bq_default_export_timeout = "h"

        self.config = {}
        self.config['GCS_ACCESS_KEY'] = self.gcs_access_key
        self.config['GCS_SECRET_KEY'] = self.gcs_secret_key
        self.config['GCS_BUCKET_NAME'] = self.gcs_bucket_name
        self.config['BQ_PROJECT_ID'] = self.bq_project_id
        self.config['BQ_SERVICE_ACCOUNT'] = self.bq_service_account
        self.config['BQ_PRIVATE_KEY_PATH'] = self.bq_private_key_path
        self.config['BQ_DEFAULT_QUERY_TIMEOUT'] = self.bq_default_query_timeout
        self.config['BQ_DEFAULT_EXPORT_TIMEOUT'] = self.bq_default_export_timeout
        self.exporter = Exporter(self.config)

    @mock.patch.object(Exporter, "_get_file")
    def test_bq_private_key(self, mock_get_file):
        mock_get_file.return_value = "expected"

        result = self.exporter.bq_private_key

        self.assertEquals(result, "expected")
        mock_get_file.assert_called_with(self.bq_private_key_path)

    @mock.patch("bigquery_gcs.base.boto.connect_gs")
    def test_gcs_client(self, mock_connect_gs):
        mock_connect_gs.return_value = "expected"

        result = self.exporter.gcs_client

        self.assertEquals(result, "expected")
        mock_connect_gs.assert_called_with(self.gcs_access_key, self.gcs_secret_key)

    @mock.patch.object(Exporter, "bq_private_key")
    @mock.patch("bigquery_gcs.base.get_client")
    def test_bq_client(self, mock_bq_client, mock_bq_private_key):
        mock_bq_private_key.return_value = "anything"
        mock_bq_client.return_value = "expected"

        result = self.exporter.bq_client

        self.assertEquals(result, "expected")

    @mock.patch("bigquery_gcs.base.Bucket")
    def test_gcs_bucket(self, mock_bucket):
        mock_bucket.return_value = "expected"

        result = self.exporter.gcs_bucket

        self.assertEquals(result, "expected")

    @mock.patch.object(Exporter, "bq_client")
    def test_delete_table(self, mock_bq_client):
        self.exporter._delete_table("test", "test2")
        mock_bq_client.delete_table.assert_called_with("test", "test2")

    @mock.patch.object(Exporter, "bq_client")
    def test_write_to_table(self, mock_bq_client):
        job_resp = {
            "configuration": {
                "query": {
                    "destinationTable": {
                        "datasetId": "55",
                        "tableId": "66"
                    }
                }
            }
        }

        mock_bq_client.write_to_table.return_value = "job"
        mock_bq_client.wait_for_job.return_value = job_resp

        dataset_id, table_id = self.exporter._write_to_table("dataset", "table", "query", "write_disposition")

        self.assertEquals(dataset_id, "55")
        self.assertEquals(table_id, "66")
        mock_bq_client.write_to_table.assert_called_with(
            query="query",
            dataset="dataset",
            table="table",
            create_disposition=JOB_CREATE_IF_NEEDED,
            write_disposition="write_disposition",
            allow_large_results=True
        )

        mock_bq_client.wait_for_job.assert_called_with("job", timeout=self.bq_default_query_timeout)

    @mock.patch.object(Exporter, "bq_client")
    def test_export_table_to_gcs(self, mock_bq_client):
        mock_bq_client.export_data_to_uris.return_value = "job"

        self.exporter._export_table_to_gcs("dataset_id", "table_id", "myfolder", "myfile")

        expected_gspath = 'gs://c/myfolder/myfile.csv-parts-*'
        mock_bq_client.export_data_to_uris.assert_called_with(
            [expected_gspath],
            "dataset_id",
            "table_id",
            print_header=False
        )

    @mock.patch.object(Exporter, "gcs_bucket")
    def test_delete_file(self, mock_bucket):
        mock_bucket.get_key.return_value = None

        self.exporter._delete_file("myfolder", "myfile")

        # delete_key should not be called
        expected_filepath = 'myfolder/myfile.csv'
        mock_bucket.get_key.assert_called_with(expected_filepath)
        assert not mock_bucket.delete_key.called

        # create anonymous object
        # http://stackoverflow.com/questions/1528932/how-to-create-inline-objects-with-properties-in-python
        key = type('key', (object,), {'name': 'paul'})

        # delete_key should be called
        mock_bucket.get_key.return_value = key
        self.exporter._delete_file("myfolder", "myfile")
        mock_bucket.delete_key.assert_called_with("paul")

    @mock.patch.object(Exporter, "gcs_bucket")
    def test_delete_file_parts(self, mock_bucket):
        mock_bucket.list.return_value = []

        self.exporter._delete_file_parts("myfolder", "myfile")

        # delete_key should not be called
        expected_parts_path = 'myfolder/myfile.csv-parts-'
        mock_bucket.list.assert_called_with(expected_parts_path)
        assert not mock_bucket.delete_key.called

        # create anonymous object
        # http://stackoverflow.com/questions/1528932/how-to-create-inline-objects-with-properties-in-python
        parts = type('parts', (object,), {'name': 'paul'})

        # delete_key should be called
        mock_bucket.list.return_value = [parts]
        self.exporter._delete_file_parts("myfolder", "myfile")
        mock_bucket.delete_key.assert_called_with("paul")

    @mock.patch("bigquery_gcs.utils.split_every")
    @mock.patch("bigquery_gcs.base.Key")
    @mock.patch.object(Exporter, "_delete_file_parts")
    @mock.patch.object(Exporter, "gcs_bucket")
    def test_delete_file_parts(self, mock_bucket, mock_delete_file_parts, mock_key, mock_split_every):

        mock_bucket.list.return_value = []
        mock_bucket.get_key.return_value = 3
        mock_split_every.return_value = [[0, 1]]

        # mock a Key instance since key instance is created inside self._join_file_parts method
        mock_key_instance = mock.Mock()
        mock_key.return_value = mock_key_instance

        self.exporter._join_file_parts("myfolder", "myfile")

        mock_key_instance.set_contents_from_string.assert_called_with('', {'content-type': CONTENT_TYPE_CSV}, replace=False)
        mock_split_every.assert_called_with(20, [])
        mock_bucket.get_key.assert_called_with('myfolder/myfile.csv')
        mock_delete_file_parts.assert_called_with("myfolder", "myfile")
        mock_key_instance.compose.assert_called_with([0, 1, 3], content_type=CONTENT_TYPE_CSV)
