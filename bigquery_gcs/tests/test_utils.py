import unittest
import mock
from bigquery_gcs import utils


class TestUtils(unittest.TestCase):

    def test_split_every(self):
        result = utils.split_every(5, range(9))
        self.assertEquals(list(result), [[0, 1, 2, 3, 4], [5, 6, 7, 8]])
