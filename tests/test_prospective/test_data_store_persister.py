import unittest
from provlake.prospective.data_store_catalog import DataStoreCatalog
from provlake.utils.constants import DataStores
import os

PROV_LAKE_SERVER_URL = os.getenv('PROV_LAKE_SERVER_URL', 'http://127.0.0.1:5000')


class TestDataStorePersister(unittest.TestCase):
    def setUp(self) -> None:
        super(TestDataStorePersister, self).setUp()
        self.service_url = PROV_LAKE_SERVER_URL

    def test_create_data_store(self):
        data_store_persister = DataStoreCatalog(service_url=self.service_url)
        result = data_store_persister.create_data_store(name="Postgres1",
                                                        type=DataStores.POSTGRESQL, id="Postgres01",
                                                        custom_metadata={"host_address": "localhost"})
        self.assertEqual("Postgres01", result["data_store_hkg_id"], "Error in creating data store.")

    def test_get_data_store(self):
        data_store_persister = DataStoreCatalog(self.service_url)
        result = data_store_persister.create_data_store(name="Postgres30",
                                                        type=DataStores.POSTGRESQL, id="Postgres30",
                                                        custom_metadata={"host_address": "localhost"})
        self.assertEqual("Postgres30", result["data_store_hkg_id"], "Error in creating data store.")
        data_stores = data_store_persister.get_data_stores()
        found = False
        for data_store in data_stores:
            if data_store["hkg_id"] == "Postgres30":
                found = True
                break
        self.assertTrue(found, "Error on getting data store.")
