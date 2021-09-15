from provlake.model.data_store_prov_obj import DataStoreObj
from provlake.utils.constants import Routes
import requests


class DataStoreCatalog:
    def __init__(self, service_url: str, headers=None):
        self.service_url = service_url
        if headers is None:
            headers = dict()
        self.headers = headers

    def create_data_store(self, name: str, type: str, id: str = None, custom_metadata: dict = None):
        data_store_obj = DataStoreObj(name=name, type=type, id=id, custom_metadata=custom_metadata)
        url = f"{self.service_url}{Routes.DATA_STORES}"
        response = requests.post(url, json=data_store_obj.as_dict(), headers=self.headers)
        assert 201 == response.status_code, "error in calling the server"
        return response.json()

    def get_data_stores(self):
        url = f"{self.service_url}{Routes.DATA_STORES}"
        response = requests.get(url, headers=self.headers)
        assert 200 == response.status_code, "error in calling the server"
        return response.json()
