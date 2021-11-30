from provlake.model.data_store_prov_obj import DataStoreObj
from provlake.utils.constants import Routes
import requests
import urllib.parse


class DataStoreCatalog:
    def __init__(self, service_url: str, headers=None):
        self.service_url = service_url
        if headers is None:
            headers = dict()
        self.headers = headers

    def create_data_store(self, name: str, type: str, id: str = None, custom_metadata: dict = None, **kwargs):
        data_store_obj = DataStoreObj(name=name, type=type, id=id, custom_metadata=custom_metadata, **kwargs)
        url = f"{self.service_url}{Routes.SERVER_API_ROOT}{Routes.DATA_STORES}"
        response = requests.post(url, json=data_store_obj.as_dict(), headers=self.headers)
        assert 201 == response.status_code, "error in calling the server"
        return response.json()

    def get_data_stores(self):
        url = f"{self.service_url}{Routes.SERVER_API_ROOT}{Routes.DATA_STORES}"
        response = requests.get(url, headers=self.headers)
        assert 200 == response.status_code, "error in calling the server"
        return response.json()

    def get_data_store(self, id_: str):
        url = f"{self.service_url}{Routes.SERVER_API_ROOT}{Routes.DATA_STORES}/{urllib.parse.quote(id_)}"
        response = requests.get(url, headers=self.headers)
        if not response.ok:
            error_msg = f'Error: [code: {response.status_code}] {response.content}'
            print(error_msg)
            raise AssertionError(error_msg)
        return response.json()
