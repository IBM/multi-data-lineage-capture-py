from provlake.model.data_store_prov_obj import DataStoreObj
from provlake.utils.constants import Routes
import requests

class DataStorePersister:

    def create_data_store(self, service_url:str, name: str, type: str, id: str=None, custom_metadata: dict=None ):
        data_store_obj = DataStoreObj(name=name, type=type, id=id, custom_metadata=custom_metadata)
        url = f"{service_url}{Routes.DATA_STORES}"
        response = requests.post(url, json=data_store_obj.as_dict())
        assert 201==response.status_code, "error in calling the server"
        return response.json()

    def get_data_stores(self, service_url: str):
        url = f"{service_url}{Routes.DATA_STORES}"
        response = requests.get(url)
        assert 200==response.status_code, "error in calling the server"
        return response.json()