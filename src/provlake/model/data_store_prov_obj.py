from provlake.utils.constants import StandardNamesAndIds, DataStores

class DataStoreObj:

    def __init__(self, id: str, name: str, type: str, custom_metadata: dict=None):
        if type not in DataStores.get_known_data_stores():
            raise Exception('Data store {data_store} not known.'.format(data_store=data_store_obj.type))
        self.id = id
        self.name = name
        self.type = type
        self.custom_metadata = custom_metadata

    def as_dict(self) -> dict:
        data_store_dict = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "custom_metadata": self.custom_metadata
        }
