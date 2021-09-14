from provlake.utils.constants import StandardNamesAndIds, DataStores

class DataStoreObj:

    def __init__(self, name: str, type: str, id: str=None, custom_metadata: dict=None):
        assert type in DataStores.get_known_data_stores(), \
            'Data store {data_store} not known.'.format(data_store=type)
        self.id = id
        if not self.id:
            self.id = name
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
