from provlake.utils.constants import StandardNamesAndIds, DataStores
import uuid

class DataStoreObj:

    def __init__(self, name: str, type: str, id: str=None, custom_metadata: dict=None):
        assert type in DataStores.get_known_data_stores(), \
            'Data store {data_store} not known.'.format(data_store=type)
        self.id = id
        if not self.id:
            self.id = "ds_"+str(uuid.uuid4()).replace('-', '_')
        self.name = name
        self.type = type
        self.custom_metadata = custom_metadata

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "custom_metadata": self.custom_metadata
        }
