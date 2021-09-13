from provlake.utils.constants import StandardNamesAndIds

class DataStoreObj:


    def __init__(self, id: str, name: str, type: str, db_system: str=None, custom_metadata: dict=None):
        self.id = id
        self.name = name
        self.type = type
        self.db_system = db_system
        self.custom_metadata = custom_metadata

    def as_dict(self) -> dict:
        data_store_dict = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "db_system": self.db_system,
            "custom_metadata": self.custom_metadata
        }
