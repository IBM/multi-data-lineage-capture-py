from provlake.utils.constants import StandardNamesAndIds, DataStores
import uuid

class DataStoreObj:

    def __init__(self, name: str, type: str, id: str=None, custom_metadata: dict=None, **kwargs):
        assert type in DataStores.get_known_data_stores(), \
            'Data store {data_store} not known.'.format(data_store=type)
        self.id = id
        if not self.id:
            self.id = "ds_"+str(uuid.uuid4()).replace('-', '_')
        self.name = name
        self.type = type
        if custom_metadata:
            self.custom_metadata = custom_metadata
        super_type = DataStores.get_data_store_super_type(self.type)
        if super_type in DataStores.DATA_STORE_IDENTIFIERS.keys():
            # assert len(kwargs.keys()) == len(DataStores.DATA_STORE_IDENTIFIERS[super_type]), \
            #     f"Identifiers attributes of data store {type} {name} not defined properly."

            for attr in DataStores.DATA_STORE_IDENTIFIERS[super_type]:
                assert attr in kwargs.keys(), \
                    f"Attribute {attr} is required for {self.type} data store."
                setattr(self, attr, kwargs.get(attr))

    def as_dict(self) -> dict:
        attribs = lambda obj : {o : getattr(self, o) for o in dir(obj) if (not o.startswith('_') and not callable(getattr(obj, o)))}
        return attribs(self)

