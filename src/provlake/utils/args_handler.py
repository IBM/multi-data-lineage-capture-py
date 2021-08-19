from provlake.utils.constants import Vocabulary


def get_dict(_dict: dict) -> dict:
    if _dict is None:
        return None
    if len(_dict) == 0:
        return {}
    return {
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DICT_TYPE,
        Vocabulary.VALUES: _dict
    }


def get_list(_list: list) -> dict:
    '''
    This will make ProvLake create one node per element in the list
    '''
    if _list is None:
        return None
    if len(_list) == 0:
        return []
    return {
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.LIST_TYPE,
        Vocabulary.VALUES: _list
    }


def get_dataset(_list_of_dataset_items: list, dataset_schema_id=None) -> dict:
    '''
    This will make ProvLake create one node per element in the list
    '''
    if _list_of_dataset_items is None:
        return None
    if len(_list_of_dataset_items) == 0:
        return []
    ret = {
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATASET_TYPE,
        Vocabulary.VALUES: _list_of_dataset_items
    }
    if dataset_schema_id:
        ret[Vocabulary.DATASET_SCHEMA_ID] = dataset_schema_id
    return ret


def get_recursive_dicts(_dict: dict) -> dict:
    if _dict is None:
        return None
    if len(_dict) == 0:
        return {}
    values = dict()
    for k in _dict:
        v = _dict[k]
        if type(v) == dict:
            values[k] = get_recursive_dicts(v)
        else:
            values[k] = v
    return {
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DICT_TYPE,
        Vocabulary.VALUES: values
    }


def add_custom_metadata(value, custom_metadata: dict = None) -> dict:
    if not custom_metadata:
        return value
    return {
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.ATTRIBUTE_VALUE_TYPE,
        Vocabulary.VALUES: value,
        Vocabulary.CUSTOM_METADATA: custom_metadata
    }


def get_data_reference(value, data_store_id=None) -> dict:
    ret = {
        Vocabulary.VALUES: value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATA_REFERENCE_TYPE
    }
    if data_store_id is not None:
        ret[data_store_id] = data_store_id
    return ret


def get_data_reference_as_is(value, data_store_id=None) -> dict:
    ret = {
        Vocabulary.VALUES: value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATA_REFERENCE_TYPE_AS_IS
    }
    if data_store_id is not None:
        ret[data_store_id] = data_store_id
    return ret



def get_dataset_item(values, order:int=None) -> dict:
    ret = {
        Vocabulary.VALUES: values,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATASET_ITEM
    }
    if order is not None:
        ret[Vocabulary.DATASET_ITEM_ORDER] = order
    return ret