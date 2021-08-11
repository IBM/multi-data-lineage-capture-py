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


def get_data_reference(value, data_store_id) -> dict:
    return {
        Vocabulary.VALUES: value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATA_REFERENCE_TYPE,
        Vocabulary.DATA_STORE_ID: data_store_id
    }

