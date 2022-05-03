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
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.ATTRIBUTE_VALUE_WITH_CUSTOM_METADATA_TYPE,
        Vocabulary.VALUES: value,
        Vocabulary.CUSTOM_METADATA: custom_metadata
    }


def get_data_reference(value, data_store_id=None) -> dict:
    ret = {
        Vocabulary.VALUES: value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATA_REFERENCE_TYPE
    }
    if data_store_id is not None:
        ret[Vocabulary.DATA_STORE_ID] = data_store_id
    return ret


def get_kg_reference(value, data_store_id=None) -> dict:
    ret = {
        Vocabulary.VALUES: value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.KG_REFERENCE_TYPE
    }
    if data_store_id is not None:
        ret[Vocabulary.DATA_STORE_ID] = data_store_id
    return ret


def get_dataset_item(values, order: int = None, dataset_id=None) -> dict:
    ret = {
        Vocabulary.VALUES: values,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATASET_ITEM
    }
    if order is not None:
        ret[Vocabulary.DATASET_ITEM_ORDER] = order
    if dataset_id is not None:
        ret[Vocabulary.DATASET_ID] = dataset_id
    return ret


def get_attribute_value_type(attribute_value) -> str:
    if attribute_value is None:
        return Vocabulary.SIMPLE_ATV_TYPE
    elif type(attribute_value) in [int, str, bool, float]:
        return Vocabulary.SIMPLE_ATV_TYPE
    elif type(attribute_value) == list:
        return Vocabulary.SIMPLE_LIST_TYPE
    else:
        if Vocabulary.PROV_ATTR_TYPE in attribute_value:
            return attribute_value[Vocabulary.PROV_ATTR_TYPE]
        else:
            return Vocabulary.SIMPLE_DICT_TYPE
