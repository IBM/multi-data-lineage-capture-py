from provlake.utils.constants import Vocabulary

# TODO Finish adding the keys in the returning dicts as constants
def get_dict(_dict: dict) -> dict:
    if _dict is None:
        return None
    if len(_dict) == 0:
        return {}
    return {
        Vocabulary.PROV_ATTR_TYPE: "dict",
        "values": _dict
    }


def get_list(_list: list) -> dict:
    if _list is None:
        return None
    if len(_list) == 0:
        return []
    return {
        Vocabulary.PROV_ATTR_TYPE: "list",
        "values": _list
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
        Vocabulary.PROV_ATTR_TYPE: "dict",
        "values": values
    }


def add_custom_metadata(value, custom_metadata: dict = None) -> dict:
    if not custom_metadata:
        return value
    return {
        Vocabulary.PROV_ATTR_TYPE: "attribute_value",
        "values": value,
        "custom_metadata": custom_metadata
    }


def get_data_reference(value, datastore_id) -> dict:
    return {
        "values": value,
        Vocabulary.PROV_ATTR_TYPE: Vocabulary.DATA_REFERENCE_TYPE,
        "datastore_id": datastore_id
    }

