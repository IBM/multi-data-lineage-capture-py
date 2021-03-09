def get_dict(_dict:dict) -> dict:
    if _dict is None:
        return None
    if len(_dict) == 0:
        return {}
    return {
        "type": "dict",
        "values": _dict
    }

def get_list(_list:list) -> dict:
    if _list is None:
        return None
    if len(_list) == 0:
        return []
    return {
       "type": "list",
        "values": _list
    }

def get_recursive_dicts(_dict:dict) -> dict:
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
        "type": "dict",
        "values": values
    }
