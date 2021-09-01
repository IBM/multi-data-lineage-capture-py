from datetime import datetime
import hashlib
import os
import glob
import json

JSON_INDENT_SIZE = 1


def convert_timestamp(timestamp: float) -> str:
    t = datetime.utcfromtimestamp(timestamp)
    return t.strftime('%Y%m%dT%Hh%Mm%Ss%f')[:-3]


def id_hash(val: str) -> str:
    return hashlib.md5(val.encode()).hexdigest()


def delete_prov_logs(log_dir: str = '.') -> None:
    try:
        [os.remove(f) for f in glob.glob(os.path.join(log_dir, 'prov-*.log'))]
    except:
        pass


def stringfy_inner_dicts_in_dicts(_dict: dict) -> dict:
    ret = dict()
    for k in _dict:
        if type(_dict[k]) == dict:
            ret[k] = json.dumps(_dict[k])
        else:
            ret[k] = _dict[k]
    return ret


def stringfy_inner_dicts_in_lists(_list: list) -> list:
    return [json.dumps(el, indent=JSON_INDENT_SIZE) if type(el) == dict else el for el in _list]


