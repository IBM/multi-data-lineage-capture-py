from datetime import datetime
import hashlib
import os
import glob


def convert_timestamp(timestamp: float):
    t = datetime.fromtimestamp(timestamp)
    return t.strftime('%Y%m%dT%Hh%Mm%Ss%f')[:-3]


def id_hash(val: str):
    return hashlib.md5(val.encode()).hexdigest()


def delete_prov_logs(log_dir: str = '.'):
    try:
        [os.remove(f) for f in glob.glob(os.path.join(log_dir, 'prov-*.log'))]
    except:
        pass
