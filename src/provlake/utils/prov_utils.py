"""
#####
# Exemplary Content of a prov.yaml:
#####
context_name: seismic_pipelines
dataflow_name:
  train: seismic_stratigraphic_train
  create_data: seismic_stratigraphic_create_training_data
  analyze_seismic: seismic_stratigraphic_analyze_seismic
file_storage:
with_validation: false
should_send_to_file: true
log_dir: .
align_terms:
  segy_path: seismic_file
  horizons_paths: horizon_file
graph_uri: <http://rockml.br.ibm.com>
stringify_params: ["unite_category"]
not_tracked_params: ["prov", "prov_path", "input_path", "log_level", "dataset", "prov_config", "data_path" ]
bag_size: 1
service: http://localhost:5000
should_send_to_service: false
    log_level: info
"""
import yaml
from enum import Enum
import logging
from datetime import datetime
import hashlib
logger = logging.getLogger('PROV')


class MLSemantics(Enum):
    hyper_parameter = "hyper_parameter"
    data_characteristic = "data_characteristic"
    dataset = "dataset"
    evaluation_measure = "evaluation_measure"
    model = "model"

    def __str__(self):
        return self.value


class DTYPE(Enum):
    int = "integer"
    str = "string"
    bool = "boolean"
    list = "list"
    float = "float"
    complex = "complex_attribute"
    any = "any"

    def __str__(self):
        return self.value


class ProvConf:
    conf_data: dict = None
    custom_attributes: dict = None

    def __init__(self, prov_conf_path: str = None):
        if not prov_conf_path:
            logger.warning("[Prov] You are not capturing provenance.")
            return
        with open(prov_conf_path, 'r') as stream:
            try:
                ProvConf.conf_data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise exc

        if "custom_attributes" in ProvConf.conf_data:
            with open(ProvConf.conf_data["custom_attributes"], 'r') as stream:
                try:
                    ProvConf.custom_attributes = yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    raise exc


def get_dtype_from_val(value, should_stringfy=False) -> str:
    if should_stringfy:
        return DTYPE.str
    elif type(value) == str:
        return DTYPE.str
    elif type(value) == list:
        return DTYPE.list
    elif type(value) == int:
        return DTYPE.int
    elif type(value) == float:
        return DTYPE.float
    elif type(value) == bool:
        return DTYPE.bool
    elif type(value) == dict:
        return DTYPE.complex
    else:
        return DTYPE.any


def get_dtype_from_type(_type: type, should_stringfy=False) -> str:
    if should_stringfy:
        return str(DTYPE.str)
    elif _type == str:
        return str(DTYPE.str)
    elif _type == list:
        return str(DTYPE.list)
    elif _type == int:
        return str(DTYPE.int)
    elif _type == float:
        return str(DTYPE.float)
    elif _type == bool:
        return str(DTYPE.bool)
    elif _type == dict:
        return str(DTYPE.complex)
    else:
        return "any"


def build_generic_prospective(dataflow_name: str):
    prospective_prov = dict()
    prospective_prov["dataflow_name"] = ProvConf.conf_data["dataflow_name"][dataflow_name]
    prospective_prov["context_name"] = ProvConf.conf_data["context_name"]
    prospective_prov["storages"] = {"main_filesystem": {"type": "PhysicalMachine"}}
    prospective_prov["data_transformations"] = {}
    storage_configuration = {
        "configuration": {
            "storages": {
                "main_filesystem": {
                    "type": "PhysicalMachine",
                    "host_address": ProvConf.conf_data["file_storage"]
                }
            }
        }
    }
    return prospective_prov, storage_configuration


# @Deprecated -- this is going to be depricated. Please avoid using it.
def build_prov_input_from_dict(dict_params: dict):
    retrospective_input_prov = {}
    attributes = []

    for key in dict_params:
        # Remove params not tracked:
        if "not_tracked_params" in ProvConf.conf_data and key in ProvConf.conf_data["not_tracked_params"]:
            continue

        value = dict_params[key]
        if value is None or (value is not None and value == ''):
            continue

        # Renaming param names in case we need:
        attr_name = key
        if "align_terms" in ProvConf.conf_data and key in ProvConf.conf_data["align_terms"]:
            attr_name = ProvConf.conf_data["align_terms"][key]

        should_stringify = __get_should_stringify(attr_name)
        if should_stringify:
            retrospective_input_prov[attr_name] = str(value)
        else:
            retrospective_input_prov[attr_name] = value
        attr = {
            "name": attr_name,
            #"description": ""
        }
        if "path" in attr_name or "file" in attr_name:
            attr["semantics"] = "FILE_REFERENCE"
            attr["storage_references"] = {
                "key": "main_filesystem"
            }
        else:
            attr["semantics"] = "PARAMETER"
            attr["ml_semantics"] = str(MLSemantics.hyper_parameter)

        # if "_slices" in attr_name:
        #     # we have a special case for slices in a different method
        #     continue

        dtype = get_dtype_from_val(value, should_stringify)
        if dtype == "list":
            attr["dtype"] = dtype
            if "REFERENCE" not in attr["semantics"] and len(value) > 0:
                attr["elementdtype"] = str(get_dtype_from_val(value[0], should_stringify))
        elif "REFERENCE" not in attr["semantics"]:
            attr["dtype"] = dtype

        attributes.append(attr)

    return attributes, retrospective_input_prov


def __get_should_stringify(key):
    if "stringify_params" not in ProvConf.conf_data:
        return False
    if key not in ProvConf.conf_data["stringify_params"]:
        return False
    return True


def get_prospective_attribute(key, value):
    ml_semantics = None

    if type(value) == type:
        dtype = get_dtype_from_type(value)
    elif type(value) == DTYPE:
        dtype = value
    elif type(value) == dict:
        dtype = value.get("dtype", "any")
        ml_semantics = value.get("ml_semantics", None)
    else:
        dtype = get_dtype_from_val(value)

    attr = {
        "name": key,
        "dtype":  str(dtype)
    }

    if ml_semantics:
        attr["ml_semantics"] = str(ml_semantics)

    if "path" in key or "file" in key:
        attr["semantics"] = "FILE_REFERENCE"
        attr["storage_references"] = {"key": "main_filesystem"}

    if dtype == DTYPE.list:
        if len(value) > 0:
            attr["elementdtype"] = str(get_dtype_from_val(value[0]))
    elif dtype == DTYPE.complex:
        attr["attributes"] = get_prospective_from_args(value)

    return attr


def get_prospective_from_args(prov_args: dict) -> list:
    args_schema = []

    for k in prov_args:
        # removing unwanted attributes
        if k == "self" or k.startswith('_') or "prov" in k:
            continue

        attr = get_prospective_attribute(k, prov_args[k])
        args_schema.append(attr)
    return args_schema


def build_prov_for_transformation(prospective_prov: dict, transformation):
    # IN
    prospective_in, prospective_out = [], []
    if hasattr(transformation, "static_prospective_prov_attributes_in") \
            and transformation.static_prospective_prov_attributes_in:
        prospective_in = transformation.static_prospective_prov_attributes_in
    elif transformation.static_schema_args_in:
        prospective_in = get_prospective_from_args(transformation.static_schema_args_in)
    else:
        transformation.static_schema_args_in = transformation.get_static_schema_args_from_init()
        prospective_in = get_prospective_from_args(transformation.static_schema_args_in)

    # OUT
    if transformation.static_schema_args_out:
        prospective_out = get_prospective_from_args(transformation.static_schema_args_out)
    # elif hasattr(transformation, "prospective_args_out") and transformation.prospective_args_out:
    #     prospective_out = transformation.prospective_args_out
    prospective_prov["data_transformations"].update({
        transformation.dt_name(): {
            "input_datasets": [{"attributes": prospective_in}],
            "output_datasets": [{"attributes": prospective_out}]
        }
    })
    return prospective_prov


def convert_timestamp(timestamp: float):
    t = datetime.fromtimestamp(timestamp)
    return t.strftime('%Y%m%dT%Hh%Mm%Ss%f')[:-3]


def id_hash(val: str):
    return hashlib.md5(val.encode()).hexdigest()
