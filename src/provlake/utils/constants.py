import os
import uuid
from .prov_utils import convert_timestamp, id_hash


class Vocabulary:

    START_TIME = "startTime"
    END_TIME = "endTime"
    GENERATED_TIME = "generatedTime"
    STATUS = "status"
    VALUES = "values"
    DATASET_ITEM_ORDER = "order"
    STDOUT = "stdout"
    STDERR = "stderr"
    PERSON = "person"
    PARENT_CYCLE_ITERATION = "parent_cycle_iteration"
    PARENT_CYLE_NAME = "parent_cycle_name"
    CUSTOM_METADATA = "custom_metadata"
    PROV_OBJ = "prov_obj"
    WORKFLOW_NAME = "dataflow_name"
    ACT_TYPE = "act_type"
    ATTRIBUTE_ASSOCIATIONS = "attribute_associations"
    DATA_REFERENCE_TYPE = "data_reference"
    DATA_REFERENCE_TYPE_AS_IS = "data_reference_as_is"
    ATTRIBUTE_VALUE_TYPE = "attribute_value"
    DICT_TYPE = "dict"
    LIST_TYPE = "list"
    DATASET_TYPE = "dataset"
    DATA_STORE_ID = "data_store_id"
    PROV_ATTR_TYPE = "prov_attr_type"
    DATASET_SCHEMAS_KEY = "dataset_schemas"
    DATASET_ITEM = "dataset_item"
    DATASET_ID = "dataset_id"
    DATASET_SCHEMA_ID = "dataset_schema_id"

class DataStoreConfiguration:
    DATABASES_KEY = "databases"
    DATABASES_SCHEMAS_KEY = "database_schemas"
    DATASETS_SCHEMAS_KEY = "dataset_schemas"
    ATTRIBUTES_KEY = "attributes"
    IDENTIFIER_KEY = "identifier"

class FdwMapping:
    FDW_MAPPING = "fdw_mapping"
    FDW_TYPE = "fdw"
    FDW_ATTRIBUTE_MAPPINGS = "attribute_mappings"
    FDW_DATA_STORE_FIELD = "data_store"
    FDW_DATABASE_FIELD = "database"
    FDW_DATABASE_SCHEMA_FIELD = "database_schema"
    FDW_DATASET_SCHEMA_FIELD = "dataset_schema"

class FileTypes:

    CSV = "CSV"


class Status:

    GENERATED = "GENERATED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    ERRORED = "ERRORED"


class ActType:

    WORKFLOW = "workflow"
    TASK = "task"
    CYCLE = "cycle"


class DataTransformationRequestType:

    GENERATE = "Generate"
    INPUT = "Input"
    OUTPUT = "Output"


class PersistenceStrategy:

    SERVICE = "SERVICE"
    FILE = "FILE"
    KAFKA = "KAFKA"


class StandardNamesAndIds:

    @staticmethod
    def get_prov_log_file_path(log_dir:str, workflow_name:str, wf_start_time:float) -> str:
        return os.path.abspath(os.path.join(log_dir, 'prov-{}-{}.log'.format(workflow_name, wf_start_time)))

    @staticmethod
    def get_id_att(attribute_name, data_schema_id=None):
        if data_schema_id:
            return data_schema_id + "_" + attribute_name
        return attribute_name

    @staticmethod
    def get_id_dataset(dataset_name):
        return dataset_name + "_" + str(uuid.uuid4())

    @staticmethod
    def get_id_atv(attribute_id, value, value_type=None):
        if value_type:
            if value_type == Vocabulary.DATA_REFERENCE_TYPE:
                return "" + str(value)
            if value_type == Vocabulary.DATA_REFERENCE_TYPE_AS_IS:
                return "" + str(value)
            elif value_type == Vocabulary.DATASET_ITEM:
                return "dataset_item_"+str(uuid.uuid4())
            else:
                return attribute_id + "_" + str(value)
        else:
            if type(value) in [dict, list]:
                return attribute_id + "_" + id_hash(str(value))
            else:
                # TODO if its a float, replace the dots
                return attribute_id + "_" + str(value)

    @staticmethod
    def get_wfe_id(workflow_name: str, wf_exec_id):
        if isinstance(wf_exec_id, str):
            wfe_id = workflow_name.lower() + "_exec_" + wf_exec_id
        elif isinstance(wf_exec_id, float):
            try:
                time_str = convert_timestamp(wf_exec_id)
                wfe_id = workflow_name.lower() + "_exec_" + time_str
            except:
                wfe_id = workflow_name.lower() + "_exec_" + str(wf_exec_id)
        else:
            wfe_id = workflow_name.lower() + "_exec_" + str(wf_exec_id)
        return wfe_id



    @staticmethod
    def get_dte_id(wfe_id, dt_name: str, prov_task: dict):
        task_id = prov_task["id"]
        # TODO better wfe_id + dte_name + timestamp
        if Vocabulary.GENERATED_TIME in prov_task and \
            prov_task[Vocabulary.GENERATED_TIME] == task_id:
            dte_id = dt_name + convert_timestamp(
                prov_task[Vocabulary.GENERATED_TIME]) + wfe_id
        elif Vocabulary.START_TIME in prov_task and \
            prov_task[Vocabulary.START_TIME] == task_id:
            dte_id = dt_name + convert_timestamp(
                prov_task[Vocabulary.START_TIME]) + wfe_id
        else:
            dte_id = wfe_id + "_" + dt_name + "_" + str(task_id)

        return dte_id

    @staticmethod
    def get_wfe_ctx_id(wfe_id):
        return wfe_id+"_wfe_ctx"

    @staticmethod
    def get_cce_ctx_id(cce_id):
        return cce_id + "_cce_ctx"

    @staticmethod
    def get_cci_ctx_id(cci_id):
        return cci_id + "_cci_ctx"

    @staticmethod
    def get_wfe_instantiations_ctx_id(wfe_id):
        return wfe_id + "_wfe_instantiation_ctx"

    @staticmethod
    def get_cce_instantiations_ctx_id(cce_id):
        return cce_id + "_cce_instantiation_ctx"

    @staticmethod
    def get_cci_instantiations_ctx_id(cci_id):
        return cci_id + "_cci_instantiation_ctx"

    @staticmethod
    def get_id_prj(project_name):
        return project_name

    @staticmethod
    def get_data_store_id(data_store_name):
        return data_store_name

    @staticmethod
    def get_data_store_ctx_id(data_store_id):
        return data_store_id + "_ctx"

    @staticmethod
    def get_database_id(database_name, data_store_id):
        return data_store_id + "_" + database_name

    @staticmethod
    def get_database_schema_id(database_schema_name, database_id):
        return database_id + "_" + database_schema_name

    @staticmethod
    def get_dataset_schema_id(dataset_schema_name, database_schema_id):
        return database_schema_id + "_" + dataset_schema_name

    @staticmethod
    def get_fdw_attribute_id(attribute_name, prefix):
        return prefix + "_" + attribute_name

    @staticmethod
    def get_domain_class_id(domain_class_name):
        return domain_class_name + "_class"

    @staticmethod
    def get_domain_class_schema_id(domain_class_name):
        return domain_class_name + "_schema"

