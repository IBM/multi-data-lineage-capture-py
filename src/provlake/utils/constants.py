import os
import uuid
from .prov_utils import convert_timestamp, id_hash


class Routes:

    # This class defines the routes provided by HKProv service's endpoints defined in the OpenAPI contract

    SERVER_API_ROOT = "/provenance/api"

    # PROSPECTIVE ROUTES
    METAMODEL_LOAD = "/metamodel/load"
    PROJECTS = f"{SERVER_API_ROOT}/projects"
    DATA_STORES = "/data-stores"
    QUERY_MANAGEMENT = f"{SERVER_API_ROOT}/stored-queries"
    WORKFLOWS = "/workflows"
    DATA_TRANSFORMATIONS = "/data-transformations"

    # RETROSPECTIVE ROUTES
    CYCLE_EXECUTIONS = f"{SERVER_API_ROOT}/workflow-executions/<path:workflow_execution_id>/cycle-executions"

    DATA_TRANSFORMATION_EXECUTIONS = "/data-transformation-executions"

    DATA_TRANSFORMATION_EXECUTIONS_BY_PERSON = \
        "/persons/<path:person_id>/data-transformation-executions"

    WORKFLOW_EXECUTIONS = "/workflow-executions"

    GLOBAL_WORKFLOW_EXECUTION = "/global-workflow-execution"

    WORKFLOW_EXECUTIONS_BY_WORKFLOW_EXECUTION = f"{WORKFLOW_EXECUTIONS}/<path:workflow_execution_id>"

    WORKFLOW_EXECUTIONS_SUMMARY_BY_WORKFLOW_EXECUTION = \
        "/workflow-execution-summaries/<path:workflow_execution_id>"


class EndpointsTypes:

    # This class define the types used in the endpoints schema, i.e., they are the same defined in the OpenAPI Contract

    WORKFLOW_EXECUTION_ID = "workflow_execution_id"
    GENERATED_TIME = "generated_time"
    START_TIME = "start_time"
    END_TIME = "end_time"
    WORKFLOW_NAME = "workflow_name"
    DATA_TRANSFORMATIONS_OF_WORKFLOW = "data_transformations"
    DATA_TRANSFORMATIONS_EXECUTIONS_OF_WORKFLOW_EXECUTION = "data_transformation_executions"
    NUMBER_OF_DATA_TRANSFORMATION_EXECUTIONS = "number_of_data_transformation_executions"
    STATUS = "status"
    WORKFLOW_EXECUTION_HKG_ID = "workflow_execution_hkg_id"
    DATA_STORE_HKG_ID = "data_store_hkg_id"
    DATA_TRANSFORMATION_NAME = "data_transformation_name"
    DATA_TRANSFORMATION_EXECUTION_ID = "data_transformation_execution_id"
    INPUT = "input"
    OUTPUT = "output"
    ATTRIBUTE_NAME = "attribute_name"
    ATTRIBUTE_VALUE_ID = "attribute_value_id"
    ATTRIBUTE_VALUE = "attribute_value"
    ATTRIBUTE_VALUE_TYPE = "attribute_value_type"
    DATA_STORE_ID = "data_store_id"
    CUSTOM_METADATA = "custom_metadata"
    TASK_ID = "task_id"
    PERSON_ID = "person_id"
    DATA_TRANSFORMATION_EXECUTION_HKG_ID = "data_transformation_execution_hkg_ids"

class Vocabulary:
    ID = "ID"
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
    PROV_OBJ_ID = "id"
    PROV_OBJ_DT = "dt"
    WORKFLOW_NAME = "dataflow_name"
    WF_EXECUTION= "wf_execution"
    ACT_TYPE = "act_type"
    ATTRIBUTE_ASSOCIATIONS = "attribute_associations"
    DTE_TYPE = "type"
    PROV_ATTR_TYPE = "prov_attr_type"

    DATASET_SCHEMAS_KEY = "dataset_schemas"
    DATASET_ITEM = "dataset_item"
    DATASET_ID = "dataset_id"
    DATASET_SCHEMA_ID = "dataset_schema_id"
    DATA_STORE_ID = "data_store_id"

    DATA_TRANSFORMATION_ID = "data_transformation_id"
    DATA_TRANSFORMATION_NAME = "data_transformation_name"
    INPUT = "input"
    OUTPUT = "output"
    ATTRIBUTE_VALUE_DATA_TYPE = "datatype"

    CHECK_RESULT = "check_result"
    OPERATION_RESULT = "result"
    MESSAGE = "message"

    # Types:
    DATA_REFERENCE_TYPE = "data_reference"
    KG_REFERENCE_TYPE = "kg_reference"
    ATTRIBUTE_VALUE_WITH_CUSTOM_METADATA_TYPE = "attribute_value"
    CUSTOM_METADA = "custom_metadata"
    DICT_TYPE = "dict"
    LIST_TYPE = "list"
    SIMPLE_ATV_TYPE = "simple_attribute_value"
    SIMPLE_LIST_TYPE = "simple_list"
    SIMPLE_DICT_TYPE = "simple_dict"
    DATASET_TYPE = "dataset"

    GLOBAL_WORKFLOW_EXECUTION_ID = "global"
    GLOBAL_WORKFLOW_NAME = "global_workflow"


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

    @staticmethod
    def get_status():
        return [Status.GENERATED, Status.RUNNING, Status.FINISHED, Status.ERRORED]


class ActType:

    WORKFLOW = "workflow"
    TASK = "task"
    CYCLE = "cycle"


class DataTransformationRequestType:

    GENERATE = "Generate"
    INPUT = "Input"
    OUTPUT = "Output"

class DataStores:
    RDBMS ='RDBMS'
    TRIPLESTORE = 'Triplestore'
    DOCUMENT_STORE = 'DocumentDBMS'
    OBJECT_STORE = 'ObjectStore'
    FILE_SYSTEM = 'FileSystem'
    VOLUME = 'Volume'
    GRAPH_DBMS = "GraphDBMS"

    CLOUD_OBJECT_STORE = "CloudObjectStore"
    AWSS3 = "AWSS3"
    NEO4J = "Neo4j"
    LUSTRE = "Lustre"
    GPFS = "GPFS"
    MONGODB = "MongoDB"
    POSTGRESQL = "PostgreSQL"
    JENA = "Jena"
    ALLEGROGRAPH = "AllegroGraph"

    KNOWN_DATA_STORES_TYPES = [ RDBMS, TRIPLESTORE, DOCUMENT_STORE, OBJECT_STORE, FILE_SYSTEM, GRAPH_DBMS]

    KNOWN_DATA_STORES = {
        RDBMS: [POSTGRESQL],
        TRIPLESTORE: [ALLEGROGRAPH],
        DOCUMENT_STORE: [MONGODB],
        OBJECT_STORE: [CLOUD_OBJECT_STORE, AWSS3],
        FILE_SYSTEM: [LUSTRE, GPFS]
    }

    URL = "url"
    HOST_ADDRESS = "host_address"
    BUCKET = "bucket"

    DATA_STORE_IDENTIFIERS = {
        OBJECT_STORE: [URL, BUCKET],
        RDBMS: [HOST_ADDRESS]
    }

    @staticmethod
    def get_known_data_stores():
        data_stores = list()
        for data_store_type in DataStores.KNOWN_DATA_STORES:
            data_stores.extend(DataStores.KNOWN_DATA_STORES[data_store_type])
        return data_stores

    @staticmethod
    def get_data_store_super_type(data_store):
        for data_store_super_type in DataStores.KNOWN_DATA_STORES.keys():
            if data_store in DataStores.KNOWN_DATA_STORES[data_store_super_type]:
                return data_store_super_type
        return None

class PersistenceStrategy:

    SERVICE = "SERVICE"
    FILE = "FILE"
    KAFKA = "KAFKA"


class StandardNamesAndIds:

    @staticmethod
    def get_prov_log_file_path(log_dir: str, workflow_name:str, wf_start_time: float) -> str:
        return os.path.abspath(os.path.join(log_dir, 'prov-{}-{}.log'.format(workflow_name, wf_start_time)))

    @staticmethod
    def get_id_att(attribute_name, data_schema_id=None):
        if data_schema_id:
            return data_schema_id + "_" + attribute_name
        return attribute_name

    @staticmethod
    def get_id_dataset(dte_id):
        # Here we expact that a dataset is generated in an "Data Extraction" data transformation
        return "dataset_" + dte_id

    @staticmethod
    def get_id_atv(attribute_id, value, value_type=None):
        if value_type:
            if value_type in {Vocabulary.DATA_REFERENCE_TYPE, Vocabulary.KG_REFERENCE_TYPE}:
                return attribute_id + "_" + str(value)
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
        # TODO refactor: wfe_id should be optional
        if wfe_id is None:
            wfe_id = StandardNamesAndIds.get_wfe_id(Vocabulary.GLOBAL_WORKFLOW_EXECUTION_ID)
        wfe_id = str(wfe_id)
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
    def get_data_store_hkg_id(data_store_id):
        return data_store_id

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
    def get_dataset_ctx_id(dataset_id):
        return dataset_id + "_dataset_ctx"

    @staticmethod
    def get_domain_class_id(domain_class_name):
        return domain_class_name + "_class"
        
    
    @staticmethod
    def get_domain_class_schema_id(domain_class_name):
        return domain_class_name + "_schema"

