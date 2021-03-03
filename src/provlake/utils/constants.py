import os
from .prov_utils import convert_timestamp, id_hash

class Vocabulary:

    START_TIME = "startTime"
    END_TIME = "endTime"
    GENERATED_TIME = "generatedTime"
    STATUS = "status"
    VALUES = "values"
    STDOUT = "stdout"
    STDERR = "stderr"
    PERSON = "person"
    PARENT_CYCLE_ITERATION = "parent_cycle_iteration"
    PARENT_CYLE_NAME = "parent_cycle_name"


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
    def get_id_atv(attribute, value):
        if type(value) in [dict, list]:
            return attribute + "_" + id_hash(str(value))
        else:
            # TODO if its a float, replace the dots
            return attribute + "_" + str(value)

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
    def get_dte_id(wfe_id, dt_name:str , prov_task: dict):
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
            dte_id = wfe_id + "_" + dt_name + "_" + task_id

        return dte_id
