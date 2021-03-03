import os


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
