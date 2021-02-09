from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import Vocabulary, ActType, DataTransformationRequestType


class TaskProvRequestObj(ProvRequestObj):

    def __init__(self, dt_name: str, type_: str, workflow_name: str, wf_exec_id: float, task_id: float=None,
                 person_id: str = None, values: dict = None, start_time: float = None, end_time: float = None,
                 parent_cycle_iteration=None, parent_cycle_name=None, status: str=None,
                 stdout: str=None, stderr: str=None):
        super().__init__(ActType.TASK, workflow_name, wf_exec_id)
        assert type_ in [DataTransformationRequestType.INPUT, DataTransformationRequestType.OUTPUT]
        self.dt_name = dt_name
        self.person_id = person_id
        self.type_ = type_
        self.values = values
        self.task_id = task_id
        self.start_time = start_time
        self.end_time = end_time
        self.parent_cycle_name = parent_cycle_name
        self.parent_cycle_iteration = parent_cycle_iteration
        self.stdout = stdout
        self.stderr = stderr
        self.status = status
        if not values:
            self.values = dict()

    def as_dict(self) -> dict:
        # TODO: finish using constants here
        task_dict = {
            "id": self.task_id,
            "wf_execution": self.wf_exec_id
        }
        if self.start_time:
            task_dict[Vocabulary.START_TIME] = self.start_time
        if self.end_time:
            task_dict[Vocabulary.END_TIME] = self.end_time
        if self.parent_cycle_iteration:
            task_dict[Vocabulary.PARENT_CYCLE_ITERATION] = self.parent_cycle_iteration
        if self.parent_cycle_name:
            task_dict[Vocabulary.PARENT_CYLE_NAME] = self.parent_cycle_name
        if self.stdout:
            task_dict[Vocabulary.STDOUT] = self.stdout
        if self.stderr:
            task_dict[Vocabulary.STDERR] = self.stderr
        if self.status:
            task_dict[Vocabulary.STATUS] = self.status
        ret_dict = {
            "task": task_dict,
            "dt": self.dt_name,
            "type": self.type_,
            "values": self.values
        }
        if self.person_id:
            ret_dict[Vocabulary.PERSON] = self.person_id
        return super()._inject_prov_request_args(ret_dict)
