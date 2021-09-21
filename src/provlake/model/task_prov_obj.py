from typing import Optional

from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import Vocabulary, ActType, DataTransformationRequestType, Status


class TaskProvRequestObj(ProvRequestObj):

    def __init__(self, dt_name: str, type_: str, workflow_name: Optional[str] = None, wf_exec_id: Optional = None,
                 task_id=None,
                 person_id: str = None, values: dict = None,
                 generated_time: float = None, start_time: float = None, end_time: float = None,
                 parent_cycle_iteration=None, parent_cycle_name=None, status: str = None,
                 stdout: str = None, stderr: str = None, custom_metadata: dict = None,
                 attribute_associations: dict = None):
        super().__init__(ActType.TASK, workflow_name, wf_exec_id, custom_metadata)
        assert type_ in [DataTransformationRequestType.INPUT,
                         DataTransformationRequestType.OUTPUT,
                         DataTransformationRequestType.GENERATE]
        if status:
            assert isinstance(status, str)
            assert status in Status.get_status()
        self.dt_name = dt_name
        self.person_id = person_id
        self.type_ = type_
        self.values = values
        self.task_id = task_id
        self.start_time = start_time
        self.end_time = end_time
        self.generated_time = generated_time
        self.parent_cycle_name = parent_cycle_name
        self.parent_cycle_iteration = parent_cycle_iteration
        self.stdout = stdout
        self.stderr = stderr
        self.status = status
        self.attribute_associations = attribute_associations
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
        if self.generated_time:
            task_dict[Vocabulary.GENERATED_TIME] = self.generated_time
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
        if self.attribute_associations:
            task_dict[Vocabulary.ATTRIBUTE_ASSOCIATIONS] = self.attribute_associations

        ret_dict = {
            "task": task_dict,
            "dt": self.dt_name,
            "type": self.type_
        }
        if self.values:
            ret_dict["values"] = self.values
        if self.person_id:
            ret_dict[Vocabulary.PERSON] = self.person_id
        return super()._inject_prov_request_args(ret_dict)
