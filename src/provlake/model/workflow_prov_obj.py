from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import Vocabulary, ActType


class WorkflowProvRequestObj(ProvRequestObj):


    def __init__(self, wf_exec_id, workflow_name: str, start_time: float=None, end_time: float = None,
                 status: str = None, values: dict = None, stdout: str = None, stderr: str = None,
                 custom_metadata:dict=None, generated_time: float=None):
        super().__init__(ActType.WORKFLOW, workflow_name, wf_exec_id, custom_metadata)
        self.start_time = start_time
        self.end_time = end_time
        self.generated_time = generated_time
        self.status = status
        self.values = values
        self.stdout = stdout
        self.stderr = stderr

    def as_dict(self) -> dict:
        ret_dict = {
            "wf_execution": self.wf_exec_id,
        }
        if self.generated_time:
            ret_dict[Vocabulary.GENERATED_TIME] = self.generated_time
        if self.start_time:
            ret_dict[Vocabulary.START_TIME] = self.start_time
        if self.end_time:
            ret_dict[Vocabulary.END_TIME] = self.end_time
        if self.status:
            ret_dict[Vocabulary.STATUS] = self.status
        if self.values:
            ret_dict[Vocabulary.VALUES] = self.values
        if self.stdout:
            ret_dict[Vocabulary.STDOUT] = self.stdout
        if self.stderr:
            ret_dict[Vocabulary.STDERR] = self.stderr
        return super()._inject_prov_request_args(ret_dict)


