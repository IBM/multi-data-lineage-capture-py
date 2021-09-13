from typing import Optional

from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import Vocabulary


class CycleProvRequestObj(ProvRequestObj):
    ACT_TYPE = "cycle"
    INPUT_TYPE = "Input"
    OUTPUT_TYPE = "Output"

    def __init__(self, cycle_name: str, type_: str, iteration_id, workflow_name: Optional[str] = None,
                 wf_exec_id: Optional = None,
                 values: dict = None, start_time: float = None, end_time: float = None,
                 stdout: str = None, stderr: str = None, status: str = None, custom_metadata: dict = None):
        super().__init__(CycleProvRequestObj.ACT_TYPE, workflow_name, wf_exec_id, custom_metadata)
        assert type_ in [CycleProvRequestObj.INPUT_TYPE, CycleProvRequestObj.OUTPUT_TYPE]
        self.cycle_name = cycle_name
        self.type_ = type_
        self.values = values
        self.iteration_id = iteration_id
        self.start_time = start_time
        self.end_time = end_time
        self.stdout = stdout
        self.stderr = stderr
        self.status = status
        if not values:
            self.values = dict()

    def as_dict(self):
        cycle_dict = {
            "iteration_id": self.iteration_id,
            "wf_execution": self.wf_exec_id
        }
        if self.start_time:
            cycle_dict["startTime"] = self.start_time
        if self.end_time:
            cycle_dict["endTime"] = self.end_time
        if self.stdout:
            cycle_dict[Vocabulary.STDOUT] = self.stdout
        if self.stderr:
            cycle_dict[Vocabulary.STDERR] = self.stderr
        if self.status:
            cycle_dict[Vocabulary.STATUS] = self.status

        ret_dict = {
            "cycle": cycle_dict,
            "cycle_name": self.cycle_name,
            "type": self.type_,
            "values": self.values
        }
        return super()._inject_prov_request_args(ret_dict)
