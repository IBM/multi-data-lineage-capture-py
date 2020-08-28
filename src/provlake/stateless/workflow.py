from time import time
from provlake.utils import prov_utils
import os


class Workflow:

    ACT_TYPE = "workflow"

    @staticmethod
    def begin(workflow_name: str, log_dir: str) -> float:
        wf_start_time = time()
        obj = {
            "wf_execution": wf_start_time,
            "startTime": wf_start_time
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": workflow_name,
            "act_type": Workflow.ACT_TYPE
        }
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        prov_utils.append_log(retrospective_json, log_dir, workflow_name, str(wf_start_time))
        return wf_start_time

    @staticmethod
    def end(workflow_name: str, wf_start_time: float, log_dir: str):
        obj = {
            "wf_execution": wf_start_time,
            "starTime": wf_start_time,
            "endTime": time(),
            "status": "FINISHED"
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": workflow_name,
            "act_type": Workflow.ACT_TYPE
        }
        prov_utils.append_log(retrospective_json, log_dir, workflow_name, str(wf_start_time))
