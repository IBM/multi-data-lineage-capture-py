import traceback
import logging
from time import time
from provlake.utils import prov_utils

logger = logging.getLogger('PROV')


class Task:

    ACT_TYPE = "task"

    def __init__(self, workflow_name: str, wfexec_id: int, data_transformation_name: str, input_args,
                 parent_cycle_name: str=None, parent_cycle_iteration=None):
        """
        :param prov:
        :param data_transformation_name:
        :param input_args:
        :param context_is_managed: True is the default, meaning that the context (Python's Context Manager) is being
                                   managed. Set to False if you want to use ProvTask without Context Management.
        """
        self.task_obj = None
        self.task_id = None
        self.data_transformation_name = data_transformation_name
        self.input_args = input_args
        self.stored_output = False
        self.parent_cycle_name = parent_cycle_name
        self.parent_cycle_iteration = parent_cycle_iteration
        self.wfexec_id = wfexec_id
        self.workflow_name = workflow_name

    def capture_input(self):
        t0 = time()
        self.task_id = t0
        self.task_obj = {
            "id": self.task_id,
            "startTime": t0,
            "wf_execution": self.wfexec_id
        }
        if self.parent_cycle_iteration:
            self.task_obj["parent_cycle_iteration"] = self.parent_cycle_iteration
        if self.parent_cycle_name:
            self.task_obj["parent_cycle_name"] = self.parent_cycle_name
        obj = {
            "task": self.task_obj,
            "dt": self.data_transformation_name,
            "type": "Input",
            "values": self.input_args
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": self.workflow_name,
            "act_type": Task.ACT_TYPE

        }
        return retrospective_json

    def capture_output(self, values: dict, stdout: str = None, stderr: str = None):
        '''
        :param task_id: Identifier of the task created in collect_in
        :param dt: Data Transformation key string
        :param type: i (input) or o (output)
        :param values: dict containing the expected arguments to save
        :param stdout: Optional argument for stdout msgs
        :param stderr: Optional argument for stderr msgs
        '''
        self.task_obj["endTime"] = time()
        self.task_obj["status"] = "FINISHED"

        if stdout:
            self.task_obj["stdout"] = stdout
        if stderr:
            self.task_obj["stderr"] = stderr

        obj = {
            "task": self.task_obj,
            "dt": self.data_transformation_name,
            "type": "Output",
            "values": values
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": self.workflow_name,
            "act_type": Task.ACT_TYPE
        }
        return retrospective_json
