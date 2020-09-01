from time import time

class CycleIteration:

    ACT_TYPE = "cycle"

    def __init__(self, workflow_name: str, wfexec_id: float, cycle_name: str, iteration_id, input_args: dict=None):
        """
        :param prov:
        :param cycle_name:
        :param input_args:
        :param log_dir:
        """
        self.prov_obj = {}
        self.iteration_id = iteration_id
        self.cycle_name = cycle_name
        self.input_args = input_args
        self.wfexec_id = wfexec_id
        self.workflow_name = workflow_name

    def capture_input(self):
        t0 = time()
        self.prov_obj = {
            "iteration_id": self.iteration_id,
            "startTime": t0,
            "wf_execution": self.wfexec_id
        }
        obj = {
            "cycle": self.prov_obj,
            "cycle_name": self.cycle_name,
            "type": "Input",
            "values": self.input_args
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": self.workflow_name,
            "act_type": CycleIteration.ACT_TYPE

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

        self.prov_obj["endTime"] = time()
        self.prov_obj["status"] = "FINISHED"
        self.prov_obj["iteration_id"] = self.iteration_id
        self.prov_obj["wf_execution"] = self.wfexec_id

        if stdout:
            self.prov_obj["stdout"] = stdout
        if stderr:
            self.prov_obj["stderr"] = stderr

        obj = {
            "cycle": self.prov_obj,
            "cycle_name": self.cycle_name,
            "type": "Output",
            "values": values
        }
        retrospective_json = {
            "prov_obj": obj,
            "dataflow_name": self.workflow_name,
            "act_type": CycleIteration.ACT_TYPE
        }
        return retrospective_json
