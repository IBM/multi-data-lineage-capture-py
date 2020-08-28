import traceback
import logging
from time import time
from provlake.utils import prov_utils

logger = logging.getLogger('PROV')


class CycleIteration:

    ACT_TYPE = "cycle"

    def __init__(self, workflow_name: str, wfexec_id: str, cycle_name: str,
                 iteration_id, log_dir, input_args=None):
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
        self.stored_output = False
        self.log_dir = log_dir
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
        prov_utils.append_log(retrospective_json, self.log_dir, self.workflow_name, self.wfexec_id)
        return self

    def _capture_output(self, values: dict, stdout: str = None, stderr: str = None):
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
        prov_utils.append_log(retrospective_json, self.log_dir, self.workflow_name, self.wfexec_id)

    def __enter__(self):
        try:
            self.capture_input()
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.cycle_name + " args: " + str(self.input_args))
            pass
        return self

    def __exit__(self, *args):
        if not self.stored_output:
            self._capture_output({}, "", "")

    def capture_output(self, output_args=None, stdout=None, stderr=None):
        self.stored_output = True
        try:
            if output_args:
                self._capture_output(values=output_args, stdout=stdout, stderr=stderr)
            else:
                self._capture_output({}, "", "")
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error(
                "Error storing out provenance for " + self.cycle_name + " args: " + str(output_args))
            pass

