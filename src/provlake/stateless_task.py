import numpy as np
import traceback
import logging
import uuid
from time import time
import os
from json import dumps
logger = logging.getLogger('PROV')


class StatelessTask:

    def __init__(self, wfexec_id:str, data_transformation_name: str, input_args, log_dir, context_is_managed=True):
        """
        :param prov:
        :param data_transformation_name:
        :param input_args:
        :param context_is_managed: True is the default, meaning that the context (Python's Context Manager) is being
                                   managed. Set to False if you want to use ProvTask without Context Management.
        """
        self.wfexec_id = wfexec_id
        self.task_obj = None
        self.task_id = None
        self.data_transformation_name = data_transformation_name
        self.input_args = input_args
        self.stored_output = False
        self.context_is_managed = context_is_managed
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file_path = os.path.abspath(os.path.join(log_dir, 'prov-{}.log'.format(self.wfexec_id)))
        if not self.context_is_managed:
            self.__enter__()


    def _collect_in(self):
        t0 = time()
        task_id = str(uuid.uuid4())
        self.task_id = task_id
        self.task_obj = {
            "id": task_id,
            "startTime": t0,
            "wf_execution": self.wfexec_id
        }
        obj = {
            "task": self.task_obj,
            "dt": self.data_transformation_name,
            "type": "Input",
            "values": self.input_args
        }
        with open(self.log_file_path, 'a') as f:
            f.writelines("{}\n".format(dumps([obj])))

    def _collect_out(self, values: dict, stdout: str=None, stderr: str=None):
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
        with open(self.log_file_path, 'a') as f:
            f.writelines("{}\n".format(dumps([obj])))


    def __enter__(self):
        try:
            self._collect_in()
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.data_transformation_name + " args: " + str(self.input_args))
            pass
        return self

    def __exit__(self, *args):
        try:
            if not self.stored_output:
                # There is no output, but end of task should be recorded anyway.
                self._collect_out({}, "", "")
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            pass

    def output(self, output_args=None, stdout=None, stderr=None):
        try:
            if output_args:
                self.stored_output = True
                self._collect_out(values=output_args, stdout=stdout, stderr=stderr)
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing out provenance for " + self.data_transformation_name + " args: " + str(output_args))
            pass

        if not self.context_is_managed:
            self.__exit__()
