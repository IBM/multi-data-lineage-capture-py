from provlake.persistence.persister import Persister
from provlake.capture.activity_capture import ActivityCapture
from provlake.model.workflow_prov_obj import WorkflowProvRequestObj
from provlake.model.task_prov_obj import TaskProvRequestObj
from provlake.model.cycle_prov_obj import CycleProvRequestObj
from provlake.utils.constants import Status, DataTransformationRequestType, StandardNamesAndIds
import traceback
import logging
from typing import Optional, Dict
from time import time

logger = logging.getLogger('PROV')


class ProvWorkflow(ActivityCapture):

    def __init__(self, prov_persister: Persister, workflow_name, custom_metadata: dict = None,
                 wf_exec_id=None, wf_start_time: float = None):
        super().__init__(prov_persister, custom_metadata)
        if self._prov_persister is None:
            return

        self.workflow_name = workflow_name

        if wf_start_time is None:
            self.wf_start_time = time()
        else:
            self.wf_start_time = wf_start_time

        if wf_exec_id is None:
            self.wf_exec_id = self.wf_start_time
        else:
            self.wf_exec_id = wf_exec_id

        self.stored_output = False

    def begin(self) -> 'ProvWorkflow':
        if self._prov_persister is None:
            return None
        try:
            prov_obj = WorkflowProvRequestObj(
                wf_exec_id=self.wf_exec_id,
                workflow_name=self.workflow_name,
                start_time=self.wf_start_time,
                #status=Status.RUNNING,
                custom_metadata=self.get_custom_metadata()
            )
            self._prov_persister.add_request(prov_obj)
            return self
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for begin workflow")
            return None

    def end(self, output_args=dict(), stdout=None, stderr=None, end_time=None) -> 'ProvWorkflow':
        if self._prov_persister is None:
            return None
        try:
            self.stored_output = True
            prov_obj = WorkflowProvRequestObj(
                wf_exec_id=self.wf_exec_id,
                workflow_name=self.workflow_name,
                start_time=self.wf_start_time,
                end_time=end_time if end_time is not None else time(),
                status=Status.FINISHED
            )
            if output_args:
                prov_obj.values = output_args
            if stdout:
                prov_obj.stdout = stdout
            if stderr:
                prov_obj.stderr = stderr

            self._prov_persister.add_request(prov_obj)
            self._prov_persister.close()

            return self
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing provenance for end workflow")
            return None

    def get_workflow_execution_id(self):
        return StandardNamesAndIds.get_wfe_id(self.workflow_name, self.wf_exec_id)

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, *args):
        if self._prov_persister and not self.stored_output:
            # There is no output, but end of task should be recorded anyway.
            self.end()


class ProvTask(ActivityCapture):
    def __init__(self, prov_persister: Persister, data_transformation_name: str,
                 prov_workflow: Optional[ProvWorkflow] = None,
                 input_args: Optional[Dict] = None, parent_cycle_name: str = None, parent_cycle_iteration=None,
                 person_id: str = None,
                 task_id=None, custom_metadata: dict = None, attribute_associations: dict = None,
                 generated_time: float = None):
        super().__init__(prov_persister, custom_metadata, generated_time)
        if self._prov_persister is None:
            return

        if input_args is None:
            input_args = dict()

        self.stored_output = False
        self._input_args = input_args
        self.task_id = task_id
        if self.task_id is None:
            self.task_id = self.generated_time

        self.prov_obj = TaskProvRequestObj(dt_name=data_transformation_name,
                                           type_=DataTransformationRequestType.GENERATE,
                                           wf_exec_id=None if prov_workflow is None else prov_workflow.wf_exec_id,
                                           workflow_name=None if prov_workflow is None else prov_workflow.workflow_name,
                                           generated_time=self.generated_time,
                                           parent_cycle_name=parent_cycle_name,
                                           parent_cycle_iteration=parent_cycle_iteration,
                                           person_id=person_id,
                                           task_id=self.task_id,
                                           custom_metadata=self.get_custom_metadata(),
                                           attribute_associations=attribute_associations,
                                           values=self._input_args
                                           )

        self._prov_persister.add_request(self.prov_obj)

    def get_data_transformation_execution_id(self):
        # TODO refactor
        return StandardNamesAndIds.get_dte_id(self.prov_obj.wf_exec_id, self.prov_obj.dt_name, self.prov_obj.as_dict()["prov_obj"]["task"])

    def begin(self, start_time: float = None) -> TaskProvRequestObj:
        if self._prov_persister is None:
            return None
        try:
            # self.prov_obj.values = self._input_args
            # ensuring the running request does not repeat the input_args
            self.prov_obj.values = None

            self.prov_obj.type_ = DataTransformationRequestType.INPUT
            self.prov_obj.start_time = start_time if start_time is not None else time()
            #self.prov_obj.status = Status.RUNNING
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.prov_obj.dt_name + " args: " + str(self.prov_obj.values))
            return None

    def end(self, output_args=dict(), stdout=None, stderr=None, end_time: float = None, status=Status.FINISHED,
            attribute_associations: dict = None) -> TaskProvRequestObj:
        if self._prov_persister is None:
            return None
        try:
            self.stored_output = True
            self.prov_obj.end_time = end_time if end_time is not None else time()
            self.prov_obj.status = status
            self.prov_obj.values = output_args
            self.prov_obj.type_ = DataTransformationRequestType.OUTPUT
            self.prov_obj.stderr = stderr
            self.prov_obj.stdout = stdout
            self.prov_obj.attribute_associations = attribute_associations
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing provenance for " +
                         self.prov_obj.dt_name + " args: " + str(self.prov_obj.values))
            return None

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, *args):
        if self._prov_persister and not self.stored_output:
            # There is no output, but end of task should be recorded anyway.
            self.end()


class ProvCycle(ActivityCapture):

    def __init__(self, prov_persister: Persister, cycle_name: str, iteration_id,
                 prov_workflow: Optional[ProvWorkflow] = None, input_args: Optional[Dict] = None,
                 custom_metadata: dict = None):
        super().__init__(prov_persister, custom_metadata)
        if self._prov_persister is None:
            return

        if input_args is None:
            input_args = dict()
        self.stored_output = False
        self.prov_obj = CycleProvRequestObj(cycle_name=cycle_name,
                                            type_=DataTransformationRequestType.INPUT,
                                            wf_exec_id=None if prov_workflow is None else prov_workflow.wf_exec_id,
                                            workflow_name=None if prov_workflow is None else prov_workflow.workflow_name,
                                            values=input_args,
                                            iteration_id=iteration_id,
                                            status=Status.GENERATED,
                                            custom_metadata=self.get_custom_metadata()
                                            )

    def begin(self, start_time: float = None) -> CycleProvRequestObj:
        if self._prov_persister is None:
            return
        try:
            start_time = time()
            self.prov_obj.task_id = start_time
            self.prov_obj.start_time = start_time if start_time is not None else time()
            self.prov_obj.status = Status.RUNNING
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.prov_obj.cycle_name + " args: " + str(self.prov_obj.values))
            return None

    def end(self, output_args=dict(), stdout=None, stderr=None, end_time: float = None, status=Status.FINISHED) -> \
            CycleProvRequestObj:
        if self._prov_persister is None:
            return
        try:
            self.stored_output = True
            self.prov_obj.end_time = end_time if end_time is not None else time()
            self.prov_obj.status = status
            self.prov_obj.values = output_args
            self.prov_obj.type_ = DataTransformationRequestType.OUTPUT
            self.prov_obj.stderr = stderr
            self.prov_obj.stdout = stdout
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing provenance for " +
                         self.prov_obj.cycle_name + " args: " + str(self.prov_obj.values))
            pass

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, *args):
        if self._prov_persister and not self.stored_output:
            # There is no output, but end of cycle should be recorded anyway.
            self.end()
