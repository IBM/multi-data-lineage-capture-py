from provlake.persistence.persister import Persister
from provlake.capture.activity_capture import ActivityCapture
from provlake.model.workflow_prov_obj import WorkflowProvRequestObj
from provlake.model.task_prov_obj import TaskProvRequestObj
from provlake.model.cycle_prov_obj import CycleProvRequestObj
from provlake.utils.constants import Status, DataTransformationRequestType
import traceback
import logging
from time import time

logger = logging.getLogger('PROV')


class ProvWorkflow(ActivityCapture):

    def __init__(self, prov_persister: Persister):
        super().__init__(prov_persister)
        if self._prov_persister is None:
            return
        self.stored_output = False

    def begin(self) -> 'ProvWorkflow':
        if self._prov_persister is None:
            return None
        try:
            prov_obj = WorkflowProvRequestObj(
                wf_exec_id=self._prov_persister.get_wf_exec_id(),
                workflow_name=self._prov_persister.get_workflow_name(),
                start_time=self._prov_persister.get_wf_start_time(),
                status=Status.RUNNING
            )
            self._prov_persister.add_request(prov_obj)
            return self
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for begin workflow")
            return None

    def end(self, output_args: dict=None, stdout=None, stderr=None) -> 'ProvWorkflow':
        if self._prov_persister is None:
            return None
        try:
            self.stored_output = True
            prov_obj = WorkflowProvRequestObj(
                wf_exec_id=self._prov_persister.get_wf_exec_id(),
                workflow_name=self._prov_persister.get_workflow_name(),
                start_time=self._prov_persister.get_wf_start_time(),
                end_time=time(),
                status=Status.FINISHED
            )
            if output_args:
                prov_obj.values = output_args
            if stdout:
                prov_obj.stdout = stdout
            if stderr:
                prov_obj.stderr = stderr

            self._prov_persister.add_request(prov_obj)
            self._prov_persister._close()

            return self
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing provenance for end workflow")
            return None

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, *args):
        if self._prov_persister and not self.stored_output:
            # There is no output, but end of task should be recorded anyway.
            self.end({}, "", "")


class ProvTask(ActivityCapture):

    def __init__(self, prov_persister: Persister, data_transformation_name: str, input_args: dict,
                 parent_cycle_name: str = None, parent_cycle_iteration=None, person_id: str = None, task_id=None):
        super().__init__(prov_persister)
        if self._prov_persister is None:
            return

        self.stored_output = False
        self.prov_obj = TaskProvRequestObj(dt_name=data_transformation_name,
                                           type_=DataTransformationRequestType.INPUT,
                                           wf_exec_id=prov_persister.get_wf_exec_id(),
                                           workflow_name=prov_persister.get_workflow_name(),
                                           values=input_args,
                                           status=Status.CREATED,
                                           parent_cycle_name=parent_cycle_name,
                                           parent_cycle_iteration=parent_cycle_iteration,
                                           person_id=person_id,
                                           task_id=task_id
                                           )

    def begin(self) -> TaskProvRequestObj:
        if self._prov_persister is None:
            return None
        try:
            start_time = time()

            if not self.prov_obj.task_id:
                self.prov_obj.task_id = start_time

            self.prov_obj.start_time = start_time
            self.prov_obj.status = Status.RUNNING
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.prov_obj.dt_name + " args: " + str(self.prov_obj.values))
            return None

    def end(self, output_args: dict=None, stdout=None, stderr=None) -> TaskProvRequestObj:
        if self._prov_persister is None:
            return None
        try:
            if output_args:
                self.stored_output = True

                self.prov_obj.end_time = time()
                self.prov_obj.status = Status.FINISHED
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
                         self.prov_obj.dt_name + " args: " + str(self.prov_obj.values))
            return None

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, *args):
        if self._prov_persister and not self.stored_output:
            # There is no output, but end of task should be recorded anyway.
            self.end({}, "", "")

    # def extend_input(self, args: dict):
    #     try:
    #         if self.prov:
    #             self.__extend(args, "Input")
    #     except Exception as e:
    #         traceback.print_exc()
    #         logger.error(str(e))
    #         logger.error("Error storing ext provenance for " +
    #                      self.data_transformation_name + " args: " + str(args))
    #         pass
    #
    # def extend_output(self, args: dict):
    #     try:
    #         self.__extend(args, "Output")
    #     except Exception as e:
    #         traceback.print_exc()
    #         logger.error(str(e))
    #         logger.error("Error storing ext provenance for " +
    #                      self.data_transformation_name + " args: " + str(args))
    #         pass
    #
    # def __extend(self, args: dict, dataset_type="Input"):
    #     self.prov.extend(self.task_id, self.data_transformation_name, values=args, dataset_type=dataset_type)



class ProvCycle(ActivityCapture):

    def __init__(self, prov_persister: Persister, cycle_name: str, iteration_id,
                 input_args: dict=None):
        super().__init__(prov_persister)
        if self._prov_persister is None:
            return
        self.stored_output = False
        self.prov_obj = CycleProvRequestObj(cycle_name=cycle_name,
                                            type_=DataTransformationRequestType.INPUT,
                                            wf_exec_id=prov_persister.get_wf_exec_id(),
                                            workflow_name=prov_persister.get_workflow_name(),
                                            values=input_args,
                                            iteration_id=iteration_id,
                                            status=Status.CREATED)

    def begin(self) -> CycleProvRequestObj:
        if self._prov_persister is None:
            return
        try:
            start_time = time()
            self.prov_obj.task_id = start_time
            self.prov_obj.start_time = start_time
            self.prov_obj.status = Status.RUNNING
            self._prov_persister.add_request(self.prov_obj)
            return self.prov_obj
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.prov_obj.cycle_name + " args: " + str(self.prov_obj.values))
            return None

    def end(self, output_args: dict=None, stdout=None, stderr=None) -> CycleProvRequestObj:
        if self._prov_persister is None:
            return
        try:
            if output_args:
                self.stored_output = True

                self.prov_obj.end_time = time()
                self.prov_obj.status = Status.FINISHED
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
            self.end({}, "", "")
