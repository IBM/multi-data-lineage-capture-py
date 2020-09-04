from abc import ABCMeta, abstractmethod
from provlake.model.activity_prov_obj import ProvRequestObj


class Persister:

    __metaclass__ = ABCMeta

    def __init__(self, workflow_name: str, wf_start_time: float):
        self._workflow_name = workflow_name
        self._wf_start_time = wf_start_time

    @abstractmethod
    def add_request(self, persistence_request_obj: ProvRequestObj):
        raise NotImplementedError

    def _close(self):
        pass

    def get_wf_start_time(self) -> float:
        return self._wf_start_time

    def get_workflow_name(self) -> str:
        return self._workflow_name

    def get_wf_exec_id(self) -> float:
        return self.get_wf_start_time()
