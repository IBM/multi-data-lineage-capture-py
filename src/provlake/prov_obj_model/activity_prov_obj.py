from abc import ABCMeta, abstractmethod


class ProvRequestObj:

    __metaclass__ = ABCMeta

    def __init__(self, act_type: str, workflow_name: str, wf_exec_id: float):
        self.act_type = act_type
        self.workflow_name = workflow_name
        self.wf_exec_id = wf_exec_id

    def _as_dict(self, specific_prov_obj_dict: dict) -> dict:
        return {
            "prov_obj": specific_prov_obj_dict,
            "dataflow_name": self.workflow_name,
            "act_type": self.act_type
        }

    @abstractmethod
    def as_dict(self) -> dict:
        raise NotImplementedError
