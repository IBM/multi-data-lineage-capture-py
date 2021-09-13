from abc import ABCMeta, abstractmethod
from typing import Optional

from provlake.utils.constants import Vocabulary


class ProvRequestObj:
    __metaclass__ = ABCMeta

    def __init__(self, act_type: str, workflow_name: Optional[str] = None, wf_exec_id: Optional = None,
                 custom_metadata: dict = None):
        self.act_type = act_type
        self.workflow_name = workflow_name
        self.wf_exec_id = wf_exec_id
        self.custom_metadata = custom_metadata

    def _inject_prov_request_args(self, specific_prov_obj_dict: dict) -> dict:
        ret = dict()
        if self.custom_metadata:
            ret[Vocabulary.CUSTOM_METADATA] = self.custom_metadata
        ret.update({
            Vocabulary.PROV_OBJ: specific_prov_obj_dict,
            Vocabulary.WORKFLOW_NAME: self.workflow_name,
            Vocabulary.ACT_TYPE: self.act_type,
        })
        return ret

    @abstractmethod
    def as_dict(self) -> dict:
        raise NotImplementedError
