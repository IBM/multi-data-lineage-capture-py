from abc import ABCMeta, abstractmethod
from typing import Optional

from provlake.utils.constants import Vocabulary, Status


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

    def check_status_properties(self, status=None, generated_time=None, start_time=None, end_time=None):
        result = {
            "message": "Error on status or generate, start and end times according status properties filling rule.",
            "check_result": False
        }
        if not generated_time and not start_time and not end_time:
            result["message"] = "One of generate or start or end time has to be filled."
            return result
        if end_time:
            if not status:
                result["message"] = \
                    f"If end time is filled, status has to be filled and with {Status.FINISHED} or {Status.RUNNING}."
                return result
            if status not in [Status.FINISHED, Status.ERRORED]:
                result["message"] = \
                    f"If end time is filled, status has to be filled with {Status.FINISHED} or {Status.RUNNING}."
                return result
            # generated_time and start_time are optional
            result["message"] = f"{status} status passed"
            result["check_result"] = True
            return result
        if generated_time:
            if start_time or end_time:
                result["message"] = \
                    f"If generated time is filled , start and end times should be not filled"
            # status is optional, but if status comes, it has to be GENERATED
            if status and status != Status.GENERATED:
                result["message"] = \
                    f"If generated time is filled and status is filled then status has to be {status.GENERATED}"
                return result
            result["message"] = f"{Status.GENERATED} status passed"
            result["check_result"] = True
            return result
        if start_time:
            if end_time:
                result["message"] = \
                    f"If start time is filled, end time cannot be filled"
                return result
            # status and generated_time are optional, but if status comes, it has to be RUNNING
            if status and status != Status.RUNNING:
                result["message"] = \
                    f"If start time is filled and status is filled then status has to be {Status.RUNNING}"
                return result
            result["message"] = f"{Status.RUNNING} status passed"
            result["check_result"] = True
            return result
        return result



