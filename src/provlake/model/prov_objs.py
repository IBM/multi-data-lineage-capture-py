
class ProvObj:

    def __init__(self, act_type: str):
        pass


    def as_dict(self) -> dict:
        return {}



class TaskObj:

    def __init__(self, task_id, wf_exec_id, start_time=None, end_time=None, parent_cycle_iteration=None, parent_cycle_name=None):
        self.task_id = task_id
        self.start_time = start_time
        self.end_time = end_time
        self.wf_exec_id = wf_exec_id
        self.parent_cycle_iteration = parent_cycle_iteration
        self.parent_cycle_name = parent_cycle_name

    def as_dict(self):
        ret_dict = {
            "id": self.task_id,
            "wf_execution": self.wf_exec_id
        }
        if self.start_time:
            ret_dict["startTime"] = self.start_time
        if self.end_time:
            ret_dict["endTime"] = self.end_time
        if self.parent_cycle_iteration:
            ret_dict["parent_cycle_iteration"] = self.parent_cycle_iteration
        if self.parent_cycle_name:
            ret_dict["parent_cycle_name"] = self.parent_cycle_name
        return ret_dict


class TaskProvObj:

    def __init__(self, dt_name:str, type_:str, values: dict, task_obj: TaskObj):
        self.dt_name = dt_name
        self.type_ = type_
        self.values = values
        self.task_obj = task_obj

    def as_dict(self):
        return {
            "task": self.task_obj.as_dict(),
            "dt": self.dt_name,
            "type": self.type_,
            "values": self.values
        }


class RetrospectiveProv:

    def __init__(self, prov_obj: ProvObj, workflow_name: str, act_type: str):
        self.prov_obj = prov_obj
        self.dataflow_name = workflow_name
        self.act_type = act_type

    def as_dict(self):
        return {
            "prov_obj": self.prov_obj.as_dict(),
            "dataflow_name": self.dataflow_name,
            "act_type": self.act_type
        }
