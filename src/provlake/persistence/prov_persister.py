# def collect_in(self, dt: str, values: dict):
#     '''
#     :param dt: Data Transformation key string
#     :param values: dict containing the expected arguments to save
#     :return: true if success
#     '''
#     t0 = time()
#     task_id = str(self.last_task_id) + "_" + str(id(self)) if self.cores > 1 else str(self.last_task_id)
#     task = {
#         "id": task_id,
#         "startTime": t0,
#         "wf_execution": self.wf_execution
#     }
#     self.last_task_id += 1
#     self.tasks[task_id] = task
#     obj = {
#         "task": task,
#         "dt": dt,
#         "type": "Input",
#         "values": values
#     }
#     self.__persist_prov(obj)
#     return task_id
#
# def collect_out(self, task_id: str, dt: str, values: dict, stdout: str=None, stderr: str=None):
#     '''
#     :param task_id: Identifier of the task created in collect_in
#     :param dt: Data Transformation key string
#     :param type: i (input) or o (output)
#     :param values: dict containing the expected arguments to save
#     :param stdout: Optional argument for stdout msgs
#     :param stderr: Optional argument for stderr msgs
#     '''
#     task = self.tasks[task_id]
#     task["endTime"] = time()
#     task["status"] = "FINISHED"
#
#     if stdout:
#         task["stdout"] = stdout
#     if stderr:
#         task["stderr"] = stderr
#
#     obj = {
#         "task": task,
#         "dt": dt,
#         "type": "Output",
#         "values": values
#     }
#     self.__persist_prov(obj)
#
# def extend(self, task_id: str, dt: str, values: dict, dataset_type:str= "Input"):
#     '''
#     :param task_id: Identifier of the task created in collect_in
#     :param dt: Data Transformation key string
#     :param type: i (input) or o (output)
#     :param values: dict containing the expected arguments to save
#     '''
#     task = self.tasks[task_id]
#     obj = {
#         "task": task,
#         "dt": dt,
#         "type": dataset_type,
#         "values": values,
#         "is_extension": True
#     }
#     self.__persist_prov(obj)
#
# def __persist_prov(self, obj, act_type="task"):
#     if act_type == "workflow":
#         func = self.prov_persister.persist_workflow
#     else:
#         func = self.prov_persister.persist_task
#     func(obj)

