import os
import json
from time import time
from provlake import _ProvPersister
import logging
logger = logging.getLogger('PROV')


class ProvLake(object):

    def __init__(self,
                 prospective_provenance_dict_path: str=None,
                 prospective_provenance_dict: dict = None,
                 storage_configuration_path: str=None,
                 storage_configuration_dict: dict=None,
                 dataflow_name: str=None,
                 context: str=None,
                 insert_prospective=False,
                 with_validation: bool=False,
                 log_level: str='error',
                 should_log_to_file=False,
                 log_dir='.',
                 online=True,
                 service_url=None,
                 bag_size=None,
                 db_name: str=None,
                 cores=1):
        """
        :param prospective_provenance_dict_path:
        :param prospective_provenance_dict:
        :param storage_configuration_path:
        :param storage_configuration_dict:
        :param context:
        :param insert_prospective:
        :param with_validation:
        :param log_level:
        :param should_log_to_file:
        :param log_dir:
        :param online:
        :param service_url:
        :param bag_size:
        :param db_name:
        """
        if prospective_provenance_dict_path:
            with open(prospective_provenance_dict_path, 'r') as f:
                self.df_structure = json.load(f)
        elif prospective_provenance_dict:
            self.df_structure = prospective_provenance_dict
        else:
            self.df_structure = dict()

        if not online:
            assert should_log_to_file is True, "If you are using ProvLake in offline mode, " \
                                               "you need to log prov data to file. Check your 'should_log_to_file' and " \
                                               "'online' parameters."

        self.cores = cores
        self.storage_configuration_dict = None
        if storage_configuration_path:
            with open(storage_configuration_path, 'r') as f:
                self.storage_configuration_dict = json.load(f)
        elif storage_configuration_dict:
            self.storage_configuration_dict = storage_configuration_dict

        if not service_url:
            service_url = os.getenv("PROV_SERVICE_URL", "http://localhost:5000")

        if not bag_size:
            bag_size = int(os.getenv("PROV_BAG_SIZE", 1))

        self.last_task_id = 0
        self.wf_start_time = time()

        self.df_name = dataflow_name or self.df_structure.get("dataflow_name", "NI")

        self.tasks = dict()
        self.wf_execution = "wfexec_" + str(self.wf_start_time)
        self.wf_obj = {
            "wf_execution":  self.wf_execution,
            "startTime": self.wf_start_time
        }

        should_store_offline_log = False
        if should_log_to_file:
            if not os.path.exists(log_dir):
                os.makedirs(os.path.join(os.getcwd(),log_dir))
            self.filename = 'prov-{}.log'.format(self.wf_execution)
            offline_prov_log_path = os.path.join(log_dir, self.filename)
            handler = logging.FileHandler(offline_prov_log_path, mode='a+', delay=False)
            offline_prov_log = logging.getLogger("OFFLINE_PROV")
            offline_prov_log.setLevel("DEBUG")
            offline_prov_log.addHandler(handler)
            should_store_offline_log = True

        log_level = log_level.upper()
        if log_level == "NONE":
            log_level = "ERROR"
        log_lvl = getattr(logging, log_level.upper())
        #logging.getLogger().setLevel(log_lvl)
        logger.setLevel(log_lvl)

        self.prov_persister = _ProvPersister(self.df_name, service_url=service_url, context=context, bag_size=bag_size,
                                             with_validation=with_validation, db_name=db_name, online=online,
                                             should_store_offline_log=should_store_offline_log)

        if insert_prospective:
            self.insert_prospective()
        if self.storage_configuration_dict:
            self.wf_obj.update(self.storage_configuration_dict)
        self.__persist_prov(self.wf_obj, "workflow")

    def insert_prospective(self):
        self.prov_persister.persist_prospective(self.df_structure)

    def collect_in(self, dt: str, values: dict):
        '''
        :param dt: Data Transformation key string
        :param values: dict containing the expected arguments to save
        :return: true if success
        '''
        t0 = time()
        task_id = str(self.last_task_id) + "_" + str(id(self)) if self.cores > 1 else str(self.last_task_id)
        task = {
            "id": task_id,
            "startTime": t0,
            "wf_execution": self.wf_execution
        }
        self.last_task_id += 1
        self.tasks[task_id] = task
        obj = {
            "task": task,
            "dt": dt,
            "type": "Input",
            "values": values
        }
        self.__persist_prov(obj)
        return task_id

    def collect_out(self, task_id: str, dt: str, values: dict, stdout: str=None, stderr: str=None):
        '''
        :param task_id: Identifier of the task created in collect_in
        :param dt: Data Transformation key string
        :param type: i (input) or o (output)
        :param values: dict containing the expected arguments to save
        :param stdout: Optional argument for stdout msgs
        :param stderr: Optional argument for stderr msgs
        '''
        task = self.tasks[task_id]
        task["endTime"] = time()
        task["status"] = "FINISHED"

        if stdout:
            task["stdout"] = stdout
        if stderr:
            task["stderr"] = stderr

        obj = {
            "task": task,
            "dt": dt,
            "type": "Output",
            "values": values
        }
        self.__persist_prov(obj)

    def extend(self, task_id: str, dt: str, values: dict, dataset_type:str= "Input"):
        '''
        :param task_id: Identifier of the task created in collect_in
        :param dt: Data Transformation key string
        :param type: i (input) or o (output)
        :param values: dict containing the expected arguments to save
        '''
        task = self.tasks[task_id]
        obj = {
            "task": task,
            "dt": dt,
            "type": dataset_type,
            "values": values,
            "is_extension": True
        }
        self.__persist_prov(obj)

    def __persist_prov(self, obj, act_type="task"):
        if act_type == "workflow":
            func = self.prov_persister.persist_workflow
        else:
            func = self.prov_persister.persist_task
        func(obj)

    def close(self):
        wf_end_time = time()
        self.wf_obj["endTime"] = wf_end_time
        self.wf_obj["status"] = "FINISHED"
        logger.info("Waiting to get response from all submitted provenance tasks...")
        self.prov_persister.close(self.wf_obj)

        logger.info("[Prov][Done]")

    def get_dataflow_structure(self):
        return self.df_structure

    def __add_overhead(self, initial_timestamp: float):
        self.added_overheads.append(time() - initial_timestamp)
