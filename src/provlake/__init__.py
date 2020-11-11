from provlake.persistence.persister import Persister
from provlake.persistence.unmanaged_persister import UnmanagedPersister
from provlake.persistence.managed_persister import ManagedPersister
from provlake.capture import ProvWorkflow
from provlake.utils.constants import StandardNamesAndIds
from time import time
import os
import logging

logger = logging.getLogger('PROV')


class ProvLake:

    @staticmethod
    def _build_managed_persister(
            workflow_name: str,
            log_dir: str,
            wf_start_time: float,
            service_url: str,
            should_send_to_file: bool,
            bag_size: int,
            cores: int,
            context: str,
            with_validation: bool,
            db_name: str
    ) -> ManagedPersister:

        should_send_to_service = False
        if service_url:
            should_send_to_service = True

        if not bag_size:
            bag_size = int(os.getenv("PROV_BAG_SIZE", 1))

        #self.last_task_id = 0
        #task_id = str(self.last_task_id) + "_" + str(id(self)) if self.cores > 1 else str(self.last_task_id)
        #self.tasks = dict()

        if not should_send_to_service:
            assert should_send_to_file is True, "If you are using ProvLake in offline mode, " \
                                               "you need to log prov data to file. Check your 'should_send_to_file' and " \
                                               "'should_send_to_service' parameters."
        if should_send_to_file:
            if not os.path.exists(log_dir):
                os.makedirs(os.path.join(os.getcwd(), log_dir))

            offline_prov_log_path = StandardNamesAndIds.get_prov_log_file_path(log_dir, workflow_name, wf_start_time)
            handler = logging.FileHandler(offline_prov_log_path, mode='a+', delay=False)
            offline_prov_log = logging.getLogger("OFFLINE_PROV")
            offline_prov_log.setLevel("DEBUG")
            offline_prov_log.addHandler(handler)
            #should_send_to_file = True

        return ManagedPersister(
            workflow_name=workflow_name,
            wf_start_time=wf_start_time,
            service_url=service_url,
            context=context,
            with_validation=with_validation,
            db_name=db_name,
            bag_size=bag_size,
            should_send_to_file=should_send_to_file,
            should_send_to_service=should_send_to_service)

    @staticmethod
    def _build_unmanaged_persister(workflow_name: str, log_dir: str, wf_start_time: float=None) -> UnmanagedPersister:
        return UnmanagedPersister(workflow_name, wf_start_time, log_dir)

    @staticmethod
    def get_persister(
            workflow_name: str,
            wf_start_time: float = None,
            managed_persistence=True,
            context: str = None,
            with_validation: bool = False,
            log_level: str = 'error',
            should_send_to_file=True,
            log_dir='.',
            service_url=None,
            bag_size=None,
            db_name: str = None,
            cores=1
    ) -> Persister:

        assert workflow_name is not None, "Please inform a name for this workflow."
        if not wf_start_time:
            wf_start_time = time()
        ProvLake._set_log_lvl(log_level)

        if managed_persistence:
            persister = ProvLake._build_managed_persister(
                workflow_name=workflow_name,
                log_dir=log_dir,
                wf_start_time=wf_start_time,
                service_url=service_url,
                should_send_to_file=should_send_to_file,
                bag_size=bag_size,
                cores=cores,
                context=context,
                with_validation=with_validation,
                db_name=db_name
            )

        else:
            persister = ProvLake._build_unmanaged_persister(
                workflow_name=workflow_name,
                log_dir=log_dir,
                wf_start_time=wf_start_time
            )

        return persister

    @staticmethod
    def _set_log_lvl(log_level):
        log_level = log_level.upper()
        if log_level == "NONE":
            log_level = "ERROR"
        log_lvl = getattr(logging, log_level.upper())
        logger.setLevel(log_lvl)

    # TODO revise these commented out lines below
    # def storage_configuration(storage_configuration_path: str = None,
        #         storage_configuration_dict: dict = None):
    #     self.storage_configuration_dict = None
    #     if storage_configuration_path:
    #         with open(storage_configuration_path, 'r') as f:
    #             self.storage_configuration_dict = json.load(f)
    #     elif storage_configuration_dict:
    #         self.storage_configuration_dict = storage_configuration_dict
    #
    #
    #
    # def prospective(prospective_provenance_dict_path: str = None,
    #         prospective_provenance_dict: dict = None,
    #         insert_prospective = False):
    #     if insert_prospective:
    #         self.insert_prospective()
    #     if self.storage_configuration_dict:
    #         self.wf_obj.update(self.storage_configuration_dict)
    #     self.__persist_prov(self.wf_obj, "workflow")
    #
    #
    #     if prospective_provenance_dict_path:
    #         with open(prospective_provenance_dict_path, 'r') as f:
    #             self.df_structure = json.load(f)
    #     elif prospective_provenance_dict:
    #         self.df_structure = prospective_provenance_dict
    #     else:
    #         self.df_structure = dict()
