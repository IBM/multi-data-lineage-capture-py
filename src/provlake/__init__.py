from provlake.persistence.persister import Persister
from provlake.persistence.unmanaged_persister import UnmanagedPersister
from provlake.persistence.managed_persister import ManagedPersister
from provlake.capture import ProvWorkflow
from provlake.utils.constants import StandardNamesAndIds
import uuid
import os
import logging

logger = logging.getLogger('PROV')


class ProvLake:
    _persister_singleton_instance: Persister = None

    @staticmethod
    def _build_managed_persister(
            log_dir: str,
            service_url: str,
            should_send_to_file: bool,
            bag_size: int,
            context: str,
            with_validation: bool,
            db_name: str,
            log_file_path: str
    ) -> ManagedPersister:
        should_send_to_service = False
        if service_url is not None:
            should_send_to_service = True

        if not bag_size:
            bag_size = int(os.getenv("PROV_BAG_SIZE", 1))

        if not should_send_to_service:
            assert should_send_to_file is True, "If you are using ProvLake in offline mode, " \
                                               "you need to log prov data to file. Check your 'should_send_to_file' and " \
                                               "'should_send_to_service' parameters."

        return ManagedPersister(
            service_url=service_url,
            context=context,
            with_validation=with_validation,
            db_name=db_name,
            bag_size=bag_size,
            should_send_to_file=should_send_to_file,
            log_dir=log_dir,
            log_file_path=log_file_path,
            should_send_to_service=should_send_to_service)

    @staticmethod
    def _build_unmanaged_persister(log_file_path:str) -> UnmanagedPersister:
        return UnmanagedPersister(log_file_path)


    @staticmethod
    def get_persister(
            log_file_path=None,
            managed_persistence=True,
            context: str = None,
            with_validation: bool = False,
            log_level: str = 'error',
            should_send_to_file=True,
            log_dir='.',
            service_url=None,
            bag_size=None,
            db_name: str = None
    ) -> Persister:
        if ProvLake._persister_singleton_instance is None:

            if log_file_path is None:
                log_file_path = os.path.join(log_dir, "prov-"+str(uuid.uuid4()) + ".log")

            ProvLake._set_log_lvl(log_level)

            if managed_persistence:
                persister = ProvLake._build_managed_persister(
                    log_dir=log_dir,
                    service_url=service_url,
                    should_send_to_file=should_send_to_file,
                    bag_size=bag_size,
                    context=context,
                    with_validation=with_validation,
                    db_name=db_name,
                    log_file_path=log_file_path
                )
            else:
                persister = ProvLake._build_unmanaged_persister(
                    log_file_path
                )
            ProvLake._persister_singleton_instance = persister
        return ProvLake._persister_singleton_instance

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
