from abc import ABCMeta, abstractmethod
from provlake.model.activity_prov_obj import ProvRequestObj


class Persister:

    __metaclass__ = ABCMeta

    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path

    @abstractmethod
    def add_request(self, persistence_request_obj: ProvRequestObj):
        raise NotImplementedError

    def get_file_path(self)->str:
        return self.log_file_path

    def close(self):
        # harakiri
        from provlake import ProvLake
        del ProvLake._persister_singleton_instance
        ProvLake._persister_singleton_instance = None


