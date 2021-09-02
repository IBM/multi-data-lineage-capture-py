from abc import ABCMeta, abstractmethod
from provlake.persistence.persister import Persister
from provlake.utils.constants import Status
from time import time


class ActivityCapture:

    __metaclass__ = ABCMeta

    def __init__(self, prov_persister: Persister, custom_metadata: dict=None, generated_time=None):
        self._prov_persister: Persister = prov_persister
        self._custom_metadata = custom_metadata
        if not generated_time:
            self.generated_time = time()
        else:
            self.generated_time = generated_time

    def get_custom_metadata(self):
        return self._custom_metadata

    def get_persister(self):
        return self._prov_persister

    @abstractmethod
    def begin(self, start_time: float = None) -> 'ActivityCapture':
        raise NotImplementedError

    @abstractmethod
    def end(self, output_args=dict(), stdout=None, stderr=None, status=Status.FINISHED) -> 'ActivityCapture':
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *args):
        raise NotImplementedError


