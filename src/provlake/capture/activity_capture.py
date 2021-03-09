from abc import ABCMeta, abstractmethod
from provlake.persistence.persister import Persister


class ActivityCapture:

    __metaclass__ = ABCMeta

    def __init__(self, prov_persister: Persister):
        self._prov_persister: Persister = prov_persister

    @abstractmethod
    def begin(self) -> 'ActivityCapture':
        raise NotImplementedError

    @abstractmethod
    def end(self, output_args: dict=None, stdout=None, stderr=None) -> 'ActivityCapture':
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *args):
        raise NotImplementedError


