from provlake.persistence.persister import Persister
from provlake.capture import ProvTask
from provlake.utils.constants import FileTypes
from typing import List, Dict
from provlake.utils.sample_extraction_functions import csv_extraction_function
from provlake.utils.args_handler import get_data_reference

from abc import ABCMeta, abstractmethod

__metaclass__ = ABCMeta


class FileExtraction(object):

    def __init__(self, prov: Persister, file_path: str, type_: str, extraction_function, dataset_name: str = None,
                 dataset_schema_id=None):
        self._prov = prov
        self._file_path = file_path
        self._extraction_function = extraction_function
        self._type_ = type_
        self._dataset_name = dataset_name
        self._datastore_id = "datastore_id" #TODO
        self._dataset_schema_id = dataset_schema_id
        # TODO check if this argument should be mandatory

        self._extraction_name = self._dataset_name + "_extraction" if self._dataset_name else type_ + "_extraction"
        self._extraction_name = self._extraction_name.lower()

    @abstractmethod
    def extract(self) -> List[Dict]:
        raise NotImplementedError


class CSVFileExtraction(FileExtraction):

    def __init__(self, prov: Persister, file_path: str, dataset_name: str = None, dataset_schema_id=None,
                 header: list = None,  extraction_function=csv_extraction_function):
        super().__init__(prov, file_path=file_path, type_=FileTypes.CSV, extraction_function=extraction_function,
                         dataset_name=dataset_name,
                         dataset_schema_id=dataset_schema_id)
        self._header = header

    def extract(self) -> List[Dict]:
        with ProvTask(self._prov, self._extraction_name,
                      {"file_path": get_data_reference(self._file_path, data_store_id=self._datastore_id)}) as provtask:
            args_list = self._extraction_function(self._file_path, self._dataset_name)
            provtask.end(args_list)
        return args_list
