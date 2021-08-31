from typing import List, Dict
from io import StringIO

from provlake.persistence.persister import Persister
from provlake.capture import ProvTask
from provlake.utils.constants import FileTypes
from provlake.utils.sample_extraction_functions import csv_extraction_function
from provlake.utils.args_handler import get_data_reference, get_dict

from abc import ABCMeta, abstractmethod

__metaclass__ = ABCMeta


class FileExtraction(object):

    def __init__(self, prov: Persister, file_path_or_buffer: str, type_: str, extraction_function, dataset_name: str = None,
                 dataset_id = None, dataset_schema_id=None, data_store_id=None, extraction_function_kwargs:dict={}):
        self._prov = prov
        self._file_path_or_buffer = file_path_or_buffer
        self._extraction_function = extraction_function
        self._extraction_function_kwargs = extraction_function_kwargs
        self._type_ = type_
        self._dataset_id = dataset_id
        self._dataset_name = dataset_name
        self._data_store_id = data_store_id
        self._dataset_schema_id = dataset_schema_id
        # TODO check if this argument should be mandatory

        self._extraction_name = self._dataset_name + "_extraction" if self._dataset_name else type_ + "_extraction"
        self._extraction_name = self._extraction_name.lower()

    @abstractmethod
    def extract(self) -> List[Dict]:
        raise NotImplementedError


class CSVFileExtraction(FileExtraction):

    def __init__(self, prov: Persister, file_path_or_buffer: str, dataset_name: str = None, dataset_id = None,
                 dataset_schema_id=None, header: List = None, separator=',', data_store_id=None,
                 extraction_function=csv_extraction_function, extraction_function_kwargs:dict= {}):
        super().__init__(prov, file_path_or_buffer=file_path_or_buffer, type_=FileTypes.CSV, extraction_function=extraction_function,
                         dataset_name=dataset_name, dataset_id=dataset_id, dataset_schema_id=dataset_schema_id,
                         extraction_function_kwargs=extraction_function_kwargs)
        self._header = header

    def extract(self) -> List[Dict]:
        args = {
            # "file_path": get_data_reference(self._file_path, data_store_id=self._data_store_id),
            "header": str(self._header),
            "dataset_type": "CSV"

        }

        if not isinstance(self._file_path_or_buffer, StringIO):
            args["file_path"] = self._file_path_or_buffer
        else:
            self._file_path_or_buffer.seek(0)

        in_arg = {"input_data": get_dict(args)}
        with ProvTask(self._prov, self._extraction_name, in_arg, custom_metadata={"type": "CSVFileExtraction"}) as provtask:
            args_list = self._extraction_function(self._file_path_or_buffer, **self._extraction_function_kwargs)
            provtask.end(args_list)
        return args_list
