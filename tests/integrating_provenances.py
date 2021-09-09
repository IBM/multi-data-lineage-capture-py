from unittest import TestCase
from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
from provlake.data_extraction.file_extraction import CSVFileExtraction
from provlake.utils.prov_utils import delete_prov_logs
from provlake.utils.sample_extraction_functions import csv_extraction_function
from time import sleep


class TestIntegratingProvenances(TestCase):

    def setUp(self) -> None:
        delete_prov_logs()
        pass

    def prov_1(self):
        persister = ProvLake.get_persister()
        prov_wf = ProvWorkflow(persister, workflow_name="integrated_provenance")
        prov_wf.begin()
        in_args = {"sleep_time": 1}
        with ProvTask(prov_wf, "sleep", in_args):
            sleep(in_args.get("sleep_time"))

        return persister.get_file_path()

    def prov_2(self, log_file_path):
        persister = ProvLake.get_persister(log_file_path=log_file_path)
        prov_wf = ProvWorkflow(persister, workflow_name="integrated_provenance")
        csv_path = "test_data/test_csv.csv"
        extraction_function_kwargs = {
            "dataset_schema_id": "Cities",
            "dataset_id": "ds_cities01"
        }
        items = CSVFileExtraction(prov_wf, dataset_name="cities", file_path_or_buffer=csv_path,
                                  extraction_function=csv_extraction_function,
                                  extraction_function_kwargs=extraction_function_kwargs).extract()
        prov_wf.end()


    def test(self):
        path = self.prov_1()
        self.prov_2(path)