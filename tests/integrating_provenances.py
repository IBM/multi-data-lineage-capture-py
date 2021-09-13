from unittest import TestCase
from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
from provlake.data_extraction.file_extraction import CSVFileExtraction
from provlake.utils.prov_utils import delete_prov_logs
from provlake.utils.sample_extraction_functions import csv_extraction_function
from time import sleep
import logging


class TestIntegratingProvenances(TestCase):

    def setUp(self) -> None:
        delete_prov_logs()
        pass

    def prov_1(self):
        persister = ProvLake.get_persister()
        prov_wf = ProvWorkflow(persister, workflow_name="integrated_provenance")
        prov_wf.begin()
        in_args = {"sleep_time": 1}
        with ProvTask(persister, "sleep", prov_workflow=prov_wf, input_args=in_args):
            sleep(in_args.get("sleep_time"))

        return persister.get_file_path()

    def prov_3(self):
        persister = ProvLake.get_persister()
        prov_wf = ProvWorkflow(persister, workflow_name="integrated_provenance")
        in_args = {"sleep_time": 1}
        with ProvTask(persister, "sleep2", prov_wf, in_args):
            sleep(in_args.get("sleep_time"))

        prov_wf.end()

    def test_all(self):
        path = self.prov_1()
        self.prov_3()

    def test_changing_persister(self):
        current_persister = ProvLake.get_persister()
        persister_id = id(current_persister)
        path = self.prov_1()

        # releasing singleton instance
        ProvLake._persister_singleton_instance.close()
        del ProvLake._persister_singleton_instance
        ProvLake._persister_singleton_instance = None

        current_persister = ProvLake.get_persister()
        new_persister_id = id(current_persister)
        self.assertNotEqual(persister_id, new_persister_id, 'there should be a new persister')

        self.prov_3()
        print("a")

    def test_open_ended_workflow(self):
        path = self.prov_1()
