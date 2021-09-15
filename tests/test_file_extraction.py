from unittest import TestCase
from provlake import ProvLake
from provlake.capture import ProvWorkflow
from provlake.data_extraction.file_extraction import CSVFileExtraction
from provlake.utils.prov_utils import delete_prov_logs
from provlake.utils.sample_extraction_functions import city_csv_extraction_function


class TestMolecules(TestCase):

    def setUp(self) -> None:
        delete_prov_logs()
        pass

    def test_extraction(self):
        persister = ProvLake.get_persister()
        workflow = ProvWorkflow(persister, workflow_name="city_dataset_ingestion")
        workflow.begin()

        csv_path = "test_data/test_csv.csv"
        extraction_function_kwargs = {
            "dataset_schema_id": "Cities"
        }
        items = CSVFileExtraction(workflow, dataset_name="cities", file_path_or_buffer=csv_path,
                                  extraction_function=city_csv_extraction_function,
                                  extraction_function_kwargs=extraction_function_kwargs).extract()

        # for item in items:
        #     with ProvTask(prov, data_transformation_name='consume_item', input_args=item) as provtask:
        #         provtask.end()

        workflow.end()
