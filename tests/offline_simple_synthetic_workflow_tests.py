import traceback
import unittest
from provlake.prov_lake import ProvLake
from provlake.prov_task import ProvTask
import uuid
import json
from random import randint, uniform
import sys
import os
import subprocess


class SimpleSyntethicWorkflowTests(unittest.TestCase):
    """
    author: Marcelo Costalonga Cardoso (Marcelo.Costalonga.Cardoso@ibm.com)
    date: 16/December/2019

    Test if ProvLakePy works correctly with dlprov-collector and prov-manager and if the log files are being created
    correctly
    """
    current_dir = os.getcwd()
    if current_dir[current_dir.rfind("/"):] != "/tests":
        current_dir = os.path.join(current_dir, "tests")
    prospective_path = os.path.join(current_dir, "test_data/prospective_json/synthetic_dataflow.json")
    output_dir = os.path.join(current_dir, "output")

    def SimpleSyntethicWorkflowTests(self):
        self.prov = ProvLake(prospective_provenance_dict_path=self.prospective_path,
                             context="<http://br.ibm.com/provlake_tests>",
                             online=False, log_dir=self.output_dir, should_log_to_file=True)
        self.number_of_tasks = 0
        self.data_transformations_args = dict()
        self.tasks_ids = list()

    def update_data_transformations_args_dict(self, data_transformation_name: str, args: dict, args_type: str):
        if args_type.lower() in ["input", "output"]:
            if data_transformation_name not in self.data_transformations_args:
                self.data_transformations_args[data_transformation_name] = dict()
            self.data_transformations_args[data_transformation_name][args_type] = args

    def clear_logs_dir(self):
        if os.path.exists(self.output_dir):
            for log_file in os.listdir(self.output_dir):
                if log_file.endswith(".log") and log_file.startswith("prov-wfexec"):
                    os.remove(os.path.join(self.output_dir, log_file))

    def check_logs_dir_size(self):
        was_log_file_created = False
        if os.path.exists(self.output_dir):
            if len(os.listdir(self.output_dir)) > 0:
                was_log_file_created = True
        assert was_log_file_created == True, "Log file was not created after test was completed"

    def append_task_id(self, task_log: dict):
        assert task_log["prov_obj"].__contains__("task"), "Missing 'task' key"
        task = task_log["prov_obj"]["task"]
        try:
            if not task["id"] in self.tasks_ids:
                self.tasks_ids.append(task["id"])
        except:
            traceback.print_exc()

    def check_log_file_tasks(self):
        if os.path.exists(self.output_dir):
            for log_file in os.listdir(self.output_dir):
                self.tasks_ids.clear()
                with open(os.path.join(self.output_dir, log_file)) as f:
                    for logs in f.readlines():
                        task_logs = json.loads(logs)
                        workflow_count = 0
                        task_count = 0
                        for task_log in task_logs:
                            assert task_log.__contains__("prov_obj"), "Missing 'prov_obj' key"
                            self.check_status(task_log["prov_obj"])
                            if task_log["act_type"] == "task":
                                self.append_task_id(task_log)
                                self.check_task_args_size(task_log)
                                task_count+=1
                            elif task_log["act_type"] == "workflow":
                                workflow_count+=1
                        self.check_json_array_elements(workflow_count, task_count)
                self.check_number_of_task_ids()


    def check_task_args_size(self, task_log: dict):
        try:
            data_transformation_name = task_log["prov_obj"]["dt"]
            data_transformation_type = task_log["prov_obj"]["type"]
            values = task_log["prov_obj"]["values"]
            assert len(values) == len(self.data_transformations_args[data_transformation_name][data_transformation_type])
        except:
            traceback.print_exc()

    def check_number_of_task_ids(self):
        assert self.number_of_tasks == len(self.tasks_ids), "Number of tasks ids expected: {}.\nNumber of tasks ids " \
                                                            "found:{}".format(self.number_of_tasks,len(self.tasks_ids))

    def check_json_array_elements(self, workflow_count, task_count):
        assert workflow_count <= 1, "Json array contains more than one workflow"
        assert task_count <= 1, "Json array contains more than one task"

    def check_status(self, prov_obj):
        status_expected = "FINISHED"
        if prov_obj.__contains__("status"):
            status_found = prov_obj["status"]
            assert status_found == status_expected, "Status expected: {}. Status found: {}".format(status_expected,
                                                                                                   status_found)

    def create_complex_args(self):
        a1 = list()
        for i in range(randint(3,5)):
            a1.append(randint(100,200))
        a2 = uniform(1.1, 3.1)
        a3 =  list()
        for i in range(randint(2,4)):
            complex_attributes = dict()
            complex_attributes["a3_ca1"] = randint(1000, 1500)
            complex_attributes["a3_ca2"] = randint(1000, 1500)
            complex_attributes["a3_ca3"] = randint(1000, 1500)
            a3.append(complex_attributes)
        return {"a1_lst": a1, "a2": a2, "a3": a3}


    # Mock functions:
    def mock_function1(self, input_args: dict):
        data_transformation_name = "act_1"
        self.update_data_transformations_args_dict(data_transformation_name, input_args, "Input")
        with ProvTask(self.prov, data_transformation_name, input_args) as task:
            out_id_val = str(uuid.uuid4())
            print('\nTesting task: {}'.format(data_transformation_name))
            output_args = {"a4": "a4_{}".format(out_id_val), "a5": "a5_{}".format(out_id_val),
                           "a6": "a6_{}".format(out_id_val)}
            task.output(output_args)
            self.number_of_tasks += 1
            self.update_data_transformations_args_dict(data_transformation_name, output_args, "Output")
            return output_args

    def mock_function2(self, input_args: dict):
        data_transformation_name = "act_2"
        self.update_data_transformations_args_dict(data_transformation_name, input_args, "Input")
        with ProvTask(self.prov, data_transformation_name, input_args) as task:
            out_id_val = str(uuid.uuid4())
            print('\nTesting task: {}'.format(data_transformation_name))
            output_args = {"a7": "a7_{}".format(out_id_val), "a8": "a8_{}".format(out_id_val),
                           "a9": "a9_{}".format(out_id_val)}
            task.output(output_args)
            self.number_of_tasks += 1
            self.update_data_transformations_args_dict(data_transformation_name, output_args, "Output")
            return output_args

    def mock_function3(self, input_args: dict):
        data_transformation_name = "act_20"
        self.update_data_transformations_args_dict(data_transformation_name, input_args, "Input")
        with ProvTask(self.prov, data_transformation_name, input_args) as task:
            output_args = self.create_complex_args()
            print('\nTesting task: {}'.format(data_transformation_name))
            task.output(output_args)
            self.number_of_tasks += 1
            self.update_data_transformations_args_dict(data_transformation_name, output_args, "Output")
            return output_args

    # Tests:
    def test_simple(self):
        # self.clear_logs_dir()
        self.SimpleSyntethicWorkflowTests()
        in_id_val = str(uuid.uuid4())

        input_args1 = {"a1": "a1_{}".format(in_id_val), "a2": "a2_{}".format(in_id_val), "a3": "a3_{}".format(in_id_val)}
        output_args1 = self.mock_function1(input_args1)

        self.mock_function2(output_args1)

        input_args3 = self.create_complex_args()
        self.mock_function3(input_args3)

        self.prov.close()

        self.check_logs_dir_size()
        self.check_log_file_tasks()

# if __name__ == '__main__':
#     if '--unittest' in sys.argv:
#         subprocess.call([sys.executable, '-m', 'unittest', 'discover'])