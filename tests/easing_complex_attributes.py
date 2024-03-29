from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
from provlake.utils.args_handler import get_dict, get_list, get_recursive_dicts, get_data_reference, add_custom_metadata
import sys
import json
"""
Very simple example to show how ProvLake is used to instrument a simple python script for provenance data management.
"""


prov = ProvLake.get_persister(workflow_name="example_workflow")
workflow = ProvWorkflow(prov).begin()

in_args = {
    "student_id": get_list([1,2,3]),
    "dict_test_simple": get_dict({
        "a": 1
    }),
    "dict_test_complex": get_dict({
        "b": 1,
        "subnode": {
            "c": 2,
            "d": 3
        }
    }),
    "dict_test_super_complex": get_recursive_dicts({
        "b": 1,
        "subnode": {
            "e": 4,
            "f": 5
        }
    }),
    "dict_test_hyper_complex": get_recursive_dicts({
        "b": 1,
        "mylist0": [1,2,3],
        "mybiglist": [ 0.3432443227822222223, 0.4442880900021000000042, 0.47777722777782223332212],
        "mymatrix0": [[1,2,3],[1,2,3],[1,2,3]],
        "subnode": {
            "e": 4,
            "f": 5,
            "mylist2": [1,2,3],
            "subsubnode": {
                "y": 4,
                "x": 5,
                "mylist3": [1,2,3],
            },


        }
    }),
    "ordinary_list": [1,2,3],
    "ordinary_dict": {
        "a":"b",
        "mylist1": [1,2,3],
        "mylist2": [[1,2,3],[1,2,3],[1,2,3]]
    },
    "list_of_dicts": [
        {"a": 1},
        {"b": 2},
    ],
    "seismic_uri": get_data_reference("http://ibm.org/Netherlands", data_store_id="Allegro1"),
    "mysimple_arg": add_custom_metadata(value="myval", custom_metadata={"custom": 1}),
    "mysimple_arg2": add_custom_metadata(value={"myval2": "val2"}, custom_metadata={"custom": 1}),
    "mydict_list": get_list([
        {
            "name": "a",
            "path": "/home/data/file_a.dat"
        },
        {
            "name": "b",
            "path": "/home/data/file_b.dat"
        }
    ])
}
with ProvTask(prov, "act_1", in_args) as prov_task:
    out_args = {"out": 50}
    prov_task.end(out_args)

workflow.end()
