from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
"""
Very simple example to show how ProvLake is used to instrument a simple python script for provenance data management.
"""


prov = ProvLake.get_persister(workflow_name="factorial_dataflow")
workflow = ProvWorkflow(prov).begin()

in_args = {
    "student_id": {
        "type": "list",
        "values": [1, 2, 3]
    },
    "dict_test_simple": {
        "type": "dict",
        "values": {
            "a": 1
        }
    },
    "dict_test_complex": {
        "type": "dict",
        "values": {
            "b": 1,
            "subnode": {
                "c": 2,
                "d": 3
            }
        }
    },
    "dict_test_super_complex": {
        "type": "dict",
        "values": {
            "b": 1,
            "subnode": {
                "type": "dict",
                "values": {
                    "e": 4,
                    "f": 5
                }
            }
        }
    },
    "dict_test_hyper_complex": {
        "type": "dict",
        "values": {
            "b": 1,
            "subnode": {
                "type": "dict",
                "values": {
                    "e": 4,
                    "f": 5,
                    "subsubnode": {
                        "type": "dict",
                        "values": {
                            "y": 4,
                            "x": 5
                        }
                    }
                }
            }
        }
    },
    "ordinary_list": [1,2,3],
    "ordinary_dict": {"a":"b"}
}
with ProvTask(prov, "act_1", in_args) as prov_task:

    out_args = {"out": 50}
    prov_task.end(out_args)

workflow.end()
