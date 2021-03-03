from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
from provlake.utils.constants import Status
"""
Very simple example to show how ProvLake is used to instrument a simple python script for provenance data management.
One workflow with 1 task.
"""


def calc_factorial(n):
    num = n
    result = 1
    while num > 1:
        result = result * num
        num = num - 1
    return result


prov = ProvLake.get_persister("factorial_dataflow_without_ctx_mgmt", service_url="http://localhost:5000")
prov_workflow = ProvWorkflow(prov)
prov_workflow.begin()

in_args = {"n": 5}
prov_task = ProvTask(prov, "factorial_number", in_args)
prov_task.begin()


factorial = calc_factorial(in_args.get("n"))

out_args = {"factorial": factorial}
prov_task.end(out_args, status=Status.ERRORED)

prov_workflow.end()
