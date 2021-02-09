from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
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


person_id = "lga" #It could be read from a specification file (prospective provenacne data file)
prov = ProvLake.get_persister("factorial_dataflow")
with ProvWorkflow(prov):

    in_args = {"n": 5}
    with ProvTask(prov, "factorial_number", in_args, person_id=person_id) as prov_task:

        factorial = calc_factorial(in_args.get("n"))

        out_args = {"factorial": factorial}
        prov_task.end(out_args)
