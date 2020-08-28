from provlake.prov_lake import ProvLake
from provlake.prov_task import ProvTask

"""
Very simple example to show how ProvLake is used to instrument a simple python script for provenance data management.
"""


def calc_factorial(n):
    num = n
    result = 1
    while num > 1:
        result = result * num
        num = num - 1
    return result


prov = ProvLake(workflow_name="factorial_dataflow", online=False, should_log_to_file=True)

in_args = {"n": 5}
with ProvTask(prov, "factorial_number", in_args) as prov_task:

    factorial = calc_factorial(in_args.get("n"))

    out_args = {"factorial": factorial}
    prov_task.output(out_args)



with ProvTask(prov, "factorial_number", in_args) as prov_task:

    factorial = calc_factorial(in_args.get("n"))

    out_args = {"factorial": factorial}
    prov_task.output(out_args)








prov.close()
