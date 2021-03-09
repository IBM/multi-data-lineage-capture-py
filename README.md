# Multi-Data Lineage Capture - ProvLake Lib

This is part of ProvLake Project. See [ibm.biz/provlake](http://ibm.biz/provlake) for more details.

## Installation

`pip install git+git://github.com/IBM/multi-data-lineage-capture-py.git`


### A Python lib to capture multiworkflow provenance data from Python Scripts

Use this library for code instrumentation to collect provenance data of function calls in a script. Input arguments or output values from functions can come from distributed data storages, including file systems and database systems.

Python 3.6


### Very simple utilization example

```python
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


prov = ProvLake.get_persister("factorial_dataflow")
with ProvWorkflow(prov):

    in_args = {"n": 5}
    with ProvTask(prov, "factorial_number", in_args) as prov_task:

        factorial = calc_factorial(in_args.get("n"))

        out_args = {"factorial": factorial}
        prov_task.end(out_args)
```


If you prefer, you can use it without context management:

```python
from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask
prov = ProvLake.get_persister("factorial_dataflow_without_ctx_mgmt")
prov_workflow = ProvWorkflow(prov)
prov_workflow.begin()

in_args = {"n": 5}
prov_task = ProvTask(prov, "factorial_number", in_args)
prov_task.begin()

factorial = calc_factorial(in_args.get("n"))

out_args = {"factorial": factorial}
prov_task.end(out_args)

prov_workflow.end()
```
