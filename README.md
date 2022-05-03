# Multi-Data Lineage Capture - ProvLake Lib

This is part of ProvLake Project. See [ibm.biz/provlake](http://ibm.biz/provlake) for more details.

## Installation

`pip install provlake`


### A Python lib to capture data processed by multiple workflows using provenance

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
prov = ProvLake.get_persister("factorial_dataflow")
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

By executing this file, the library generates raw provenance data in JSON format. If you don't specify the backend using the `service_url` argument in the `ProvLake.get_persister` builder, the JSON provenance data will be dumped to a local file, updated during workflow execution.
Users can analyze the file using their preferred tools.

To use the analytics tools provided by IBM, such as the Knowledge Exploration System (KES) tool or run more complex queries, users need to use the ProvLake backend service. ProvLake backend processes the JSON provenance data and transforms it into a knowledge graph using W3C standards (PROV, OWL, and RDF) and stores in a knowledge graph system. This backend service is proprietary by IBM. Please contact {rfsouza, lga, rcerq, mstelmar}@br.ibm.com for more information.

