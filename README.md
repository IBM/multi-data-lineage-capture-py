# Multi-Lineage Data System

 IBM Research Multi-Lineage Data System is a provenance data management system capable of capturing, integrating, and querying provenance data generated across multiple, distributed services, programs, databases, and computational workflows.

**For more information on the project, including participants and publications, please see [ibm.biz/provlake](http://ibm.biz/provlake).**

This repository contains the Python library that captures provenance data in Python applications and send to the Multi-Data Lineage Manager, which is responsible for integrating the data in a provenance database stored as a knowledge graph (semantic detabase),
then allowing users to run queries over the data.

It supports Python>=3.6


### Very simple utilization example

```python
from provlake.prov_lake import ProvLake
from provlake.prov_task import ProvTask

"""
Very simple example to show how this library is used to instrument a simple python script for provenance data management.
"""


def calc_factorial(n):
    num = n
    result = 1
    while num > 1:
        result = result * num
        num = num - 1
    return result


prov = ProvLake(online=False, should_log_to_file=True)

in_args = {"n": 5}
with ProvTask(prov, "factorial_number", in_args) as prov_task:

    factorial = calc_factorial(in_args.get("n"))

    out_args = {"factorial": factorial}
    prov_task.output(out_args)

prov.close()
```
