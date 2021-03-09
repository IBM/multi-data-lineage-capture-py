from provlake import ProvLake
from provlake.capture import ProvWorkflow, ProvTask, ProvCycle

workflow_name = "factorial_workflow_example"
cycle_name = "factorial_loop"
dt_name = "do_multiplication"
log_dir = "."


prov = ProvLake.get_persister(workflow_name=workflow_name, managed_persistence=False)
workflow = ProvWorkflow(prov).begin()


print("Start workflow")
n = 5
current_n = n
result = 1
iteration_id = 0

while current_n > 1:
    with ProvCycle(prov, cycle_name=cycle_name, iteration_id=iteration_id):

        with ProvTask(prov, data_transformation_name=dt_name,
                      parent_cycle_name=cycle_name, parent_cycle_iteration=iteration_id,
                      input_args={"current_n": current_n}) as prov_task:

            result = result * current_n
            current_n = current_n - 1

            prov_task.end({"factorial_result": result})

        iteration_id += 1

print("Finished workflow")
print(result)

workflow.end()
