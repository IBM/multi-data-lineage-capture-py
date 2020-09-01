from provlake.capture.stateless.task import Task
from provlake.capture.stateless.workflow import Workflow
from provlake.capture.stateless.cycle import CycleIteration

workflow_name = "factorial_workflow_example"
cycle_name = "factorial_loop"
dt_name = "do_multiplication"
log_dir = "."

wf_start_time = Workflow.begin(workflow_name, log_dir)
wfexec_id = str(wf_start_time)


print("Start workflow")
n = 5
current_n = n
result = 1
iteration_id = 0

while current_n > 1:
    with CycleIteration(workflow_name=workflow_name,
                        wfexec_id=wfexec_id,
                        cycle_name=cycle_name,
                        iteration_id=iteration_id,
                        input_args={}, log_dir=log_dir):

        with Task(workflow_name=workflow_name,
                  wfexec_id=wfexec_id, data_transformation_name=dt_name,
                  parent_cycle_name=cycle_name, parent_cycle_iteration=iteration_id,
                  input_args={"current_n": current_n},
                  log_dir=log_dir) as prov_task:

            result = result * current_n
            current_n = current_n - 1

            prov_task.capture_output({"factorial_result": result})

        iteration_id += 1

print("Finished workflow")
print(result)

Workflow.end(workflow_name, wf_start_time, log_dir)
