from prepare_problem import get_problem_data

from pprint import pprint

from gurobi_solver import gp_solution 
from cqm_solution import cqm_solution

import time

DEADLINE = 40000

cost, runtimes, jobs, paths = get_problem_data('pegasus-workflow.json', 'cyfronet_machines.json')

start = time.time()
sol = gp_solution(cost, jobs, runtimes, paths, DEADLINE)
end = time.time()
pprint(sol)
print(f"{end - start}s")
# print("=" * 40)
# start = time.time()
# sol = cqm_solution(cost, jobs, runtimes, paths, DEADLINE, debug = True)
# end = time.time()
# pprint(sol)
# print(f"{end - start}s")
pprint(jobs)
