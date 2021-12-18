#!python3
import numpy as np

from parse import parse

from dimod import cqm_to_bqm, Binary, Integer, ConstrainedQuadraticModel
from dwave.system import LeapHybridBQMSampler, LeapHybridCQMSampler

from prepare_problem import get_problem_data

from pprint import pprint

import time

DEADLINE = 40000

cost, runtimes, jobs, paths = get_problem_data('pegasus-workflow.json')

def cqm_solution(cost_df, jobs, runtimes, paths, debug = False):
    cqm = ConstrainedQuadraticModel()
    xs = []
    job_count = len(jobs)

    for machines in range(len(cost_df.columns)):
        x = [Binary(f'm{machines}_x{i}') for i in range(job_count)]
        xs.extend(x)

    # Problem definition
    model = None
    for variable, cost in zip(np.array(cost_df).flatten(), xs):
        if model:
            model = model + cost * variable
        else:
            model = cost * variable
    cqm.set_objective(model)

    # Must use exactly one machine
    for i in range(job_count):
        one_machine = None
        for j in range(i, len(xs), job_count):
            if one_machine:
                one_machine = one_machine + xs[j]
            else:
                one_machine = xs[j]
        cqm.add_constraint( one_machine == 1)

    # All paths finish before dedline 

    flat_runtimes = [(runtime, name) for n, machine_runtimes in runtimes.items() for runtime, name in zip(machine_runtimes, [j.name for j in jobs])]

    for path in paths:
        path_runtime = None
        for var, (runtime, name) in zip(xs, flat_runtimes):
            if name not in path:
                continue
            if path_runtime:
                path_runtime = path_runtime + runtime * var
            else:path_runtime = runtime * var
        cqm.add_constraint(path_runtime <= DEADLINE)
    
    sampler_cqm = LeapHybridCQMSampler()

    start = time.time()
    solution = sampler_cqm.sample_cqm(cqm,time_limit=5)
    end = time.time()
    pprint(f"SOLUTION TIME: {end - start}")

    if debug:
        pprint(solution.info)

    def is_correct_solution(cqm, sol):
        return len(cqm.violations(sol, skip_satisfied = True)) == 0

    correct_solutions = [ s for s in solution if is_correct_solution(cqm, s)]
    if debug:
        pprint(solution.info)

    best_solution = correct_solutions[0]

    machine_names = cost_df.columns
    job_names = cost_df.index

    actual_solution = {}

    for k, is_used in best_solution.items():
        machine, var = parse('m{}_x{}', k)
        if is_used:
            actual_solution[int(var)] = machine_names[int(machine)]
    return actual_solution


import gurobipy as gp
from gurobipy import GRB

def gp_solution(cost_df, jobs, runtimes, paths):
    gpm = gp.Model("workflow")
    xs = []
    job_count = len(jobs)

    for machines in range(len(cost_df.columns)):
        for i in range(job_count):
            xs.append(gpm.addVar(vtype=GRB.BINARY, name=f'm{machines}_x{i}'))

    # Problem definition
    model = None
    for variable, cost in zip(np.array(cost_df).flatten(), xs):
        if model:
            model = model + cost * variable
        else:
            model = cost * variable

    gpm.setObjective(model, GRB.MINIMIZE)

    # Must use exactly one machine
    for i in range(job_count):
        one_machine = None
        for j in range(i, len(xs), job_count):
            if one_machine:
                one_machine = one_machine + xs[j]
            else:
                one_machine = xs[j]
        gpm.addConstr( one_machine == 1, f"one_machine_{i}")

    # All paths finish before dedline 

    flat_runtimes = [(runtime, name) for n, machine_runtimes in runtimes.items() for runtime, name in zip(machine_runtimes, [j.name for j in jobs])]

    for path in paths:
        path_runtime = None
        for var, (runtime, name) in zip(xs, flat_runtimes):
            if name not in path:
                continue
            if path_runtime:
                path_runtime = path_runtime + runtime * var
            else:
                path_runtime = runtime * var
        gpm.addConstr(path_runtime <= DEADLINE, f"path_deadline_{i}")
    
    # Optimize model
    gpm.optimize()

    machine_names = cost_df.columns
    job_names = cost_df.index

    actual_solution = {}
    for v in gpm.getVars():
        machine, var = parse('m{}_x{}', v.VarName)
        if v.X == 1.0:
            actual_solution[int(var)] = machine_names[int(machine)]
    return actual_solution

start = time.time()
sol = gp_solution(cost, jobs, runtimes, paths)
end = time.time()
pprint(sol)
print(f"{end - start}s")
print("=" * 40)
start = time.time()
sol = cqm_solution(cost, jobs, runtimes, paths, debug = True)
end = time.time()
pprint(sol)
print(f"{end - start}s")
