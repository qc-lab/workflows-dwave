import gurobipy as gp
from gurobipy import GRB

from parse import parse
import numpy as np
import time

def gp_solution(cost_df, jobs, runtimes, paths, deadline):
    gpm = gp.Model("workflow")
    xs = []
    job_count = len(jobs)

    for machines in range(len(cost_df.columns)):
        for i in range(job_count):
            xs.append(gpm.addVar(vtype=GRB.BINARY, name=f'm{machines}_x{i}'))

    # Problem definition
    model = None
    for cost, variable in zip(np.array(cost_df).flatten(), xs):
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
        print(path_runtime <= deadline)
        gpm.addConstr(path_runtime <= deadline, f"path_deadline_{i}")
    
    # Optimize model
    gpm.optimize()

    machine_names = cost_df.columns
    job_names = cost_df.index

    actual_solution = {}
    for v in gpm.getVars():
        machine, var = parse('m{}_x{}', v.VarName)
        if v.X == 1.0:
            actual_solution[jobs[int(var)].name] = machine_names[int(machine)]
    return actual_solution
