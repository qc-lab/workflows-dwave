from pprint import pprint
import sys, getopt
from prepare_problem import get_problem_data
import json
from dimod import cqm_to_bqm, Binary, Integer, ConstrainedQuadraticModel
from dwave.system import LeapHybridBQMSampler, LeapHybridCQMSampler

import numpy as np
from parse import parse
import time

import pickle
import os

from os import walk

def generate_histogram(inputfile, machinefile, deadline, output_name):

    with open(inputfile) as f:
        input = json.loads(f.read())

    machines = None
    if machinefile is not None:
        with open(machinefile) as f:
            machines = json.loads(f.read())

    # def cqm_solution(cost_df, jobs, runtimes, paths, deadline, debug = False):
    cost_df, runtimes, jobs, paths = get_problem_data(input, machines)


    cqm = ConstrainedQuadraticModel()
    xs = []
    job_count = len(jobs)

    for machines in range(len(cost_df.columns)):
        x = [Binary(f'm{machines}_x{i}') for i in range(job_count)]
        xs.extend(x)

    # Problem definition
    model = None
    for cost, variable in zip(np.array(cost_df).flatten(), xs):
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
        cqm.add_constraint(path_runtime <= deadline)

    sampler_cqm = LeapHybridCQMSampler()

    start = time.time()
    solution = sampler_cqm.sample_cqm(cqm,time_limit=5)
    end = time.time()


    # if debug:
    #     pprint(solution.info)

    def is_correct_solution(cqm, sol):
        return len(cqm.violations(sol, skip_satisfied = True)) == 0

    correct_solutions = [ s for s in solution if is_correct_solution(cqm, s)]
    # if debug:
    #     pprint(solution.info)

    best_solution = correct_solutions[0]

    machine_names = cost_df.columns
    job_names = cost_df.index

    actual_solution = {}

    for k, is_used in best_solution.items():
        machine, var = parse('m{}_x{}', k)
        if is_used:
            actual_solution[jobs[int(var)].name] = machine_names[int(machine)]

    # for s in solution:
    # print(solution.data_vectors.keys())
    print(solution.data_vectors["energy"])
    print(actual_solution)
    # print(solution.data_vectors["num_occurrences"])
    # print(solution.data_vectors["is_feasible"])
    # print(solution.data_vectors["is_satisfied"])
    # print(solution.info.keys())
    # print(solution.info['qpu_access_time'])

    solved_cqm = {
        "input" : inputfile,
        "machines" : machines,
        "deadline" : deadline,
        "solution" : {
            "info" : solution.info,
            "data_vectors" : solution.data_vectors,
            "solutions" : [s for s in solution],
        },
        "machine_names" : machine_names,
        "job_names" : job_names
    }

    with open(output_name, 'wb') as out_file:
        pickle.dump(solved_cqm, out_file)


# deadlines = [20, 130, 260, 300]
deadlines = [10000]

# ['Jobs_492.json'] #
workflows = next(walk("workflows"), (None, None, []))[2]  # [] if no file
machines = ['basic_test.json', 'cyfronet.json'] #next(walk("machines"), (None, None, []))[2]  # [] if no file

print(machines)
print(workflows)
for deadline in deadlines:
    for workflow in workflows:
        for machine in machines:
            output_name = f"pickled_cqm_results/{os.path.splitext(workflow)[0]}_{os.path.splitext(machine)[0]}_{deadline}.pkl"
            if not os.path.exists(output_name):
                print(f"RUNNING: {output_name}")
                generate_histogram(f"workflows/{workflow}", f"machines/{machine}", deadline, output_name)
            else:
                print(f"CACHED: {output_name}")

