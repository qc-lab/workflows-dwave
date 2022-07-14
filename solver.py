import copy
import pickle
from abc import ABC, abstractmethod

import gurobipy as gp
import numpy as np
from dimod import Binary, ConstrainedQuadraticModel
from dwave.system import LeapHybridCQMSampler
from parse import parse

import utils
from prepare_problem import (calc_dataframes, create_tasks,
                             find_all_paths_in_dag,
                             get_machines_from_wfcommons_file)


def solver(wfcommons_file, machine_file, output_file, deadline, method):
    solvers = {
        "CQM_solver": CqmSolver,
        "Gurobi_solver": GurobiSolver,
    }
    return solvers[method](wfcommons_file, machine_file, output_file, deadline, method)


class Solver(ABC):
    def __init__(self, wfcommons_file, machine_file, output_file, deadline, method):
        self.wfcommons_file = wfcommons_file
        self.deadline = deadline
        self.method = method
        self.machine_file = machine_file
        self.output_file = output_file
        self.wfcommons_data = self.set_wfcommons_data()
        self.machines = self.set_machines()
        self.wf_machines = self.set_wf_machines()
        self.tasks = self.set_tasks()
        self.paths = self.set_paths()
        self.cost_df, self.runtimes_df = self.set_dataframes()

    @abstractmethod
    def solve(self):
        pass

    def set_wfcommons_data(self):
        return utils.read_json_file(self.wfcommons_file)

    def set_dataframes(self):  # todo break in 2 sub-functions
        return calc_dataframes(self.machines, self.tasks)

    def set_machines(self):
        if self.machine_file is not None:
            machines = get_machines_from_wfcommons_file(self.machine_file)
        else:
            machines = self.wf_machines
        return machines

    def set_tasks(self):
        return create_tasks(self.wfcommons_data, self.wf_machines)

    def set_wf_machines(self):
        return get_machines_from_wfcommons_file(self.wfcommons_file)

    def set_paths(self):
        return find_all_paths_in_dag(self.wfcommons_file)

    def prepare_cost_function(self, binary_variables):
        tasks_amount = len(self.tasks)

        # Problem definition
        cost_function = None
        for cost, variable in zip(np.array(self.cost_df).flatten(), binary_variables):
            if cost_function:
                cost_function = cost_function + cost * variable
            else:
                cost_function = cost * variable

        # Has to use exactly one machine
        constraint_one_machine = []
        for i in range(tasks_amount):
            one_machine = None
            for j in range(i, len(binary_variables), tasks_amount):
                if one_machine:
                    one_machine = one_machine + binary_variables[j]
                else:
                    one_machine = binary_variables[j]
            constraint_one_machine.append(one_machine)

        # All paths have to finish before the dedline
        flat_runtimes = [(runtime, name) for n, machine_runtimes in self.runtimes_df.items() for runtime, name in
                         zip(machine_runtimes, [j.name for j in self.tasks])]

        constraint_path_runtime = []
        for path in self.paths:
            path_runtime = None
            for var, (runtime, name) in zip(binary_variables, flat_runtimes):
                if name not in path:
                    continue
                if path_runtime:
                    path_runtime = path_runtime + runtime * var
                else:
                    path_runtime = runtime * var
            constraint_path_runtime.append(path_runtime)

        return cost_function, constraint_one_machine, constraint_path_runtime

    def save_result(self, solution):
        output = copy.deepcopy(self.wfcommons_data)
        final_tasks = []
        for task in self.wfcommons_data["workflow"]["tasks"]:
            final_task = task.copy()
            final_task["machine"] = solution[final_task["name"]]
            final_tasks.append(final_task)
        output["workflow"]["tasks"] = final_tasks
        output["workflow"]["machines"] = self.machines

        utils.write_json_file(output, self.output_file)


class GurobiSolver(Solver):
    @utils.calculate_time
    def solve(self):
        solution = self.new_gurobi_solution()
        self.save_result(solution)

    def new_gurobi_solution(self):
        gpm = gp.Model("workflow")
        binary_variables = []
        tasks_amount = len(self.tasks)

        for machines in range(len(self.cost_df.columns)):
            for i in range(tasks_amount):
                binary_variables.append(gpm.addVar(vtype=gp.GRB.BINARY, name=f'm{machines}_x{i}'))

        cost_function, constraint_one_machine, constraint_path_runtime = self.prepare_cost_function(
            binary_variables)

        gpm.setObjective(cost_function, gp.GRB.MINIMIZE)

        for i in range(tasks_amount):
            gpm.addConstr(constraint_one_machine[i] == 1, f"one_machine_{i}")

        for i in range(len(self.paths)):
            gpm.addConstr(constraint_path_runtime[i] <= self.deadline, f"path_deadline_{i}")

        gpm.optimize()

        machine_names = self.cost_df.columns

        actual_solution = {}
        for v in gpm.getVars():
            machine, var = parse('m{}_x{}', v.VarName)
            if v.X == 1.0:
                actual_solution[self.tasks[int(var)].name] = machine_names[int(machine)]
        return actual_solution


class CqmSolver(Solver):
    @utils.calculate_time
    def solve(self):
        solution = self.new_cqm_solution()
        self.save_result(solution)

    def new_cqm_solution(self):
        cqm = ConstrainedQuadraticModel()
        tasks_amount = len(self.tasks)
        machines_amount = len(self.cost_df.columns)

        binary_variables = []
        for machines in range(machines_amount):
            x = [Binary(f'm{machines}_x{i}') for i in range(tasks_amount)]
            binary_variables.extend(x)

        cost_function, constraint_one_machine, constraint_path_runtime = self.prepare_cost_function(binary_variables)

        cqm.set_objective(cost_function)

        for i in range(tasks_amount):
            cqm.add_constraint(constraint_one_machine[i] == 1)

        for i in range(len(self.paths)):
            cqm.add_constraint(constraint_path_runtime[i] <= self.deadline)

        sampler_cqm = LeapHybridCQMSampler()

        solution = sampler_cqm.sample_cqm(cqm, time_limit=5)

        def is_correct_solution(cqm, sol):
            return len(cqm.violations(sol, skip_satisfied=True)) == 0

        correct_solutions = [s for s in solution if is_correct_solution(cqm, s)]

        best_solution = correct_solutions[0]

        machine_names = self.cost_df.columns

        actual_solution = {}

        for k, is_used in best_solution.items():
            machine, var = parse('m{}_x{}', k)
            if is_used:
                actual_solution[self.tasks[int(var)].name] = machine_names[int(machine)]

        solved_cqm = {
            "solution": {
                "info": solution.info,
                "data_vectors": solution.data_vectors,
                "solutions": [s for s in solution],
            },
        }

        output_name = "data/results/cqm_results.pkl"
        with open(output_name, 'wb') as out_file:
            pickle.dump(solved_cqm, out_file)

        return actual_solution


# def save_result(wfcommons_data, solution, machines, output_file):
#     output = copy.deepcopy(wfcommons_data)
#     final_tasks = []
#     for task in wfcommons_data["workflow"]["tasks"]:
#         final_task = task.copy()
#         final_task["machine"] = solution[final_task["name"]]
#         final_tasks.append(final_task)
#     output["workflow"]["tasks"] = final_tasks
#     output["workflow"]["machines"] = machines
#
#     utils.write_json_file(output, output_file)
