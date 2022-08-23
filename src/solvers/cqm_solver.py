import pickle

from dimod import Binary, ConstrainedQuadraticModel
from dwave.system import LeapHybridCQMSampler
from parse import parse

import utils

from .solver import Solver


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
