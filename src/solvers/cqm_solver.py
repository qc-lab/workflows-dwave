"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.

Authors: Mateusz Hurbol, Justyna Zawalska
"""
import pickle

from dimod import Binary, ConstrainedQuadraticModel
from dwave.system import LeapHybridCQMSampler
from parse import parse

from ..config import cqm_config
from ..utils.execution_stats import calculate_time
from ..utils.file_management import write_pickle_file
from .solver import Solver


class CqmSolver(Solver):
    """Solver that uses Constrained Quadratic Model from D-Wave."""

    def solve(self):
        """Finds the solution and saves it."""

        solution = self.find_solution()
        self.save_result(solution)

    @calculate_time
    def find_solution(self) -> dict[str, str]:
        """Uses CQM to find the optimal solution.

        The Leap Hybrid CQM Sampler returns a few potential candiadates for optimal solution.
        To find the optimal solution the results from the sampler have to be validated."""

        cqm = ConstrainedQuadraticModel()
        tasks_amount = len(self.tasks)
        machines_amount = len(self.cost_df.columns)

        binary_variables = []
        for machines in range(machines_amount):
            var = [Binary(f'm{machines}_x{i}') for i in range(tasks_amount)]
            binary_variables.extend(var)

        cost_function, constraint_one_machine, constraint_path_runtime = self.prepare_cost_function(binary_variables)

        cqm.set_objective(cost_function)

        for i in range(tasks_amount):
            cqm.add_constraint(constraint_one_machine[i] == 1)

        for i in range(len(self.paths)):
            cqm.add_constraint(constraint_path_runtime[i] <= self.deadline)

        sampler_cqm = LeapHybridCQMSampler()

        solutions = sampler_cqm.sample_cqm(cqm, cqm_config.TIME_LIMIT)
        save_solution_energies(solutions)

        valid_solution = {}
        correct_solutions = [s for s in solutions if len(cqm.violations(s, skip_satisfied=True)) == 0]
        best_solution = correct_solutions[0]
        machine_names = self.cost_df.columns
        for k, is_used in best_solution.items():
            machine, var = parse('m{}_x{}', k)
            if is_used:
                valid_solution[self.tasks[int(var)].name] = machine_names[int(machine)]

        return valid_solution


def save_solution_energies(solutions) -> None:
    """Saves all the solutions returned from D-Wave and their energies."""

    solved_cqm = {
        "solution": {
            "info": solutions.info,
            "data_vectors": solutions.data_vectors,
            "solutions": list(solutions),
        },
    }
    write_pickle_file(solved_cqm, cqm_config.OUTPUT_FILENAME)
