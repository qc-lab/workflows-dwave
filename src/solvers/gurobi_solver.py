"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.

Authors: Mateusz Hurbol, Justyna Zawalska
"""
from typing import Dict

import gurobipy as gp
from parse import parse

from ..utils.execution_stats import calculate_time
from .solver import Solver


class GurobiSolver(Solver):
    """Solver that uses Gurobi Optimizer."""

    def solve(self) -> None:
        """Finds the solution and saves it."""
        solution = self.find_solution()
        self.save_result(solution)

    @calculate_time
    def find_solution(self) -> Dict[str, str]:
        """Uses Gurobi Optimizer to find the optimal solution."""
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

        solution = {}
        for variable in gpm.getVars():
            machine, var = parse('m{}_x{}', variable.VarName)
            if variable.X == 1.0:
                solution[self.tasks[int(var)].name] = machine_names[int(machine)]
        return solution
