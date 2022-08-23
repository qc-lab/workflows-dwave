import gurobipy as gp
from parse import parse

import utils

from .solver import Solver


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
