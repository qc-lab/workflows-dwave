from .cqm_solver import CqmSolver
from .gurobi_solver import GurobiSolver


def get_solver(solver_type):
    return {
        "CQM_solver": CqmSolver,
        "Gurobi_solver": GurobiSolver,
    }[solver_type]
