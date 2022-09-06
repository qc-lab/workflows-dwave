"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.
"""

from .cqm_solver import CqmSolver
from .gurobi_solver import GurobiSolver


def get_solver(solver_type):
    return {
        "CQM_solver": CqmSolver,
        "Gurobi_solver": GurobiSolver,
    }[solver_type]
