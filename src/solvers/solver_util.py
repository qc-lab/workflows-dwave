"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.
"""

from .cqm_solver import CqmSolver
from .gurobi_solver import GurobiSolver
from .solver import Solver


def get_solver(solver_type: str) -> Solver:
    """Returns the class for the given solver type."""
    return {
        "CQM_solver": CqmSolver,
        "Gurobi_solver": GurobiSolver,
    }[solver_type]
