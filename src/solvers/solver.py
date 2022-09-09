"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.

Authors: Mateusz Hurbol, Justyna Zawalska
"""

import copy
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from pandas import DataFrame

from src.prepare_problem import (Task, calc_dataframes, extract_tasks,
                                 find_all_paths_in_dag,
                                 get_machines_from_wfcommons_file, get_deadlines)
from src.utils.file_management import read_json_file, write_json_file


class Solver(ABC):
    """The base class for solvers."""
    cost_df: DataFrame
    runtimes_df: DataFrame

    def __init__(self, wfcommons_filename: str, machines_filename: str, output_filename: str, deadline: float):
        self.output_filename: str = output_filename
        self.deadline: float = deadline

        self.wfcommons_data: dict = read_json_file(wfcommons_filename)
        self.machines: dict[str, dict] = get_machines_from_wfcommons_file(machines_filename)
        self.paths: list[list[str]] = find_all_paths_in_dag(wfcommons_filename)
        self.tasks: list[Task] = extract_tasks(self.wfcommons_data)
        self.cost_df, self.runtimes_df = calc_dataframes(self.machines, self.tasks)

        get_deadlines(self.paths, self.tasks, self.runtimes_df)

    @abstractmethod
    def solve(self) -> None:
        """Finds the solution with a given method and saves it."""

    @abstractmethod
    def find_solution(self) -> dict[str, str]:
        """Uses the selected method for finding the solution."""

    def prepare_cost_function(self, binary_variables: list) -> tuple[Any, list[Any], list[Any]]:
        """Uses the QUBO definition of the Workflow Scheduling Problem
        to create the cost function with constraints."""

        tasks_amount = len(self.tasks)

        # Problem definition
        cost_function = 0
        for cost, variable in zip(np.array(self.cost_df).flatten(), binary_variables):
            if cost_function:
                cost_function = cost_function + cost * variable
            else:
                cost_function = cost * variable

        # Has to use exactly one machine
        constraint_one_machine = []
        for i in range(tasks_amount):
            one_machine = 0
            for j in range(i, len(binary_variables), tasks_amount):
                one_machine += binary_variables[j]
            constraint_one_machine.append(one_machine)

        # All paths have to finish before the dedline
        flat_runtimes = [(runtime, name) for n, machine_runtimes in self.runtimes_df.items() for runtime, name in
                         zip(machine_runtimes, [j.name for j in self.tasks])]

        constraint_path_runtime = []
        for path in self.paths:
            path_runtime = 0
            for var, (runtime, name) in zip(binary_variables, flat_runtimes):
                if name not in path:
                    continue
                path_runtime += runtime * var
            constraint_path_runtime.append(path_runtime)

        return cost_function, constraint_one_machine, constraint_path_runtime

    def save_result(self, solution: dict[str, str]) -> None:
        """Assigns the correct machine names for the tasks."""
        output = copy.deepcopy(self.wfcommons_data)
        final_tasks = []
        for task in self.wfcommons_data['workflow']['tasks']:
            final_task = task.copy()
            final_task['machine'] = solution[final_task['name']]
            final_tasks.append(final_task)
        output['workflow']['tasks'] = final_tasks
        output['workflow']['machines'] = self.machines

        write_json_file(output, self.output_filename)
