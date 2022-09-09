"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.

Authors: Mateusz Hurbol, Justyna Zawalska
"""

import inspect
import math
from collections import defaultdict
from dataclasses import dataclass

import networkx as nx
import pandas as pd
import wfcommons
from dataclasses_json import dataclass_json

from .utils.file_management import read_json_file


@dataclass_json
@dataclass
class Task:
    """
    Class with the properties of a task.
    """
    id: str
    name: str
    machine: str
    memory: float
    runtime: float

    @classmethod
    def from_dict(cls, env):
        """Allows to extract only selected fields that are needed to create the Task class."""

        return cls(**{
            k: v for k, v in env.items()
            if k in inspect.signature(cls).parameters
        })


def get_machines_from_wfcommons_file(machines_filename: str) -> dict[str, dict]:
    """Reads a WfCommons file that contains data about machines and
    returns them in the form of a dict where the key is the machine
    name and the values are machine details."""

    machines_data = read_json_file(machines_filename)
    machines = {machine['nodeName']: machine for machine in machines_data['workflow']['machines']}
    return machines


def calc_cost(machine: dict, task: Task, runtime: float) -> float:
    """Calculates the cost of performing the task on the selected machine."""

    if task.memory is not None:
        kib_free = machine['memory']
        cost_power = (task.memory - kib_free) // (1024 * 1024)
    else:
        cost_power = 0
    cost = (runtime * machine.get('price', 1) *
            (1 + machine.get('memory_cost_multiplier', 0) ** cost_power))
    return cost


def find_all_paths_in_dag(wfcommons_filename: str) -> list[list[str]]:
    """Uses a function from WfCommons to create a DAG of the workflow
    and then returns the paths from source to all the destinations."""

    workflow_dag = wfcommons.wfchef.utils.create_graph(wfcommons_filename)
    paths = list(nx.all_simple_paths(workflow_dag, source='SRC', target='DST'))
    return paths


def calc_dataframes(machines: dict[str, dict], tasks: list[Task]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Creates dataframes of costs and runtimes."""

    costs, runtimes = {}, {}
    for machine_name, machine_details in machines.items():
        machine_cost = []
        machine_runtime = []
        for task in tasks:
            real_runtime = task.runtime / (
                    machine_details['cpu']['speed'] * machine_details['cpu']['count'])
            machine_cost.append(calc_cost(machine_details, task, real_runtime))
            machine_runtime.append(real_runtime)
        costs[machine_name] = machine_cost
        runtimes[f"{machine_name}Runtime"] = machine_runtime

    index = [task.name for task in tasks]
    cost_df = pd.DataFrame(costs, index)
    runtime_df = pd.DataFrame(runtimes, index)
    return cost_df, runtime_df


def extract_tasks(wfcommons_data: dict) -> list[Task]:
    """Creates a list of tasks  based on the WfCommons data."""
    return [Task.from_dict(task) for task in wfcommons_data['workflow']['tasks']]


def get_deadlines(paths: list[list[str]], tasks: list[Task], runtimes: pd.DataFrame) -> tuple[float, float]:
    """Calculates the minimum and maximum path runtime for the whole workflow."""

    flat_runtimes = [(runtime, name) for n, machine_runtimes in runtimes.items() for runtime, name
                     in zip(machine_runtimes, [j.name for j in tasks])]

    max_path_runtime = 0.0
    min_path_runtime = 0.0

    for path in paths:
        max_runtime: defaultdict[str, float] = defaultdict(lambda: 0.0)
        min_runtime: defaultdict[str, float] = defaultdict(lambda: math.inf)

        for runtime, name in flat_runtimes:
            if name not in path:
                continue
            max_runtime[name] = max(max_runtime[name], runtime)
            min_runtime[name] = min(min_runtime[name], runtime)
        max_path_runtime = max(max_path_runtime, sum(max_runtime.values()))
        min_path_runtime = max(min_path_runtime, sum(min_runtime.values()))

    return min_path_runtime, max_path_runtime
