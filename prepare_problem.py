import math
from collections import defaultdict

import networkx as nx
import pandas as pd
from wfcommons.wfchef.utils import create_graph

import utils


class Task:
    def __init__(self, task_id, name, machine, memory, normalized_runtime):
        self.id = task_id
        self.name = name
        self.machine = machine
        self.memory = memory
        self.normalized_runtime = normalized_runtime

    def __repr__(self):
        return str(
            f"Task '{self.name}', machine '{self.machine}', normalized runtime: {self.normalized_runtime}")

    def __str__(self):
        return str(vars(self))


class TaskBuilder:
    def __init__(self, task):
        self.id = task.get('id')
        self.name = task.get('name')
        self.machine = task.get('machine')
        self.memory = task.get('memory')
        self.runtime = task.get('runtime')
        self.category = task.get('category')
        self.children = task.get('children')
        self.parents = task.get('parents')
        self.commmand = task['command'].get('program')
        self.arguments = task['command'].get('arguments')
        self.files = task.get('files')
        self.avgCPU = task.get('avgCPU')
        self.priotrity = task.get('priority')
        self.type = task.get('type')

    def __repr__(self):
        return str(vars(self))

    def __str__(self):
        return str(vars(self))

    def build(self, normalized_runtime) -> Task:
        return Task(task_id=self.id, name=self.name, machine=self.machine, memory=self.memory,
                    normalized_runtime=normalized_runtime)


def get_machines_from_wfcommons_file(workflow_file):
    machines_data = utils.read_json_file(workflow_file)
    machines = {machine["nodeName"]: machine for machine in machines_data['workflow']['machines']}
    return machines


def calc_cost(machine, task, runtime):
    if task.memory is not None:
        kib_free = machine['memory']
        cost_power = (task.memory - kib_free) // (1024 * 1024)
    else:
        cost_power = 0
    cost = runtime * machine.get('price', 1) * (1 + machine.get('memory_cost_multiplayer', 0) ** cost_power)
    return cost


def find_all_paths_in_dag(wfcommons_file):
    workflow_dag = create_graph(wfcommons_file)
    paths = list(nx.all_simple_paths(workflow_dag, source='SRC', target='DST'))
    return paths


def real_runtime_formula(machine_details, task=None):
    real_runtime = task.normalized_runtime / (
            machine_details['cpu']['speed'] * machine_details['cpu']['count'])
    return real_runtime


def create_dataframe(data, index):
    dataframe = pd.DataFrame(data, index)
    return dataframe


def calc_dataframes(machines, tasks):
    costs = {}
    runtimes = {}
    for machine_name, machine_details in machines.items():
        machine_cost = []
        machine_runtime = []
        for task in tasks:
            real_runtime = task.normalized_runtime / (
                    machine_details['cpu']['speed'] * machine_details['cpu']['count'])
            machine_cost.append(calc_cost(machine_details, task, real_runtime))
            machine_runtime.append(real_runtime)
        costs[machine_name] = machine_cost
        runtimes[f"{machine_name}Runtime"] = machine_runtime

    index = [j.name for j in tasks]
    cost_df = create_dataframe(costs, index)
    runtime_df = create_dataframe(runtimes, index)
    return cost_df, runtime_df


def create_tasks(wfcommons_data, wf_machines):
    tasks = [TaskBuilder(task) for task in wfcommons_data['workflow']['tasks']]

    new_tasks = []
    for task in tasks:
        normalized_runtime = task.runtime / wf_machines[task.machine]['cpu']['speed'] * \
                             wf_machines[task.machine]['cpu']['count']
        new_task = task.build(normalized_runtime)
        new_tasks.append(new_task)

    return new_tasks


def get_deadlines(paths, tasks, runtimes):
    flat_runtimes = [(runtime, name) for n, machine_runtimes in runtimes.items() for runtime, name in
                     zip(machine_runtimes, [j.name for j in tasks])]

    max_path_runtime = 0.0
    min_path_runtime = 0.0

    for path in paths:
        max_runtime = defaultdict(lambda: 0.0)
        min_runtime = defaultdict(lambda: math.inf)

        for runtime, name in flat_runtimes:
            if name not in path:
                continue
            max_runtime[name] = max(max_runtime[name], runtime)
            min_runtime[name] = min(min_runtime[name], runtime)
        max_path_runtime = max(max_path_runtime, sum(max_runtime.values()))
        min_path_runtime = max(min_path_runtime, sum(min_runtime.values()))
    return min_path_runtime, max_path_runtime
