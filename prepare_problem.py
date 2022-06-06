import pandas as pd
import json 
import networkx as nx
import math
from collections import defaultdict

class Machine(object):
  def __init__(self, machine):
    self.name = machine.get('nodeName')
    self.system = machine.get('system')
    self.architecture = machine.get('architecture')
    self.release = machine.get('release')
    self.cpu = machine.get('cpu')
    self.memory = machine.get('memory')
  def __repr__(self):
    return str(vars(self))

  def __str__(self):
    return str(vars(self))


class Job(object):
  def __init__(self, id, name, children, parents, machine, inputs, outputs, runtime, used_memeory, normalized_runtime):
    self.id = id
    self.name = name
    self.children = children
    self.parents = parents
    self.machine = machine
    self.inputs = inputs
    self.outputs = outputs
    self.runtime = runtime
    self.used_memeory = used_memeory
    self.normalized_runtime = normalized_runtime


  def find_and_set_parents(self, jobs):
    parent_jobs = []
    for j in jobs:
      for p in self.parents:
        if p == j.name:
          parent_jobs.append(j)
    self.parents = parent_jobs

  def find_and_set_children(self, jobs):
    children_jobs = []
    for j in jobs:
      for c in self.children:
        if c == j.name:
          children_jobs.append(j)
    self.children = children_jobs


  def __repr__(self):
    return str(f"['{self.name}']: on machine '{self.machine.get('name')}' parent: '{[p.name for p in self.parents]}' children: '{[c.name for c in self.children]}'" )

  # def __str__(self):
  #   return str(vars(self))

class JobBuilder(object):
  def __init__(self, job, *, machine_name = None):
    self.id = job.get('id')
    self.name = job.get('name')
    self.category =  job.get('category') 
    self.children_names = job.get('children') or [] 
    self.parent_names = job.get('parents') or [] 
    self.commmand = job['command'].get('program') 
    self.arguments = job['command'].get('arguments')
    files = job.get('files')
    if files is None:
      self.inputs = []
      self.outputs = []
    else: 
      self.inputs = [ (file.get('name'), file.get('size')) for file in files if file.get('link') == 'input']
      self.outputs = [ (file.get('name'), file.get('size')) for file in files if file.get('link') == 'output']
      
    self.avg_cpu_usage = job.get('avgCPU')
    self.used_memeory = job.get('memory')
    self.priotrity = job.get( 'priority')
    self.runtime = job.get('runtime')
    self.type = job.get('type')
    self.normalized_runtime = 1

  def __repr__(self):
    return str(vars(self))

  def __str__(self):
    return str(vars(self))

  def set_machine(self, machine):
    self.machine = machine

  def build(self) -> Job:
    return Job(id = self.id, name = self.name, children = self.children_names, parents = self.parent_names, machine = self.machine, inputs = self.inputs, outputs = self.outputs, runtime = self.runtime, used_memeory = self.used_memeory, normalized_runtime = self.normalized_runtime)


def create_graph(content) -> nx.DiGraph:
  graph = nx.DiGraph()

  # Add src/dst nodes
  graph.add_node("SRC", label="SRC", type="SRC", id="SRC")
  graph.add_node("DST", label="DST", type="DST", id="DST")

  id_count = 0

  for job in content['workflow']['jobs']:
    # specific for epigenomics -- have to think about how to do it in general
    if "genome-dax" in content['name']:
      _type, *_ = job['name'].split('_')
      graph.add_node(job['name'], label=_type, type=_type, id=str(id_count))
      id_count += 1
    else:
      try:
        _type, _id = job['name'].split('_ID')
      except ValueError:
        _type, _id = job['name'].split('_0')
      graph.add_node(job['name'], label=_type, type=_type, id=_id)

    # for job in content['workflow']['jobs']:
    for parent in job['parents']:
      graph.add_edge(parent, job['name'])

  for node in graph.nodes:

    if node in ["SRC", "DST"]:
      continue
    if graph.in_degree(node) <= 0:
      graph.add_edge("SRC", node)
    if graph.out_degree(node) <= 0:
      graph.add_edge(node, "DST")

  return graph

def get_problem_data(workflow_data, machines = None):
  graph = create_graph(workflow_data)

  paths = list(nx.all_simple_paths(graph, source='SRC', target='DST'))

  wf_machines =  [Machine(machine) for machine in workflow_data['workflow']['machines']]
  wf_machines = { machine['nodeName'] : machine for machine in workflow_data['workflow']['machines']}

  jobs_b = [ (JobBuilder(job), job['machine']) for job in workflow_data['workflow']['jobs']]

  for job, machine_name in jobs_b:
      job.set_machine(wf_machines[machine_name])
      job.normalized_runtime = job.runtime / job.machine['cpu']['speed'] * job.machine['cpu']['count']

  min_runtime = min(map(lambda x: x[0].normalized_runtime, jobs_b))

  for j,_ in jobs_b:
    j.normalized_runtime /= min_runtime

  jobs = [job.build() for job, _ in jobs_b]


  for j in jobs:
    j.find_and_set_parents(jobs)
    j.find_and_set_children(jobs)

  if machines is None: 
    machines = wf_machines

  def calc_cost(machine , job, runtime):
    if job.used_memeory is not None:
      kib_free = machine['memory'] 
      cost_power = (job.used_memeory - kib_free) // (1024 * 1024)
    else:
      cost_power = 0
    return runtime * machine.get('price', 1) * (1 + machine.get('memory_cost_multiplayer', 0)** cost_power)

  costs = {}
  runtimes = {}
  for machine in machines:
    machine_cost = []
    machine_runtime = []
    for j in jobs:
      m = machines[machine]
      runtime = j.normalized_runtime / (m['cpu']['speed'] * m['cpu']['count'])
      machine_cost.append(calc_cost(m, j, runtime))
      machine_runtime.append(runtime)
    costs[machine] = machine_cost
    runtimes[f"{machine}Runtime"] = machine_runtime

  cost_df = pd.DataFrame(data=costs, index = [j.name for j in jobs])
  runtime_df = pd.DataFrame(data=runtimes, index = [j.name for j in jobs])

  return cost_df, runtime_df, jobs, paths

def get_deadlines(paths, jobs, runtimes):
  flat_runtimes = [(runtime, name) for n, machine_runtimes in runtimes.items() for runtime, name in zip(machine_runtimes, [j.name for j in jobs])]

  max_path_runtime = 0.0
  min_path_runtime = 0.0

  for path in paths:
    max_runtime = defaultdict(lambda: 0.0)
    min_runtime = defaultdict(lambda: math.inf)

    for runtime, name in flat_runtimes:
        if name not in path:
            continue
        max_runtime[name] = max( max_runtime[name], runtime)
        min_runtime[name] = min( min_runtime[name], runtime)
    # print(path_runtime <= deadline)
    max_path_runtime = max(max_path_runtime, sum(max_runtime.values()))
    min_path_runtime = max(min_path_runtime, sum(min_runtime.values()))
  return min_path_runtime, max_path_runtime