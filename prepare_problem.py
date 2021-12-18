import pandas as pd
import json 
import networkx as nx
from wfcommons.wfchef.utils import create_graph

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
    return str(f"['{self.name}']: on machine '{self.machine.name}' parent: '{[p.name for p in self.parents]}' children: '{[c.name for c in self.children]}'" )

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

  def set_machine(self, machine : Machine):
    self.machine = machine

  def build(self) -> Job:
    return Job(id = self.id, name = self.name, children = self.children_names, parents = self.parent_names, machine = self.machine, inputs = self.inputs, outputs = self.outputs, runtime = self.runtime, used_memeory = self.used_memeory, normalized_runtime = self.normalized_runtime)


def get_problem_data(path):
  with open(path) as f:
    json_data = json.loads(f.read())
  graph = create_graph(path)

  paths = list(nx.all_simple_paths(graph, source='SRC', target='DST'))

  machines =  [Machine(machine) for machine in json_data['workflow']['machines']]
  machines = { machine.name : machine for machine in machines}

  jobs_b = [ (JobBuilder(job), job['machine']) for job in json_data['workflow']['jobs']]

  for job, machine_name in jobs_b:
      job.set_machine(machines[machine_name])
      job.normalized_runtime = job.runtime / job.machine.cpu['speed'] * job.machine.cpu['count']

  min_runtime = min(map(lambda x: x[0].normalized_runtime, jobs_b))

  for j,_ in jobs_b:
    j.normalized_runtime /= min_runtime

  jobs = [job.build() for job, _ in jobs_b]


  for j in jobs:
    j.find_and_set_parents(jobs)
    j.find_and_set_children(jobs)


  base_cpu_hour_price = 0.08
  base_gpu_hour_price = 5.0
  gpu_speed_vs_cpu = 100

  cpus = {'Zeus': 0.25, 'Prometeus': 1 , 'Ares': 1.6}
  cpus_base_memory = {'Zeus': 2, 'Prometeus': 4 , 'Ares': 8}
  cpus_more_memory_factor = {'Zeus': 0.2, 'Prometeus': 0.40 , 'Ares': 0.80}


  gpus = {'Prometeus': 1 , 'Ares': 1.6}
  gpus_base_memory = {'Prometeus': 4 , 'Ares': 8}
  gpus_more_memory_factor = {'Prometeus': 0.40 , 'Ares': 0.80}


  cyfronet_machines = {}
  for cpu in cpus.keys():
    cyfronet_machines[f"{cpu}Cpu"] = {
        'base_price' :  base_cpu_hour_price,
        'base_memory' : cpus_base_memory[cpu],
        'memory_cost_multiplayer' : cpus_more_memory_factor[cpu],
        'speed': cpus[cpu]
    }
  for gpu in gpus.keys():
    cyfronet_machines[f"{gpu}Gpu"] = {
        'base_price' : base_gpu_hour_price,
        'base_memory' : gpus_base_memory[gpu],
        'memory_cost_multiplayer' : gpus_more_memory_factor[gpu],
        'speed' : gpus[gpu] * gpu_speed_vs_cpu
    }

  def calc_cost(machine , job):
    if job.used_memeory is not None:
      mib_free = 1024 * machine['base_memory']
      cost_power = (job.used_memeory - mib_free) // 1024
    else:
      cost_power = 0
    runtime = job.normalized_runtime
    return runtime * machine['base_price'] * (1 + machine['memory_cost_multiplayer']* cost_power) / machine['speed']

  costs = {}
  runtimes = {}
  for machine in cyfronet_machines:
    machine_cost = []
    machine_runtime = []
    for j in jobs:
      machine_cost.append(calc_cost(cyfronet_machines[machine], j))
      machine_runtime.append( j.normalized_runtime / cyfronet_machines[machine]['speed'])
    costs[machine] = machine_cost
    runtimes[f"{machine}Runtime"] = machine_runtime

  cost_df = pd.DataFrame(data=costs, index = [j.name for j in jobs])
  runtime_df = pd.DataFrame(data=runtimes, index = [j.name for j in jobs])


  

  return cost_df, runtime_df, jobs, paths