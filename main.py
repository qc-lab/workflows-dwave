#!/bin/env python3

from pprint import pprint
import sys, getopt
from prepare_problem import get_problem_data
import json 
from gurobi_solver import gp_solution 
from cqm_solution import cqm_solution

def main(argv):
    inputfile = ''
    outputfile = ''
    machinefile = None
    deadline = 1
    solver = gp_solution
    try:
        opts, args = getopt.getopt(argv,"hi:o:m:d:q",["in_file=","out_file=","machine_file=", "deadline=", "quantum"])
    except getopt.GetoptError:
        print ('main.py -i <input_workflow> -o <output_workflow> -m <target_machines_file> -d <deadline_in_ms> -q')
        sys.exit(1)
    for opt, arg in opts:
        if opt == '-h':
            print ('main.py -i <input_workflow> -o <output_workflow> -m <target_machines_file> -d <deadline_in_ms> -q')
            sys.exit(2)
        elif opt in ("-i", "--in_file"):
            inputfile = arg
        elif opt in ("-o", "--out_file"):
            outputfile = arg
        elif opt in ("-m", "--machine_file"):
            machinefile = arg
        elif opt in ("-d", "--deadline"):
            deadline = int(arg)
        elif opt in ("-q", "--quantum"):
            solver = cqm_solution
    if inputfile is None or outputfile is None:
        print ('main.py -i <input_workflow> -o <output_workflow> -m <target_machines_file> -d <deadline_in_ms> -q')

    with open(inputfile) as f:
        input = json.loads(f.read())

    machines = None
    if machinefile is not None:
        with open(machinefile) as f:
            machines = json.loads(f.read())

    cost, runtimes, jobs, paths = get_problem_data(input, machines)

    solution = solver(cost, jobs, runtimes, paths, deadline)

    output = input.copy()
    final_jobs = []
    for job in input["workflow"]["jobs"]:
        final_job = job.copy()
        final_job["machine"] = solution[final_job["name"]]
        final_jobs.append(final_job)
    output["workflow"]["jobs"] = final_jobs
    output["workflow"]["machines"] = machines 
    with open(outputfile, "w") as f:
        json.dump(output, f, indent=4)
    
if __name__ == "__main__":
   main(sys.argv[1:])