"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.
"""

import json
import pickle

import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from wfcommons.wfchef.utils import create_graph


def create_energy_histogram(output_name):
    with open(output_name, 'rb') as solution_file:
        solved_cqm = pickle.load(solution_file)

    correct_solutions = [e for i, e in enumerate(solved_cqm["solution"]["data_vectors"]["energy"])
                         if solved_cqm["solution"]["data_vectors"]["is_feasible"][i]]
    incorrect_solutions = [e for i, e in enumerate(solved_cqm["solution"]["data_vectors"]["energy"])
                           if not solved_cqm["solution"]["data_vectors"]["is_feasible"][i]]

    label = ["Correct", "Incorrect"]

    plot_energy_histogram(correct_solutions, incorrect_solutions, output_name, label)


def save_figure():
    pass


def plot_energy_histogram(correct_solutions, incorrect_solutions, title, label):
    plt.rcParams["figure.figsize"] = (6, 4)
    plt.hist([correct_solutions, incorrect_solutions], bins=30, stacked=True, label=label)
    plt.title(title)
    plt.xlabel("Energy of the solution")
    plt.ylabel("Number of returned solutions in buckets")
    plt.legend()
    plt.grid()
    plt.show()


def display_graph(graph, labels, colors):
    pos = graphviz_layout(graph, prog='dot')
    plt.figure(figsize=(21, 10))
    nx.draw(graph, pos=pos, arrows=True, with_labels=True, labels=labels, font_size=12, node_size=5000,
            node_color=colors)
    plt.draw()


def draw_workflow_schema(output_file):  # todo create_workflow_schema
    with open(output_file) as f:  # todo move this to another part
        data = json.loads(f.read())

    DAG = create_graph(output_file)
    new_labels = {task["name"]: f'{task["name"].split("_")[0]}\n{task["machine"]}' for task in
                  data["workflow"]["tasks"]}
    new_labels['SRC'] = 'SRC'
    new_labels['DST'] = 'DST'

    colors = []
    for node in DAG.nodes:
        if node in {'SRC', 'DST'}:
            colors.append(plt.cm.tab10.colors[0])
        elif "AresCpu" in new_labels[node]:
            colors.append(plt.cm.tab10.colors[1])
        elif "AresGpu" in new_labels[node]:
            colors.append(plt.cm.tab10.colors[2])
        elif "PrometeusCpu" in new_labels[node]:
            colors.append(plt.cm.tab10.colors[3])
        elif "PrometeusGpu" in new_labels[node]:
            colors.append(plt.cm.tab10.colors[4])
        elif "ZeusCpu" in new_labels[node]:
            colors.append(plt.cm.tab10.colors[5])

    display_graph(DAG, new_labels, colors)
