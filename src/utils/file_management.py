"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.
"""

import json
import pickle

FILE_EXTENSIONS = {'json_file': 'json',
                   'pickle_file': 'pkl'}


def read_json_file(file_name: str) -> dict:
    with open(file_name, "r") as file:
        data = json.loads(file.read())
    return data


def write_json_file(data, file_name: str) -> None:
    with open(file_name, "w") as file:
        json.dump(data, file, indent=4)


def read_pickle_file(file_name: str):
    with open(file_name, 'rb') as file:
        data = pickle.load(file)
    return data


def write_pickle_file(data, file_name: str) -> None:
    file = check_file_name(file_name, FILE_EXTENSIONS['pickle_file'])
    with open(file, 'wb') as file:
        pickle.dump(data, file)


def check_file_name(file_name: str, extension: str) -> str:
    extension_len = len(extension)
    if file_name[:-extension_len] != extension or file_name[:-extension_len - 1] != '.':
        file_name += f'.{extension}'
    return file_name
