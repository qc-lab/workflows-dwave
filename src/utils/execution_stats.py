"""This work was partially funded by the EuroHPC PL project,
funded in the frame of Smart Growth Operational Programme, topic 4.2.
"""

import time


def calculate_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        output = func(*args, **kwargs)
        time_diff = time.time() - start
        print(f'Finished in {round(time_diff, 3)} seconds.')
        return output

    return wrapper
