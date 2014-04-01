__author__ = 'srikanthvidapanakal'

from pylab import *
import ConfigParser
import time
from datetime import datetime
import matplotlib.pyplot as plt


conf_file = "/Users/srikanthvidapanakal/Documents/scheduler_logs/config_file.txt"

# Read the config file - Config file contains the scheduler input log file to be processed & generates an output log CSV


def read_config_file(cnf_file):
    config = ConfigParser.ConfigParser()
    config.read(cnf_file)
    input_log = config.get('scheduler_logs', 'input_log')
    output_log = config.get('scheduler_logs', 'output_log')
    return {'input_log': input_log, 'output_log': output_log}


# Process the scheduler log and generate data dictionary per queue and return a list of data dictionaries


def process_scheduler_logs(my_log_dict):
    in_fp = open(my_log_dict['input_log'])
    out_fp = open(my_log_dict['output_log'], 'w')

    pool_map_dict = {}
    pool_reducer_dict = {}
    total_map_dict = {}
    total_reduce_dict = {}

    for line in in_fp.readlines():
        queue_arr = line.split(",")
        queue_arr[0] = time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.gmtime(float(queue_arr[0])))  # Convert Epoch to Human readable format
        pool_name = queue_arr[1]  # Fetch the Pool Name

        # Build pool dictionary for Mapper and Reducers per queue
        if pool_name in pool_map_dict:
            temp_list = pool_map_dict[pool_name]
            temp_list.append([queue_arr[0], queue_arr[1], queue_arr[2], queue_arr[4], queue_arr[5]])
            pool_map_dict[pool_name] = temp_list
        else:
            temp_list = [[queue_arr[0], queue_arr[1], queue_arr[2], queue_arr[4],  queue_arr[5]]]
            pool_map_dict[pool_name] = temp_list

        if pool_name in pool_reducer_dict:
            temp_list = pool_reducer_dict[pool_name]
            temp_list.append([queue_arr[0], queue_arr[1], queue_arr[2], queue_arr[8],  queue_arr[9]])
            pool_reducer_dict[pool_name] = temp_list
        else:
            temp_list = [[queue_arr[0], queue_arr[1], queue_arr[2], queue_arr[8], queue_arr[9]]]
            pool_reducer_dict[pool_name] = temp_list

        # Build pool dictionary for Mapper and Reducer for all queues
        if queue_arr[0] in total_map_dict:
            total_map_dict[queue_arr[0]] += int(queue_arr[5])
        else:
            total_map_dict[queue_arr[0]] = int(queue_arr[5])

        if queue_arr[0] in total_reduce_dict:
            total_reduce_dict[queue_arr[0]] += int(queue_arr[9])
        else:
            total_reduce_dict[queue_arr[0]] = int(queue_arr[9])

        out_fp.write(','.join(queue_arr))

    print(total_map_dict)

    return [pool_map_dict, pool_reducer_dict, total_map_dict, total_reduce_dict]


# Generate a graph


def generate_graph(plist):

    total_map_dict = plist[2]
    total_reduce_dict = plist[3]

    for map_key in plist[0].keys():
        fig, ax = plt.subplots()

        queue_list = plist[0][map_key]
        reduce_list = plist[1][map_key]

        ax.set_xlabel('Date Time', fontsize=18)
        ax.set_ylabel('Map, Reduce', fontsize=18)
        ax.set_title(map_key)

        x = [datetime.strptime(queue_list[i][0], "%Y-%m-%d %H:%M:%S") for i in range(len(queue_list) - 1)]

        y = [queue_list[i][4] for i in range(len(queue_list) - 1)] # Maps Running
        ax.plot(x, y, color="red", label=r"Maps Running")

        y = [queue_list[i][2] for i in range(len(queue_list) - 1)] # Jobs Running
        ax.plot(x, y, color="blue", label=r"Jobs Running")

        y = [reduce_list[i][4] for i in range(len(reduce_list) - 1)] # Reduces Running
        ax.plot(x, y, color="green", label=r"Reduces Running")

        y = [reduce_list[i][3] for i in range(len(reduce_list) - 1)] # Max Reduce Share
        ax.plot(x, y, color="orange", label=r"Max Reduce Share")

        y = [queue_list[i][3] for i in range(len(queue_list) - 1)] # Max Map Share
        ax.plot(x, y, color="purple", label=r"Max Map Share")

        ax.legend(loc=1)  # upper left corner

    show()

    fig, ax = plt.subplots()
    ax.set_xlabel('Date Time', fontsize=18)
    ax.set_ylabel('Mappers', fontsize=18)
    ax.set_title("Total Mappers")

    x = sorted([datetime.strptime(key1, "%Y-%m-%d %H:%M:%S") for key1 in total_map_dict.keys()])
    y = [val1 for val1 in total_map_dict.values()]

    ax.plot(x, y, color="Brown", label=r"Total Mappers")
    ax.legend(loc=1)
    show()

    fig, ax = plt.subplots()
    ax.set_xlabel('Date Time', fontsize=18)
    ax.set_ylabel('Reducers', fontsize=18)
    ax.set_title("Total Reducers")

    y = [val1 for val1 in total_reduce_dict.values()]
    ax.plot(x, y, color="magenta", label=r"Total Reducers")
    ax.legend(loc=1)
    show()

# Main
log_dict = read_config_file(conf_file)
pool_list = process_scheduler_logs(log_dict)
generate_graph(pool_list)









