#!/usr/bin/python3

from argparse import ArgumentParser
import subprocess
import timeit
import datetime
import time
import os

def process_args():
    parser = ArgumentParser(description="Runs ")
    parser.add_argument('runs', type=int, help='Number of runs to be performed')
    #parser.add_argument('command', type=str, help='Command to be executed')
    parser.add_argument('config_dir', type=str, help='Directory to all MATSim config files')
    parser.add_argument('output_dir', type=str, help='Directory to save measured times')
    args = parser.parse_args()
    return args.runs, args.config_dir, args.output_dir

def run_simulation(command: str):
    command_list = command.split(" ")
    return_code = subprocess.call(command_list)
    return return_code


if __name__ == "__main__":
    runs, config_dir, output_dir = process_args() 
    config_files = [f for f in os.listdir(config_dir) if os.path.isfile(os.path.join(config_dir, f))]
    res_file = output_dir + "/performance_results_" + str(int(time.time())) + ".txt"

    for conf in config_files:
        times = []
        full_command = "java -Xmx16g -cp matsim-0.0.1.jar org.matsim.run.Controler" + " " + config_dir + conf;
        print(f"(TEST) Running: {full_command}")
        print(f"(TEST) Number of runs: {runs}")

        for i in range(runs):
            print(f"(TEST) Simulation {i+1} / {runs} running ...")
            starttime = timeit.default_timer()
            return_code = run_simulation(full_command)
            durtime = timeit.default_timer() - starttime
            times.append(durtime)

        average = sum(times) / len(times)
        formatted_average = str(datetime.timedelta(seconds=average))

        print(f"(TEST) Average runtime {formatted_average}s")
    
        with open(res_file, 'a') as out:
            out.write(f"{conf} {formatted_average}\n")