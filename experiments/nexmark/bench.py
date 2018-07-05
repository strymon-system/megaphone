#!/usr/bin/env python3

import sys, math, os
import argparse
from collections import namedtuple, defaultdict
from HopcroftKarp import HopcroftKarp

from patterns import InitialPattern, SuddenMigrationPattern, FluidMigrationPattern, BatchedFluidMigrationPattern, PatternGenerator
import experiments
from experiments import eprint, ensure_dir, current_commit, run_cmd, wait_all

argparser = argparse.ArgumentParser(description='Process some integers.')
argparser.add_argument("--clusterpath", help='the path of this repo on the cluster machines', required=True)
argparser.add_argument("--serverprefix", help='an ssh username@server prefix, e.g. andreal@fdr, the server number will be appended', required=True)
argparser.add_argument("--dryrun", help='don\'t actually do anything', action='store_true')
argparser.add_argument("--machineid", help='choose a machine for machine-local experiments (can be overridden per-experiment)', type=int)
argparser.add_argument("--baseid", help='choose the first machine for this experiment (can be overridden per-experiment)', type=int)
args = argparser.parse_args()
experiments.cluster_src_path = args.clusterpath
experiments.cluster_server = args.serverprefix
dryrun = args.dryrun
single_machine_id = args.machineid
base_machine_id = args.baseid
if dryrun:
    eprint("dry-run")

# - Generic interface to run benchmark with a configuration
# - Runner to run experiments remotely
# - Specific benchmarks to execute

class Experiment(object):

    def __init__(self, name, **config):
        self._name = name
        self._config = config
        self._migration = self._config["migration"]
        self._bin_shift = self._config["bin_shift"]
        self._workers = self._config["workers"]
        self._processes = self._config["processes"]
        self._duration = self._config["duration"]
        self._rate = self._config["rate"]
        self._initial_config = self._config["initial_config"]
        self._final_config = self._config["final_config"]
        self._machine_local = self._config["machine_local"]

        self.single_machine_id = single_machine_id
        self.base_machine_id = base_machine_id

    def get_directory_name(self):
        keys = sorted(self._config.keys())
        kv_pairs = []
        for key in keys:

            value = self._config[key]
            if isinstance(value, (str, int)):
                kv_pairs.append((key, value))
            else:
                kv_pairs.append((key, "|".join(value)))
        queries = "_".join(map(lambda p: "{}={}".format(p[0], p[1]), kv_pairs))
        return "{}/{}".format(current_commit, queries)

    def get_setup_directory_name(self):
        return "{}/{}".format("setups", self.get_directory_name())

    def get_result_directory_name(self):
        return "{}/{}".format("results", self.get_directory_name())

    def get_setup_file_name(self, name):
        return "{}/{}".format(self.get_setup_directory_name(), name)

    def get_result_file_name(self, name, process):
        return "{}/{}.{}".format(self.get_result_directory_name(), name, process)

    def commands(self):
        print(vars(self))
        migration_pattern_file_name = self.get_setup_file_name("migration_pattern")

        initial_pattern = InitialPattern(self._bin_shift, self._workers)

        if self._migration == "sudden":
            pattern = SuddenMigrationPattern
        elif self._migration == "fluid":
            pattern = FluidMigrationPattern
        elif self._migration == "batched":
            pattern = BatchedFluidMigrationPattern
        else:
            raise ValueError("Unknown migration pattern: {}".format(self._migration))

        if self._initial_config == "uniform":
            initial_config = initial_pattern.generate_uniform()
        elif self._initial_config == "uniform_skew":
            initial_config = initial_pattern.generate_uniform_skew()
        elif self._initial_config == "half":
            initial_config = initial_pattern.generate_half()

        if self._final_config == "uniform":
            final_config = initial_pattern.generate_uniform()
        elif self._final_config == "uniform_skew":
            final_config = initial_pattern.generate_uniform_skew()
        elif self._final_config == "half":
            final_config = initial_pattern.generate_half()

        ensure_dir(self.get_setup_directory_name())

        with open(migration_pattern_file_name, "w") as f:
            eprint("writing migration pattern to {}".format(migration_pattern_file_name))
            PatternGenerator(
                    pattern,
                    initial_pattern.generate_uniform(),
                    initial_pattern.generate_uniform_skew()).write(f)

        hostfile_file_name = self.get_setup_file_name("hostfile")
        with open(hostfile_file_name, 'w') as f:
            eprint("writing hostfile to {}".format(hostfile_file_name))
            if not self._machine_local:
                assert(self.base_machine_id is not None)
                for p in range(0, self._processes):
                    f.write("{}{}:3210\n".format(experiments.cluster_server, self.base_machine_id + p))
            else:
                assert(self.single_machine_id is not None)
                for p in range(0, self._processes):
                    f.write("{}{}:{}\n".format(experiments.cluster_server, self.single_machine_id, 3210 + p))


        command_prefix = "~/.cargo/bin/cargo run --bin timely --release --no-default-features"
        if not self._machine_local:
            make_command = lambda p: (
                    command_prefix +
                    " --features dynamic_scaling_mechanism/bin-{} {} {} {}/{} {}".format(
                        self._bin_shift, self._rate, self._duration, os.getcwd(), migration_pattern_file_name, " ".join(self._config["query"])) +
                    " --hostfile {} -n {} -p {} -w {}".format(hostfile_file_name, self._processes, p, self._workers))
            commands = [(self.base_machine_id + p, make_command(p), self.get_result_file_name("stdout", p)) for p in range(0, self._processes)]
            return commands
        else:
            assert(self.single_machine_id is not None)
            make_command = lambda p: (
                    command_prefix +
                    " --features dynamic_scaling_mechanism/bin-{} {} {} {}/{} {}".format(
                        self._bin_shift, self._rate, self._duration, os.getcwd(), migration_pattern_file_name, " ".join(self._config["query"])) +
                    " --hostfile {} -n {} -p {} -w {}".format(hostfile_file_name, self._processes, p, self._workers))
            commands = [(self.single_machine_id, make_command(p), self.get_result_file_name("stdout", p)) for p in range(0, self._processes)]
            return commands

    def run_commands(self):
        eprint("running experiment, results in {}".format(self.get_result_directory_name()), level="info")
        if not dryrun:
            ensure_dir(self.get_result_directory_name())
        wait_all([run_cmd(c, redirect=r, background=True, node=p, dryrun=dryrun) for p, c, r in self.commands()])

workers = 8 
for bin_shift in range(int(math.log2(workers)), 16):
    for migration in ["batched", "sudden", "fluid"]:
        experiment = Experiment(
                "exp1",
                duration=120,
                rate=10000000 // workers, # Rate is per worker
                query=["q0"],
                migration=migration,
                bin_shift=bin_shift,
                workers=workers,
                processes=2,
                initial_config="uniform",
                final_config="uniform_skew",
                machine_local=True)
        experiment.single_machine_id = 1
        experiment.run_commands()
