#!/usr/bin/env python3

import sys, os, json, itertools
from collections import defaultdict

def parse_filename(name):
    kvs = name.rstrip().split('+')
    def parsekv(kv):
        k, v = kv.split('=')
        if v.isdigit():
            v = int(v)
        elif v == "True":
            v = True
        elif v == "False":
            v = False
        return (k, v)
    return (name, list(parsekv(kv) for kv in kvs))

def get_all_params(ps):
    result = defaultdict(set)
    for k, v in list(itertools.chain.from_iterable(ps)):
        result[k].add(v)
    return {k: sorted(v) for k, v in result.items()}

def ensure_dir(name):
    if not os.path.exists(name):
        os.makedirs(name)

def kv_to_string(config):
    keys = sorted(config.keys())
    kv_pairs = []
    for key in keys:

        value = config[key]
        if isinstance(value, (str, int)):
            kv_pairs.append((key, value))
        else:
            kv_pairs.append((key, "|".join(value)))
    return "+".join(map(lambda p: "{}={}".format(p[0], p[1]), kv_pairs))

def kv_to_name(config):
    kv_pairs = []
    for key, value in sorted(config):

        if isinstance(value, (str, int, float)):
            kv_pairs.append((key, value))
        else:
            kv_pairs.append((key, ", ".join(value)))
    return "; ".join(map(lambda p: "{}: {}".format(p[0], p[1]), kv_pairs))

def get_files(results_dir):
    def is_done(p):
        return os.path.exists("{}/{}/done".format(results_dir, p))
    files = [parse_filename(x) for x in os.listdir(results_dir) if is_done(x)]
    # print("all params:", get_all_params(list(zip(*files))[1]), file=sys.stderr)
    return files

def _filtering_params(files, filtering):
    all_params = get_all_params(x[1] for x in files)
    single_params_value = set(x[0] for x in all_params.items() if len(x[1]) == 1)
    filtering_params = set(x[0] for x in filtering)
    for additional_filtering_param in single_params_value.difference(filtering_params):
        filtering.append((additional_filtering_param, all_params[additional_filtering_param][0]))
    return filtering

def latency_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)

    def experiment_name(experiment_dict):
        if not experiment_dict.get("queries", "").endswith("-flex"):
            return "Optimized"
        elif experiment_dict.get('fake_stateful', False):
            return "Non-stateful"
        else:
            return experiment_dict.get('migration', 'fluid')

    data = []
    experiments = []
    for filename, config in [x for x in sorted(files, key=lambda x: x[1]) if set(x[1]).issuperset(set(filtering))]:
        # print(filename)
        experiment_dict = dict(set(config).difference(set(filtering)))
        experiments.append(sorted(experiment_dict.items()))
        try:
            with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
                experiment_data = [dict(list({
                         "latency": int(x),
                         "ccdf": float(y),
                         "experiment": experiment_name(experiment_dict),
                     }.items()) + list(experiment_dict.items())) for x, y in
                        [x.split('\t')[1:3] for x in f.readlines() if x.startswith('latency_ccdf')]]
                data.append(experiment_data)
        except IOError as e:
            print("Unexpected error:", e)
            pass

    return (filtering, data, experiments)

def memory_timeline_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)

    data = []
    experiments = []
    for filename, config in [x for x in sorted(files, key=lambda x: x[1]) if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        experiments.append(sorted(experiment_dict.items()))
        try:
            with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
                experiment_data = [dict(list({
                    "time": float(x) / 1000000000,
                    "RSS": float(y),
                    "experiment": "m: {}, q: {}, r: {}".format(experiment_dict.get('migration', "None"), experiment_dict.get('queries', ""), experiment_dict.get('rate', 0)),
                }.items()) + list(experiment_dict.items())) for x, y in
                        [x.split('\t')[1:3] for x in f.readlines() if x.startswith('statm_RSS')]]
                # data.extend(experiment_data[0::10])
                data.append(experiment_data)
        except IOError as e:
            print("Unexpected error:", e)
            pass

    return (filtering, data, experiments)

def latency_timeline_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)
    # print(filtering)
    # [0.75, 0.50, 0.25, 0.05, 0.01, 0.001, 0.0]

    data = []
    experiments = []
    for filename, config in [x for x in sorted(files, key=lambda x: x[1]) if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        experiments.append(sorted(experiment_dict.items()))
        experiment_data = []
        try:
            with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
                for vals in [x.split('\t')[1:] for x in f.readlines() if x.startswith('summary_timeline')]:
                    for p, l in [(.25, 1), (.5, 2), (.75, 3), (.99, 4), (.999, 5), (1, 6)]:
                        experiment_data.append(dict(list({
                                                             "time": float(vals[0]) / 1000000000,
                                                             "latency": int(vals[l]),
                                                             "p": p,
                                                             "experiment": "m: {}, r: {}, f: {}".format(experiment_dict.get('migration', "?"), experiment_dict.get('rate', 0), experiment_dict.get('fake_stateful', False)),
                                                         }.items()) + list(experiment_dict.items())))
                data.append(experiment_data)
        except IOError as e:
            print("Unexpected error:", e)
            pass

    return (filtering, data, experiments)

def latency_breakdown_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)
    # print(filtering)
    # [0.75, 0.50, 0.25, 0.05, 0.01, 0.001, 0.0]

    data = []
    experiments = []
    for filename, config in [x for x in sorted(files, key=lambda x: x[1]) if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        experiments.append(sorted(experiment_dict.items()))
        experiment_data = []
        try:
            with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
                max_latency = [0 for _ in range(6)]
                migration_start = None
                migration_end = None
                for vals in [x.strip().split('\t')[1:] for x in f.readlines() if x.startswith('summary_timeline')]:
                    # print(vals, migration_start, migration_end, max_latency)
                    if int(vals[1]) > 1000000:
                        if migration_start is None:
                            migration_start = int(vals[0])
                        for i in range(0, len(max_latency)):
                            v = int(vals[i + 1])
                            max_latency[i] = max(max_latency[i], v)
                    elif migration_start is not None and migration_end is None:
                        migration_end = int(vals[0])

                if migration_end is None:
                    # migration did not terminate, or classifier is wrong
                    migration_end = int(vals[0])
                if migration_start is not None:
                    # We found start of migration
                    experiment_data.append(dict(list({
                            "migration_start": migration_start,
                            "migration_end": migration_end,
                            "migration_duration": migration_end - migration_start,
                             "max_p_.25": max_latency[0],
                             "max_p_.5": max_latency[1],
                             "max_p_.75": max_latency[2],
                             "max_p_.99": max_latency[3],
                             "max_p_.999": max_latency[4],
                             "max_p_1": max_latency[5],
                            "experiment": "m: {}, r: {}, f: {}".format(experiment_dict.get('migration', "?"), experiment_dict.get('rate', 0), experiment_dict.get('fake_stateful', False)),
                         }.items()) + list(experiment_dict.items())))
                    data.append(experiment_data)
        except IOError as e:
            print("Unexpected error:", e)
            pass

    return (filtering, data, experiments)

def plot_name(base_name):
    return os.path.basename(base_name)[:-3]

def quote_str(s):
    if isinstance(s, str):
        return "{}".format(s)
    return str(s)

if __name__ == "__main__" and len(sys.argv) >= 3 and sys.argv[1] == '--list-params':
    results_dir = sys.argv[2]
    print(json.dumps(get_all_params(x[1] for x in get_files(results_dir))))
