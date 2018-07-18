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

def get_files(results_dir):
    files = [parse_filename(x) for x in os.listdir(results_dir)]
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
        if experiment_dict['fake_stateful']:
            return "Non-stateful"
        else:
            return experiment_dict['migration']

    data = []
    for filename, config in [x for x in files if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
            experiment_data = [dict(list({
                     "latency": int(x),
                     "ccdf": float(y),
                     "experiment": experiment_name(experiment_dict),
                 }.items()) + list(experiment_dict.items())) for x, y in
                    [x.split('\t')[1:3] for x in f.readlines() if x.startswith('latency_ccdf')]]
            data.extend(experiment_data)

    return (filtering, data)

def memory_timeline_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)

    data = []
    for filename, config in [x for x in files if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
            experiment_data = [dict(list({
                "time": float(x) / 1000000000,
                "RSS": float(y),
                "experiment": "m: {}, q: {}, r: {}".format(experiment_dict['migration'], experiment_dict['queries'], experiment_dict['rate']),
            }.items()) + list(experiment_dict.items())) for x, y in
                    [x.split('\t')[1:3] for x in f.readlines() if x.startswith('statm_RSS')]]
            data.extend(experiment_data[0::10])

    return (filtering, data)

def latency_timeline_plots(results_dir, files, filtering):
    filtering = _filtering_params(files, filtering)
    print(filtering)
    # [0.75, 0.50, 0.25, 0.05, 0.01, 0.001, 0.0]

    data = []
    for filename, config in [x for x in files if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
            for vals in [x.split('\t')[1:] for x in f.readlines() if x.startswith('summary_timeline')]:
                for p, l in [(.25, 1), (.5, 2), (.75, 3), (.99, 4), (.999, 5), (1, 6)]:
                    data.append(dict(list({
                        "time": float(vals[0]) / 1000000000,
                        "latency": int(vals[l]),
                        "p": p,
                        "experiment": "m: {}, q: {}, r: {}, f: {}".format(experiment_dict['migration'], experiment_dict['queries'], experiment_dict['rate'], experiment_dict.get('fake_stateful', False)),
                    }.items()) + list(experiment_dict.items())))

    return (filtering, data)

def plot_name(base_name):
    return os.path.basename(base_name)[:-3]

if __name__ == "__main__" and len(sys.argv) >= 3 and sys.argv[1] == '--list-params':
    results_dir = sys.argv[2]
    print(json.dumps(get_all_params(x[1] for x in get_files(results_dir))))
