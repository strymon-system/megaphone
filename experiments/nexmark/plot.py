#!/usr/bin/env python3

import sys, os, json

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
    return dict((x[0][0], list(set(list(zip(*x))[1]))) for x in zip(*list(ps)))

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

    data = []
    for filename, config in [x for x in files if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
            experiment_data = [dict(list({"latency": int(x), "ccdf": float(y)}.items()) + list(experiment_dict.items())) for x, y in
                    [x.split('\t')[1:3] for x in f.readlines() if x.startswith('latency_ccdf')]]
            data.extend(experiment_data)

    return (filtering, data)

def timeline_plots(results_dir, files, filtering):
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

if __name__ == "__main__" and len(sys.argv) >= 3 and sys.argv[1] == '--list-params':
    results_dir = sys.argv[2]
    print(json.dumps(get_all_params(x[1] for x in get_files(results_dir))))
