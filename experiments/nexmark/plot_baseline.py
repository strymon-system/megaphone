#!/usr/bin/env python3

import sys, shutil, json #, tempfile
from os import listdir

import parse_filename

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

assert(len(sys.argv) >= 2)
results_dir = sys.argv[1]

# tempdir = tempfile.mkdtemp(results_dir.replace("/", "-"))

files = [parse_filename.parse_name(x) for x in listdir(results_dir)]
print("all params:", parse_filename.all_params(list(zip(*files))[1]), file=sys.stderr)

data = []
series = set()
for query in ['q{}'.format(x) for x in range(0, 7)]:
    filtering = [
        ('bin_shift', 8),
        ('duration', 600),
        ('machine_local', True),
        ('processes', 4),
        ('workers', 8),
        ('queries', query),
    ]
    graph_filename = "baseline_latency+{}".format(kv_to_string(dict(filtering)))
    for filename, config in [x for x in files if set(x[1]).issuperset(set(filtering))]:
        experiment_dict = dict(set(config).difference(set(filtering)))
        for k in ['final_config', 'initial_config', 'migration']: del experiment_dict[k]
        experiment = ", ".join("{}: {}".format(k, v) for k, v in sorted(experiment_dict.items(), key=lambda t: t[0]))
        series.add((experiment_dict['rate'], experiment))
        # print(experiment, file=sys.stderr)
        with open("{}/{}/stdout.0".format(results_dir, filename), 'r') as f:
            experiment_data = [{"query": query, "latency": int(x), "ccdf": float(y), "experiment": experiment} for x, y in
                    [x.split('\t')[1:3] for x in f.readlines() if x.startswith('latency_ccdf')]]
            data.extend(experiment_data)
            if len(experiment_data) > 0:
                print(query, experiment_dict['rate'], max(x["latency"] for x in experiment_data), file=sys.stderr)

chart = {
    "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
    "title": "Nexmark: vanilla timely ({} processes x {} workers)".format(dict(filtering)['processes'], dict(filtering)['workers']),
    "data": {"values": data},
    "hconcat": [
        {
            "width": 500,
            "mark": "line",
            "encoding": {
                "x": { "field": "latency", "type": "quantitative", "axis": { "format": "e", "labelAngle": -90 }, "scale": { "type": "log" }},
                "y": { "field": "ccdf", "type": "quantitative" },
                "stroke": { "field": "experiment", "type": "nominal", "legend": None },
                "shape": { "field": "experiment", "type": "nominal", "legend": None },
                "row": { "field": "query", "type": "nominal" },
            },
        },
        {
            "mark": "point",
            "encoding": {
                "shape": { "field": "experiment", "aggregate": "min", "type": "nominal", "legend": None },
                "fill": { "field": "experiment", "aggregate": "min", "type": "nominal", "legend": None },
                "y": {
                    "field": "experiment", "type": "nominal", "title": None,
                    "sort": [b for a, b in sorted(series, key=lambda x: x[0])],
                },
            },
        },
    ]
}

print(json.dumps(chart))

# shutil.rmtree(tempdir)
