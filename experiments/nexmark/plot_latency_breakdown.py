#!/usr/bin/env python3

# ./plot_latency_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import sys, os, shutil, json
import argparse
import plot
from collections import defaultdict

parser = argparse.ArgumentParser(description="Plot")
parser.add_argument('results_dir')
parser.add_argument('filtering')
parser.add_argument('primary_group')
parser.add_argument('secondary_group')
parser.add_argument('--json', action='store_true')
parser.add_argument('--gnuplot', action='store_true')
parser.add_argument('--terminal', default="pdf")
args = parser.parse_args()

results_dir = args.results_dir
files = plot.get_files(results_dir)
# for f in files:
#      print(f[1])
filtering = eval(args.filtering)

graph_filtering, data, experiments = plot.latency_breakdown_plots(results_dir, files, filtering)

commit = results_dir.rstrip('/').split('/')[-1]
# print("commit:", commit, file=sys.stderr)

if len(data) == 0:
    print("No data found: {}".format(filtering), file=sys.stderr)
    exit(0)


# Try to extract duration from filter pattern
duration = next((x[1] for x in filtering if x[0] == 'duration'), 450)

plot.ensure_dir("charts/{}".format(commit))

if args.json:
    extension = "json"
elif args.gnuplot:
    extension = "gnuplot"
else:
    extension = "html"

plot_name = plot.kv_to_string(dict(graph_filtering))

def get_chart_filename(extension):
    graph_filename = "{}+{}.{}".format(plot.plot_name(__file__), plot_name, extension)
    return "charts/{}/{}".format(commit, graph_filename)

chart_filename = get_chart_filename(extension)
# title = ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering.items(), key=lambda t: t[0]))
title = plot.kv_to_name(graph_filtering)

# for ds in data:
#     print(ds)

if args.gnuplot:

    def get_key(config, key):
        for k, v in config:
            if k == key:
                return v
        return None

    # Generate dataset
    all_headers = set()

    all_primary = defaultdict(list)
    all_secondary = defaultdict(list)

    for ds in data:
        for d in ds:
            for k in d.keys():
                all_headers.add(k)
    for config, ds in zip(iter(experiments), iter(data)):
        all_primary[get_key(config, args.primary_group)].append((config, ds))
        all_secondary[get_key(config, args.secondary_group)].append((config, ds))

    if len(all_primary) < 1 or len(all_secondary) < 1:
        print("nothing found", file=sys.stderr)
        print("all_primary: {}, all_secondary={}".format(all_primary.keys(), all_secondary.keys()), file=sys.stderr)
        exit(0)

    all_headers.remove("experiment")
    all_headers = sorted(all_headers)

    migration_to_index = defaultdict(list)

    graph_filtering_bin_shift = dict(graph_filtering)

    dataset_filename = get_chart_filename("dataset")

    all_configs = []

    def format_key(group, key):
        if group == "domain":
            return "{}M".format(int(key/1000000))
        return key

    with open(dataset_filename, 'w') as c:
        index = 0
        # print(" ".join(all_headers), file=c)
        for key, item in sorted(all_primary.items()):
            key = format_key(args.primary_group, key)
            print("\"{}\"".format(str(key).replace("_", "\\\\_")), file=c)
            for config, ds in item:
                all_configs.append(config)
                # print("\"{}\"".format(plot.kv_to_name(config).replace("_", "\\\\_")), file=c)
                for d in ds:
                    print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
                # print("\n", file=c)
            index += 1
            print("\n", file=c)

        for key, item in sorted(all_secondary.items()):
            key = format_key(args.secondary_group, key)
            print("\"{}\"".format(str(key).replace("_", "\\\\_")), file=c)
            for config, ds in item:
                all_configs.append(config)
                # print("\"{}\"".format(plot.kv_to_name(config).replace("_", "\\\\_")), file=c)
                for d in ds:
                    print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
            print("\n", file=c)
            # index += 1

    config_reorder = sorted(map(lambda x: (x[1], x[0]), enumerate(all_configs)))

    # Generate gnuplot script
    gnuplot_terminal = args.terminal
    gnuplot_out_filename = get_chart_filename(gnuplot_terminal)
    duration_index = all_headers.index("migration_duration") + 1
    p_index = all_headers.index("max_p_1") + 1
    p50_index = all_headers.index("max_p_.5") + 1
    filtered_p_index = all_headers.index("filtered_max_p_1") + 1
    # fix long titles
    if len(title) > 79:
        idx = title.find(" ", int(len(title) / 2))
        if idx != -1:
            title = "{}\\n{}".format(title[:idx], title[idx:])
    with open(chart_filename, 'w') as c:
        print("""\
set terminal {gnuplot_terminal} font \"LinuxLibertine, 16\" enhanced dashed size 4.5, 3 crop
set logscale y
set logscale x

# set format y "10^{{%T}}"
set grid xtics ytics

set xlabel "Max latency [ns]"
set ylabel "Duration"

set key title "{key1}" right outside top width 3 #Left at screen 1, .95
# set key right center
set size square
# unset key

# set xrange [10**7:10**11]

set output '{gnuplot_out_filename}'
stats '{dataset_filename}' using 0 nooutput
if (STATS_blocks == 0) exit
set for [i=1:STATS_blocks] linetype i dashtype i
# set for [i=1:STATS_blocks] linetype i dashtype i
# set title "{title}"
# set yrange [10**floor(log10(STATS_min)): 10**ceil(log10(STATS_max))]
set yrange [10**9:10**12]
set xrange [5*10**6: 2*10**11]
set multiplot
set origin 0, 0
set rmargin at screen .75
plot for [i={index}:*] '{dataset_filename}' using {p_index}:{duration_index} index (i+0) with points title columnheader(1)
set key title "{key2}" right outside bottom width 3 #Right at screen 1, .5
unset grid
set xlabel " "
set ylabel " "
plot for [i=0:{index}-1] '{dataset_filename}' using {p_index}:{duration_index} index (i+0) with lines title columnheader(1)
unset multiplot

# set xlabel "50% latency [ns]"
# plot for [i={index}:*] '{dataset_filename}' using {p50_index}:{duration_index} index (i+0) with points title columnheader(1), \\
#  for [i=0:{index}-1] '' using {p50_index}:{duration_index} index (i+0) with lines title columnheader(1)

# set xlabel "Filtered max latency [ns]"
# plot for [i={index}:*] '{dataset_filename}' using {filtered_p_index}:{duration_index} index (i+0) with points title columnheader(1), \\
#  for [i=0:{index}-1] '' using {filtered_p_index}:{duration_index} index (i+0) with lines title columnheader(1)
        """.format(dataset_filename=dataset_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   duration_index=duration_index,
                   p_index=p_index,
                   p50_index=p50_index,
                   filtered_p_index=filtered_p_index,
                   title=title.replace("_", "\\\\_"),
                   duration=duration,
                   num_plots=len(migration_to_index),
                   index=index,
                   key2=args.primary_group.replace("_", "\\\\_"),
                   key1=args.secondary_group.replace("_", "\\\\_"),
                   ), file=c)

else: # json or html

    vega_lite = {
        "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
        "title": ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering, key=lambda t: t[0])),
        "facet": {
            "column": {"field": "experiment", "type": "nominal"},
            "row": {"field": "queries", "type": "nominal"},
        },
        "spec": {
            "width": 600,
            "layer": [
                {
                    "mark": {
                        "type": "line",
                        "clip": True,
                    },
                    "encoding": {
                        "x": {"field": "time", "type": "quantitative", "axis": {"labelAngle": -90},
                              "scale": {"domain": [duration / 2 - 20, duration / 2 + 20]}},
                        # "x": { "field": "time", "type": "quantitative", "axis": { "labelAngle": -90 }, "scale": {"domain": [duration / 2 - 20, duration / 2 + 20]} },
                        "y": {"field": "latency", "type": "quantitative", "axis": {"format": "e", "labelAngle": 0},
                              "scale": {"type": "log"}},
                        "color": {"field": "p", "type": "nominal"},
                    },
                },
                {
                    "mark": "rule",
                    "encoding": {
                        "x": {
                            "aggregate": "mean",
                            "field": "time",
                            "type": "quantitative",
                        },
                        "color": {"value": "grey"},
                        "size": {"value": 1}
                    }
                }
            ]
        },
        "data": {
            "values": data
        },
    };

    if args.json:
        with open(chart_filename, 'w') as c:
            print(json.dumps(vega_lite), file=c)
    else:
        print(sorted(graph_filtering, key=lambda t: t[0]))
        vega_lite["title"] = title
        html = """
        <!DOCTYPE html>
        <html>
        <head>
          <script src="https://cdn.jsdelivr.net/npm/vega@3"></script>
          <script src="https://cdn.jsdelivr.net/npm/vega-lite@2"></script>
          <script src="https://cdn.jsdelivr.net/npm/vega-embed@3"></script>
        </head>
        <body>

          <div id="vis"></div>

          <script type="text/javascript">
            const vega_lite_spec = """ + \
               json.dumps(vega_lite) + \
               """

                   vegaEmbed("#vis", vega_lite_spec, { "renderer": "svg" });
                 </script>
               </body>
               </html>
               """
        with open(chart_filename, 'w') as c:
            print(html, file=c)

print(chart_filename)
print(os.getcwd() + "/" + chart_filename, file=sys.stderr)
