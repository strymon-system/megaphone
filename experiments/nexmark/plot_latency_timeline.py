#!/usr/bin/env python3

# ./plot_latency_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import sys, os, shutil, json
import argparse
import plot
from collections import defaultdict

parser = argparse.ArgumentParser(description="Plot")
parser.add_argument('results_dir')
parser.add_argument('filtering')
parser.add_argument('--json', action='store_true')
parser.add_argument('--gnuplot', action='store_true')
parser.add_argument('--terminal', default="pdf")
parser.add_argument('--filter', nargs='+', default=[])
parser.add_argument('--rename', default="{}")
parser.add_argument('--name')
args = parser.parse_args()

results_dir = args.results_dir
files = plot.get_files(results_dir)
# for f in files:
#      print(f[1])
filtering = eval(args.filtering)
rename = eval(args.rename)

if len(args.filter) > 0:
    graph_filtering = []
    data = []
    experiments = []
    for filter in args.filter:
        filter = eval(filter)
        # print(filtering+filter, file=sys.stderr)
        gf, da, ex = plot.latency_timeline_plots(results_dir, files, filtering + filter)
        for f in ex:
            # print(f, file=sys.stderr)
            f.extend(filter)
        graph_filtering.extend(gf)
        data.extend(da)
        experiments.extend(ex)
    # print("experiments", experiments, file=sys.stderr)
else:
    graph_filtering, data, experiments = plot.latency_timeline_plots(results_dir, files, filtering)

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
    name = ""
    if args.name:
        name = "_{}".format(args.name)
    graph_filename = "{}{}+{}.{}".format(plot.plot_name(__file__), name, plot_name, extension)
    return "charts/{}/{}".format(commit, graph_filename)

chart_filename = get_chart_filename(extension)
# title = ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering.items(), key=lambda t: t[0]))
title = plot.kv_to_name(graph_filtering)

if args.gnuplot:

    # Generate dataset
    all_headers = set()
    all_percentiles = set()
    all_migrations = set()

    for ds in data:
        for d in ds:
            for k in d.keys():
                all_headers.add(k)
            all_percentiles.add(d["p"])
            all_migrations.add(d.get("migration", None))
    all_headers.remove("experiment")
    all_headers = sorted(all_headers)

    migration_to_index = defaultdict(list)

    graph_filtering_bin_shift = dict(graph_filtering)

    dataset_filename = get_chart_filename("dataset")

    all_configs = []

    def get_key(config, key):
        for k, v in config:
            if k == key:
                return v
        return None

    order = { None: 0, 'sudden': 1, 'fluid': 2, 'batched': 3}

    with open(dataset_filename, 'w') as c:
        index = 0
        # print(" ".join(all_headers), file=c)
        for p in sorted(all_percentiles):
            for ds, config in zip(iter(data), iter(experiments)):
                key = get_key(config, "migration")
                if key in rename:
                    key = rename[key]
                else:
                    key = (key, order[key])
                migration_to_index[key].append(index)
                config_with_p = config + [('p', p)]
                all_configs.append(config_with_p)
                if p == 1:
                    name = "max"
                else:
                    name = plot.kv_to_name([('p', p)])
                # print(p, name, file=sys.stderr)
                print("\"{}\"".format(name.replace("_", "\\\\_")), file=c)
                for d in ds:
                    if d["p"] == p:
                        print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
                print("\n", file=c)
                index += 1

    config_reorder = sorted(map(lambda x: (x[1], x[0]), enumerate(all_configs)))

    # Generate gnuplot script
    gnuplot_terminal = args.terminal
    gnuplot_out_filename = get_chart_filename(gnuplot_terminal)
    time_index = all_headers.index("time") + 1
    latency_index = all_headers.index("latency") + 1
    # fix long titles
    if len(title) > 79:
        idx = title.find(" ", int(len(title) / 2))
        if idx != -1:
            title = "{}\\n{}".format(title[:idx], title[idx:])
    with open(chart_filename, 'w') as c:
        print("""\
set terminal {gnuplot_terminal} font \"TimesNewRoman, 16\"
if (!exists("MP_LEFT"))   MP_LEFT = .11
if (!exists("MP_RIGHT"))  MP_RIGHT = .98
if (!exists("MP_BOTTOM")) MP_BOTTOM = .27
if (!exists("MP_TOP"))    MP_TOP = .86
if (!exists("MP_GAP"))    MP_GAP = 0.03

max(x,y) = (x < y) ? y : x
min(x,y) = (x < y) ? x : y

set logscale y

set format y "10^{{%T}}"
set grid xtics ytics

set ylabel "Latency [ms]"
set xlabel "Time [s]"
set xtics min(200, {duration}/20*10)
set mxtics 4

set xrange [0:{duration}*.99]
# set xrange [1000:1300]

set key at screen .5, screen 0.01 center bottom maxrows 1 maxcols 10 
# unset key

set output '{gnuplot_out_filename}'
stats '{dataset_filename}' using {latency_index} nooutput
if (STATS_blocks == 0) exit
set for [i=1:STATS_blocks] linetype i dashtype i
# set yrange [10**floor(log10(STATS_min)): 10**ceil(log10(STATS_max))]
set yrange [9*10**-1:10**max(4, ceil(log10(STATS_max)))]
set bmargin at screen 0.24
set multiplot layout 1, {num_plots} columnsfirst \\
              margins screen MP_LEFT, MP_RIGHT, MP_BOTTOM, MP_TOP spacing screen MP_GAP
        """.format(dataset_filename=dataset_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   latency_index=latency_index,
                   time_index=time_index,
                   title=title.replace("_", "\\\\_"),
                   duration=duration,
                   num_plots=len(migration_to_index)
                   ), file=c)

        def print_plots():
            for key in sorted(migration_to_index, key=lambda x: x[1]):
                print("""\
    set title "{key}"
    plot for [i in "{indexes}"] '{dataset_filename}' using {time_index}:{latency_index} index (i+0) title columnheader(1) with lines linewidth 1
    unset key
    set format y ''; unset ylabel
                """.format(key=(key[0] if len(migration_to_index) != 1 else ""),
                           indexes=" ".join(map(str, sorted(migration_to_index[key], reverse=True))),
                           dataset_filename=dataset_filename,
                           time_index=time_index,
                           latency_index=latency_index
                           ), file=c)

        print_plots()
        print("""\
set key at screen .5, screen 0.01 center bottom maxrows 1 maxcols 10 
set multiplot layout 1, {num_plots} columnsfirst \\
              margins screen MP_LEFT, MP_RIGHT, MP_BOTTOM, MP_TOP spacing screen MP_GAP

set xtics 100
set xrange [{duration}*.63:{duration}*.85]
set ylabel "Latency [ms]"
set format y "10^{{%T}}"
        """.format(duration=duration, num_plots=len(migration_to_index)), file=c)

        print_plots()
        print("""\
set key at screen .5, screen 0.01 center bottom maxrows 1 maxcols 10 
set multiplot layout 1, {num_plots} columnsfirst \\
              margins screen MP_LEFT, MP_RIGHT, MP_BOTTOM, MP_TOP spacing screen MP_GAP
set xtics 20
set xrange [{duration}*.65:{duration}*.71]
set ylabel "Latency [ms]"
set format y "10^{{%T}}"
        """.format(duration=duration, num_plots=len(migration_to_index)), file=c)
        print_plots()
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
