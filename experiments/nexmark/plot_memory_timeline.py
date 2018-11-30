#!/usr/bin/env python3

# ./plot_memory_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

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
        gf, da, ex = plot.memory_timeline_plots(results_dir, files, filtering + filter)
        for f in ex:
            # print(f, file=sys.stderr)
            f.extend(filter)
        graph_filtering.extend(gf)
        data.extend(da)
        experiments.extend(ex)
    # print("experiments", experiments, file=sys.stderr)
else:
    graph_filtering, data, experiments = plot.memory_timeline_plots(results_dir, files, filtering)

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
    def name(config):
        config = dict(config)
        migration = config.get("migration", "UNKNOWN FIXME")
        if migration in rename:
            migration = rename[migration]
        return migration


# Generate dataset
    all_headers = set()

    for ds in data:
        for d in ds:
            for k in d.keys():
                all_headers.add(k)
    all_headers.remove("experiment")
    all_headers = sorted(all_headers)

    graph_filtering_bin_shift = dict(graph_filtering)

    dataset_filename = get_chart_filename("dataset")

    all_configs = []

    def get_key(config, key):
        for k, v in config:
            if k == key:
                return v
        return None

    with open(dataset_filename, 'w') as c:
        index = 0
        # print(" ".join(all_headers), file=c)
        for ds, config in zip(iter(data), iter(experiments)):
            all_configs.append(config)
            print("\"{}\"".format(name(config).replace("_", "\\\\_")), file=c)
            for d in ds:
                print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
            print("\n", file=c)
            index += 1

    config_reorder = sorted(map(lambda x: (x[1], x[0]), enumerate(all_configs)))

    # Generate gnuplot script
    gnuplot_terminal = args.terminal
    gnuplot_out_filename = get_chart_filename(gnuplot_terminal)
    time_index = all_headers.index("time") + 1
    rss_index = all_headers.index("RSS") + 1
    # fix long titles
    if len(title) > 79:
        idx = title.find(" ", int(len(title) / 2))
        if idx != -1:
            title = "{}\\n{}".format(title[:idx], title[idx:])
    with open(chart_filename, 'w') as c:
        print("""\
set terminal {gnuplot_terminal} font \"TimesNewRoman, 10\" size 3.3, 2.0 monochrome
# set logscale y

# set format y "10^{{%T}}"
set format y '%.1s%cB'
set grid xtics ytics
set mytics 4
set mxtics 4

set ylabel "RSS"
set xlabel "Time [s]"

set xrange [0:{duration}+20]

# set key at screen .5, screen 0.01 center bottom maxrows 1 maxcols 10
set key top left
# unset key

set output '{gnuplot_out_filename}'
stats '{dataset_filename}' using {rss_index} nooutput
if (STATS_blocks == 0) exit
set for [i=1:STATS_blocks] linetype i dashtype i
# set yrange [10**floor(log10(STATS_min)): 10**ceil(log10(STATS_max))]
# set title "{title}"
set bmargin at screen 0.24
plot for [i=0:*] '{dataset_filename}' using {time_index}:{rss_index} index i title columnheader(1) with lines
        """.format(dataset_filename=dataset_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   rss_index=rss_index,
                   time_index=time_index,
                   title=title.replace("_", "\\\\_"),
                   duration=duration,
                   ), file=c)

else: # json or html

    vega_lite = {
      "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
      "title": ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering, key=lambda t: t[0])),
      "width": 600,
      "mark": {
          "type": "line",
          "clip": True,
      },
      "encoding": {
        "x": { "field": "time", "type": "quantitative", "axis": { "labelAngle": -90 }, "scale": {"domain": [0,450]} },
        "y": { "field": "RSS", "type": "quantitative", "axis": { "format": "s", "labelAngle": 0 }, "scale": { "type": "log" }},
        "row": { "field": "experiment", "type": "nominal" },
      },
      "data": {
        "values": data
      }
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
