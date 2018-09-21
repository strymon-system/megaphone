#!/usr/bin/env python3

# ./plot_migration_queries_latency.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import sys, os, shutil, json
import argparse
import plot

parser = argparse.ArgumentParser(description="Plot")
parser.add_argument('results_dir')
parser.add_argument('filtering')
parser.add_argument('--json', action='store_true')
parser.add_argument('--gnuplot', action='store_true')
parser.add_argument('--terminal', default="pdf")
args = parser.parse_args()

results_dir = args.results_dir
files = plot.get_files(results_dir)
filtering = eval(args.filtering)

graph_filtering, data, experiments = plot.latency_plots(results_dir, files, filtering)

commit = results_dir.rstrip('/').split('/')[-1]
# print("commit:", commit, file=sys.stderr)

if len(data) == 0:
    print("No data found: {}".format(filtering), file=sys.stderr)
    exit(0)

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
title = plot.kv_to_name(dict(graph_filtering))

if args.gnuplot:

    # Generate dataset
    all_headers = set()

    for ds in data:
        for d in ds:
            for k in d.keys():
                all_headers.add(k)
    # all_headers.remove("bin_shift")
    all_headers = sorted(all_headers)
    graph_filtering_bin_shift = dict(graph_filtering)

    dataset_filename = get_chart_filename("dataset")

    with open(dataset_filename, 'w') as c:
        # print(" ".join(all_headers), file=c)
        for ds, config in zip(iter(data), iter(experiments)):
            print("\"{} {}\"".format(config.get('queries', ""), config.get('rate', "")), file=c)
            for d in ds:
                print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
            print("\n", file=c)

    # Generate gnuplot script
    gnuplot_terminal = args.terminal
    gnuplot_out_filename = get_chart_filename(gnuplot_terminal)
    ccdf_index = all_headers.index("ccdf") + 1
    latency_index = all_headers.index("latency") + 1
    # fix long titles
    if len(title) > 79:
        idx = title.find(" ", int(len(title) / 2))
        if idx != -1:
            title = "{}\\n{}".format(title[:idx], title[idx:])
    with open(chart_filename, 'w') as c:
        print("""\
set terminal {gnuplot_terminal} font \"LinuxLibertine, 10\"
set logscale x
set logscale y

set format x "10^{{%T}}"
set format y "10^{{%T}}"
set grid xtics ytics

set xlabel "Latency [ns]"
set ylabel "CCDF"
set title "{title}"

set key left bottom box

set output '{gnuplot_out_filename}'
stats '{dataset_filename}' using 0 nooutput
plot for [i=0:(STATS_blocks - 1)] '{dataset_filename}' using {latency_index}:{ccdf_index} index i title columnheader(1) with linespoints
        """.format(dataset_filename=dataset_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   latency_index=latency_index,
                   ccdf_index=ccdf_index,
                   title=title.replace("_", "\\\\_"),
                   ), file=c)

else: # json or html
    vega_lite = {
      "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
      "hconcat": [
        {
          "mark": "line",
          "encoding": {
            "x": { "field": "latency", "type": "quantitative", "axis": { "format": "e", "labelAngle": -90 }, "scale": { "type": "log" }},
            "y": { "field": "ccdf", "type": "quantitative", "scale": { "type": "log" } },
            "row": { "field": "experiment", "type": "nominal" },
            "column": { "field": "queries", "type": "nominal" },
            "stroke": { "field": "rate", "type": "nominal", "legend": None },
            "shape": { "field": "rate", "type": "nominal", "legend": None }
          }
        },
        {
          "mark": "point",
          "encoding": {
            "shape": { "field": "rate", "aggregate": "min", "type": "nominal", "legend": None },
            "fill": { "field": "rate", "aggregate": "min", "type": "nominal", "legend": None },
            "y": { "field": "rate", "type": "nominal", "title": None }
          }
        }
      ],
      "data": {
        "values": [d for ds in data for d in ds]
      }
    };

    if args.json:
        with open(chart_filename, 'w') as c:
            print(json.dumps(vega_lite), file=c)
    else:
        print(sorted(graph_filtering.items(), key=lambda t: t[0]))
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
