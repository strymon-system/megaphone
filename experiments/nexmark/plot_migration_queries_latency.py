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
parser.add_argument('--table', action='store_true')
parser.add_argument('--terminal', default="pdf")
parser.add_argument('--filter', nargs='+', default=[])
parser.add_argument('--rename', default="{}")
parser.add_argument('--name')
args = parser.parse_args()

results_dir = args.results_dir
files = plot.get_files(results_dir)
filtering = eval(args.filtering)
rename = eval(args.rename)

if len(args.filter) > 0:
    graph_filtering = []
    data = []
    experiments = []
    for filter in args.filter:
        filter = eval(filter)
        # print(filtering+filter, file=sys.stderr)
        gf, da, ex = plot.latency_plots(results_dir, files, filtering + filter)
        for f in ex:
            # print(f, file=sys.stderr)
            f.extend(filter)
        graph_filtering.extend(gf)
        data.extend(da)
        experiments.extend(ex)
    # print("experiments", experiments, file=sys.stderr)
else:
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
elif args.table:
    extension = "tex"
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
        if "native" in config.get('backend', ""):
            return "Native"
        return config.get("bin_shift", "UNKNOWN FIXME")

    # Generate dataset
    all_headers = set()

    for ds in data:
        for d in ds:
            for k in d.keys():
                all_headers.add(k)
    # all_headers.remove("bin_shift")
    all_headers = sorted(all_headers)

    dataset_filename = get_chart_filename("dataset")

    with open(dataset_filename, 'w') as c:
        # print(" ".join(all_headers), file=c)
        for ds, config in zip(iter(data), iter(experiments)):
            print("\"{}\"".format(name(config)), file=c)
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
set terminal {gnuplot_terminal} font \"TimesNewRoman, 20\"
set logscale x
set logscale y

set format x "10^{{%T}}"
set format y "10^{{%T}}"
set grid xtics ytics

set yrange [.00001:1]
set xrange [0.5:2000]

set xlabel "Latency [ms]"
set ylabel "CCDF"
#set title "{title}"

set key outside right

set output '{gnuplot_out_filename}'
stats '{dataset_filename}' using 0 nooutput
if (STATS_blocks == 0) exit
set for [i=1:STATS_blocks] linetype i dashtype i
plot for [i=0:(STATS_blocks - 1)] '{dataset_filename}' using {latency_index}:{ccdf_index} index i title columnheader(1) with lines linewidth 2
        """.format(dataset_filename=dataset_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   latency_index=latency_index,
                   ccdf_index=ccdf_index,
                   title=title.replace("_", "\\\\_"),
                   ), file=c)
elif args.table:
    tex_filename = get_chart_filename(extension)

    with open(tex_filename, 'w') as c:
        def name(config):
            config = dict(config)
            if "native" in config.get('backend', ""):
                return "Native"
            return config.get("bin_shift", "UNKNOWN FIXME")

        def format_lat(d, c):
            print("& {:.2f} ".format(d['latency']), file=c)


        # print(" ".join(all_headers), file=c)

        print("Experiment & 90\\% & 99\\% & 99.99\\% & max \\tabularnewline", file=c)
        print("\\midrule", file=c)

        for ds, config in zip(iter(data), iter(experiments)):
            # print(config, file=sys.stderr)
            print("{} ".format(name(config)), file=c)
            for d in ds:
                if d['ccdf'] <= 0.1:
                    format_lat(d, c)
                    break
            for d in ds:
                if d['ccdf'] <= 0.01:
                    format_lat(d, c)
                    break
            for d in ds:
                if d['ccdf'] <= 0.001:
                    format_lat(d, c)
                    break
            format_lat(ds[-1], c)

        # print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
            print("\\tabularnewline\n", file=c)

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
