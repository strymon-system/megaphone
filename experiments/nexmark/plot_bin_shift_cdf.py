#!/usr/bin/env python3

# ./plot_latency_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import argparse
import json
import os
import sys

from collections import defaultdict

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

all_data = []
all_filtering = defaultdict(set)
for bin_shift in range(4, 21):
    if bin_shift % 2 == 1:
        continue
    filtering_bin_shift = list(filtering)
    filtering_bin_shift.append(('bin_shift', bin_shift))
    graph_filtering, data, experiments = plot.latency_plots(results_dir, files, filtering_bin_shift)
    for ds in data:
        for d in ds:
            d['bin_shift'] = bin_shift
            if d['queries'].endswith('-flex'):
                d['queries'] = d['queries'][:-5]
            else:
                # d['experiment'] = ""
                d['bin_shift'] = "Native"
    all_data.extend(data)
    for k, vs in graph_filtering.items():
        for v in vs:
            all_filtering[k].add(v)
graph_filtering = {}
for k, v in all_filtering.items():
    if len(v) == 1:
        graph_filtering[k] = next(iter(v))
    else:
        graph_filtering[k] = ",".join(map(str, sorted(v)))
graph_filtering = list(x for x in graph_filtering.items() if x[0] is not 'bin_shift')

# print(graph_filtering, file=sys.stderr)
data = all_data

# Try to extract duration from filter pattern
duration = next((x[1] for x in filtering if x[0] == 'duration'), 450)

vega_lite = {
    "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
    "hconcat": [
        {
            "mark": "line",
            "encoding": {
                "x": { "field": "latency", "type": "quantitative", "axis": { "format": "e", "labelAngle": -90 }, "scale": { "type": "log" }},
                "y": { "field": "ccdf", "type": "quantitative", "scale": { "type": "log" }},
                # "row": { "field": "experiment", "type": "nominal" },
                "column": { "field": "queries", "type": "nominal" },
                "stroke": { "field": "bin_shift", "type": "nominal", "legend": None },
                "shape": { "field": "bin_shift", "type": "nominal", "legend": None }
            }
        },
        {
            "mark": "point",
            "encoding": {
                "shape": { "field": "bin_shift", "aggregate": "min", "type": "nominal", "legend": None },
                "fill": { "field": "bin_shift", "aggregate": "min", "type": "nominal", "legend": None },
                "y": { "field": "bin_shift", "type": "nominal", "title": None,
                       # "axis": { "labels": False, "labelFont": "Times" }
                       }
            }
        }
    ],
    "config": {
        # "text": {
        #     "font": "Helvetica Neue",
        # },
        "axis": {
            "labelFont": "monospace",
            "labelFontSize": 20,
            "titleFont": "cursive",
            "titleFontSize": 30,
            "titlePadding": 20
        },
        "title": {
            "font": "monospace",
            "fontSize": 40,
        },
    },
    "data": {
        "values": [d for ds in data for d in ds]
    }
};

title = plot.kv_to_name(dict(graph_filtering))

if args.json:
    extension = "json"
elif args.gnuplot:
    extension = "gp"
else:
    extension = "html"
    vega_lite["title"] = title


html = """
<!DOCTYPE html>
<html>
<head>
  <script src="svg2pdf.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/jspdf@1.4.1"></script>
  <script src="https://cdn.jsdelivr.net/npm/d3@5.5.0"></script>
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
    var form = d3.select("#configuration").append("form");
    var fieldset3 = form.append("fieldset").classed("conf_axis", true);
    var exportPNG = fieldset3.append("a").text("SVG");
    exportPNG.on("click", function(d) {

        var svg = d3.select("marks") 
        // create a new jsPDF instance
        const pdf = new jsPDF('l', 'pt');
        
        // render the svg element
        svg2pdf(svg, pdf, {
            xOffset: 0,
            yOffset: 0,
            scale: 1
        });
        exportPNG.attr("href", pdf.output('datauristring'));
        exportPNG.attr("download", "plot.pdf");
    });

         </script>
       </body>
       </html>
       """


commit = results_dir.rstrip('/').split('/')[-1]
# print("commit:", commit, file=sys.stderr)

plot.ensure_dir("charts/{}".format(commit))

graph_filename = "{}+{}.{}".format(plot.plot_name(__file__), plot.kv_to_string(dict(graph_filtering)), extension)
chart_filename = "charts/{}/{}".format(commit, graph_filename)

def sort_mixed(x):
    if isinstance(x, str):
        return -1
    else:
        return x

with open(chart_filename, 'w') as c:
    if args.json:
        print(json.dumps(vega_lite), file=c)
    if args.gnuplot:
        all_headers = set()
        all_bin_shifts = set()

        for ds in data:
            for d in ds:
                for k in d.keys():
                    all_headers.add(k)
            all_bin_shifts.add(d["bin_shift"])
        # all_headers.remove("bin_shift")
        all_headers = sorted(all_headers)
        graph_filtering_bin_shift = dict(graph_filtering)
        with open(chart_filename, 'w') as c:
            for bin_shift in sorted(all_bin_shifts, key=sort_mixed):
                print('"bin shift {}"'.format(bin_shift), file=c)
                print(" ".join(all_headers), file=c)
                for ds in data:
                    for d in ds:
                        if d["bin_shift"] == bin_shift:
                            print(" ".join(map(plot.quote_str, [d[k] for k in all_headers])), file=c)
                print("\n", file=c)
    else:
        print(html, file=c)


if args.gnuplot:
    gnuplot_terminal = args.terminal
    gnuplot_filename = "{}+{}.{}".format(plot.plot_name(__file__), plot.kv_to_string(dict(graph_filtering)), "gnuplot")
    gnuplot_filename = "charts/{}/{}".format(commit, gnuplot_filename)
    gnuplot_out_filename = "{}+{}.{}".format(plot.plot_name(__file__), plot.kv_to_string(dict(graph_filtering)), gnuplot_terminal)
    gnuplot_out_filename = "charts/{}/{}".format(commit, gnuplot_out_filename)
    ccdf_index = all_headers.index("ccdf") + 1
    latency_index = all_headers.index("latency") + 1
    bin_shift_index = all_headers.index("bin_shift") + 1

    # fix long titles
    if len(title) > 79:
        idx = title.find(" ", int(len(title) / 2))
        if idx != -1:
            title = "{}\\n{}".format(title[:idx], title[idx:])
    with open(gnuplot_filename, 'w') as c:
        print("""\
set terminal {gnuplot_terminal} font \"LinuxLibertine, 10\"
set logscale x
set logscale y

set for [i=1:9] linetype i dashtype i

set format x "10^{{%T}}"
set format y "10^{{%T}}"
set grid xtics ytics

set xlabel "Latency [ns]"
set ylabel "CCDF"
set title "{title}"

set key left bottom box

set output '{gnuplot_out_filename}'
stats '{chart_filename}' using 0 nooutput
plot for [i=0:(STATS_blocks - 1)] '{chart_filename}' using {latency_index}:{ccdf_index} index i title column({bin_shift_index}) with points
        """.format(chart_filename=chart_filename,
                   gnuplot_terminal=gnuplot_terminal,
                   gnuplot_out_filename=gnuplot_out_filename,
                   latency_index=latency_index,
                   ccdf_index=ccdf_index,
                   bin_shift_index=bin_shift_index,
                   title=title.replace("_", "\\\\_"),
                   ), file=c)
    print(gnuplot_filename)
    print(os.getcwd() + "/" + gnuplot_filename, file=sys.stderr)
else:
    print(chart_filename)
    print(os.getcwd() + "/" + chart_filename, file=sys.stderr)
