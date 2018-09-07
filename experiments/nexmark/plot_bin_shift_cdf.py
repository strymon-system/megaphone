#!/usr/bin/env python3

# ./plot_latency_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import argparse
import json
import os
import sys

import plot

parser = argparse.ArgumentParser(description="Plot")
parser.add_argument('results_dir')
parser.add_argument('filtering')
parser.add_argument('--json', action='store_true')
args = parser.parse_args()

results_dir = args.results_dir
files = plot.get_files(results_dir)
filtering = eval(args.filtering)

all_data = []
all_filtering = set()
for bin_shift in range(4, 21):
    if bin_shift % 2 == 1:
        continue
    filtering_bin_shift = list(filtering)
    filtering_bin_shift.append(('bin_shift', bin_shift))
    graph_filtering, data = plot.latency_plots(results_dir, files, filtering_bin_shift)
    print(graph_filtering, file=sys.stderr)
    for d in data:
        d['bin_shift'] = bin_shift
    all_data.extend(data)
    for filter in graph_filtering:
        all_filtering.add(filter)
graph_filtering = list(x for x in all_filtering if x[0] is not 'bin_shift')

print(graph_filtering, file=sys.stderr)
data = all_data

# Try to extract duration from filter pattern
duration = next((x[1] for x in filtering if x[0] == 'duration'), 450)

vega_lite = {
    "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
    "title": ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering, key=lambda t: t[0])),
    "hconcat": [
        {
            "mark": "line",
            "encoding": {
                "x": { "field": "latency", "type": "quantitative", "axis": { "format": "e", "labelAngle": -90 }, "scale": { "type": "log" }},
                "y": { "field": "ccdf", "type": "quantitative", "scale": { "type": "log" }},
                "row": { "field": "experiment", "type": "nominal" },
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
                "y": { "field": "bin_shift", "type": "nominal", "title": None }
            }
        }
    ],
    "data": {
        "values": data
    }
};

if args.json:
    print(json.dumps(vega_lite))
    exit(0)

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
        <div id="configuration">
            <h1>Configuration</h1>
        </div>

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

graph_filename = "{}+{}.html".format(plot.plot_name(__file__), plot.kv_to_string(dict(graph_filtering)))

commit = results_dir.rstrip('/').split('/')[-1]
print("commit:", commit, file=sys.stderr)

plot.ensure_dir("charts/{}".format(commit))
chart_filename = "charts/{}/{}".format(commit, graph_filename)
with open(chart_filename, 'w') as c:
    print(html, file=c)

print(chart_filename)
print(os.getcwd() + "/" + chart_filename, file=sys.stderr)
