#!/usr/bin/env python3

import sys, shutil, json
import argparse
from os import listdir
import plot

assert(len(sys.argv) >= 3)
results_dir = sys.argv[1]
files = plot.get_files(results_dir)
filtering = eval(sys.argv[2])

# filtering = [ ('bin_shift', 8), ('duration', 300), ('machine_local', True), ('processes', 4), ('workers', 8), ]

graph_filtering, data = plot.latency_plots(results_dir, files, filtering)

vega_lite = {
  "$schema": "https://vega.github.io/schema/vega-lite/v2.json",
  "title": ", ".join("{}: {}".format(k, v) for k, v in sorted(graph_filtering, key=lambda t: t[0])),
  "hconcat": [
    {
      "mark": "line",
      "encoding": {
        "x": { "field": "latency", "type": "quantitative", "axis": { "format": "e", "labelAngle": -90 }, "scale": { "type": "log" }},
        "y": { "field": "ccdf", "type": "quantitative" },
        "column": { "field": "migration", "type": "nominal" },
        "row": { "field": "queries", "type": "nominal" },
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
    "values": data
  }
};

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

graph_filename = "latency+{}.html".format(plot.kv_to_string(dict(graph_filtering)))

commit = results_dir.rstrip('/').split('/')[-1]
print("commit:", commit, file=sys.stderr)

plot.ensure_dir("charts/{}".format(commit))
with open("charts/{}/{}".format(commit, graph_filename), 'w') as c:
    print(html, file=c)
