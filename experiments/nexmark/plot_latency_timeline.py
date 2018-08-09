#!/usr/bin/env python3

# ./plot_latency_timeline.py results/98f4e2fa2e8bc839/ "[ ('bin_shift', 8), ('duration', 120), ('machine_local', True), ('processes', 2), ('workers', 8), ]"

import json
import os
import sys

import plot

assert(len(sys.argv) >= 3)
results_dir = sys.argv[1]
files = plot.get_files(results_dir)
filtering = eval(sys.argv[2])

graph_filtering, data = plot.latency_timeline_plots(results_dir, files, filtering)

# Try to extract duration from filter pattern
duration = next((x[1] for x in filtering if x[0] == 'duration'), 450)

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

graph_filename = "{}+{}.html".format(plot.plot_name(__file__), plot.kv_to_string(dict(graph_filtering)))

commit = results_dir.rstrip('/').split('/')[-1]
print("commit:", commit, file=sys.stderr)

plot.ensure_dir("charts/{}".format(commit))
chart_filename = "charts/{}/{}".format(commit, graph_filename)
with open(chart_filename, 'w') as c:
    print(html, file=c)

print(chart_filename)
print(os.getcwd() + "/" + chart_filename, file=sys.stderr)
