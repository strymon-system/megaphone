#!/usr/bin/env python3

import sys
import json

def parse_name(name):
    kvs = name.rstrip().split('+')
    def parsekv(kv):
        k, v = kv.split('=')
        if v.isdigit():
            v = int(v)
        elif v == "True":
            v = True
        elif v == "False":
            v = False
        return (k, v)
    return (name, list(parsekv(kv) for kv in kvs))

def all_params(ps):
    return dict((x[0][0], list(set(list(zip(*x))[1]))) for x in zip(*list(ps)))

if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == '--list-params':
        print(json.dumps(all_params(parse_name(line)[1] for line in sys.stdin)))
    else:
        for line in sys.stdin:
            name, params = parse_name(line)
            print(json.dumps({
                "filename": name,
                "params": dict(params),
            }))
