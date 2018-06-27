#!/usr/bin/env python3

from collections import namedtuple, defaultdict
from HopcroftKarp import HopcroftKarp

# - Generic interface to run benchmark with a configuration
# - Runner to run experiments remotely
# - Specific benchmarks to execute


class MigrationPattern(object):

    def __init__(self, current_map, target_map):
        self._current_map = current_map
        self._target_map = target_map

    def generate(self):
        pass

class SuddenMigrationPattern(MigrationPattern):

    def generate(self):
        yield self._target_map

class FluidMigrationPattern(MigrationPattern):

    def generate(self):
        current_map = self._current_map
        for (i, (src, dst)) in enumerate(zip(current_map, self._target_map)):
            if src != dst:
                current_map[i] = dst
                yield current_map

class BatchedFluidMigrationPattern(MigrationPattern):

    def generate(self):
        current_map = self._current_map
        graph = defaultdict(set)
        for (src, dst) in zip(self._current_map, self._target_map):
            if src != dst:
                graph[src].add(str(dst))
        while True:
            matching = HopcroftKarp(graph).maximum_matching()
            for src, dst in list(matching.items()):
                if isinstance(src, str):
                    del matching[src]
                if src in graph:
                    graph[src].remove(dst)
            if len(matching) == 0:
                break
            for (matching_src, matching_dst) in matching.items():
                for (i, (src, dst)) in enumerate(zip(current_map, self._target_map)):
                    if src == matching_src and dst == int(matching_dst):
                        current_map[i] = int(matching_dst)
            yield current_map



Experiment = namedtuple("Experiment", ["duration", "query", "migration", "bin_shift"])


for migration in BatchedFluidMigrationPattern([0,0,2,2], [0,1,2,3]).generate():
    print(migration)
for migration in BatchedFluidMigrationPattern([0,1,1,2,3], [0,1,2,2,3]).generate():
    print(migration)
for migration in BatchedFluidMigrationPattern([0,1,1,2,2,3], [1,1,2,1,2,3]).generate():
    print(migration)
print("---")
for migration in FluidMigrationPattern([0,1,1,2,2,3], [1,1,2,1,2,3]).generate():
    print(migration)

