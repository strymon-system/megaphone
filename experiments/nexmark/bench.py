#!/usr/bin/env python3

import sys
from collections import namedtuple, defaultdict
from HopcroftKarp import HopcroftKarp

# - Generic interface to run benchmark with a configuration
# - Runner to run experiments remotely
# - Specific benchmarks to execute

class InitialPattern(object):

    def __init__(self, bin_shift, workers):
        self._bin_shift = bin_shift
        self._workers = workers

    def generate_uniform(self):
        return [i % self._workers for i in range(2**self._bin_shift)]

    def generate_uniform_skew(self):
        generator = InitialPattern(self._bin_shift, self._workers//2)
        map = generator.generate_half()
        map.extend(generator.generate_uniform())
        return map

    def generate_half(self):
        return [(i // 2 * 2 + i % 2 * self._workers // 2) % self._workers for i in range(2**self._bin_shift)]

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
        current_map = self._current_map.copy()
        # Migration as graph representation
        graph = defaultdict(set)
        # Remember edge labels. (src, dst) -> [bin]
        labels = defaultdict(list)
        for (i, (src, dst)) in enumerate(zip(self._current_map, self._target_map)):
            if src != dst:
                graph[src].add(str(dst))
                labels[src, str(dst)].append(i)
        while True:
            # Compute maximum matching
            # Need to copy graph as graph will me modified
            matching = HopcroftKarp(graph.copy()).maximum_matching()
            # Diffs
            diffs = {}
            # Filter reverse matchings and update `current_map` at matching positions
            for src, dst in list(matching.items()):
                # Filter reverse matchings
                if isinstance(src, str):
                    del matching[src]
                else:
                    # The matching belongs to an edge in the migration graph
                    if len(labels[src, dst]) > 0:
                        # Determine the entry number
                        entry = labels[src, dst].pop()
                        # update map
                        dst = int(dst)
                        current_map[entry] = dst
                        diffs[entry] = dst
                    elif len(labels[src, dst]) == 0:
                        # remove edge from graph
                        graph[src].remove(dst)
            # We are done if there are no more matchings
            if len(matching) == 0:
                break
            # Emit diffs
            yield diffs



Experiment = namedtuple("Experiment", ["duration", "query", "migration", "bin_shift"])


class PatternGenerator(object):

    def __init__(self, migration_pattern, initial_pattern, target_pattern):
        self._migration_pattern = migration_pattern
        self._initial_pattern = initial_pattern
        self._target_pattern = target_pattern
        print(initial_pattern)
        print(target_pattern)

    def write_pattern(self, file, pattern):
        for w in pattern:
            file.write(str(w))
            file.write(" ")
        file.write('\n')

    def write_diff(self, file, pattern):
        for b, w in pattern.items():
            file.write(str(b))
            file.write(" ")
            file.write(str(w))
            file.write(" ")
        file.write('\n')

    def write(self, file):
        generator = self._migration_pattern(self._initial_pattern, self._target_pattern)
        i = 1
        self.write_pattern(file, self._initial_pattern)
        for pattern in generator.generate():
            i += 1
            self.write_diff(file, pattern)
        print(i)

for migration in BatchedFluidMigrationPattern([0,0,2,2], [0,1,2,3]).generate():
    print(migration)
for migration in BatchedFluidMigrationPattern([0,1,1,2,3], [0,1,2,2,3]).generate():
    print(migration)
for migration in BatchedFluidMigrationPattern([0,1,1,2,2,3], [1,1,2,1,2,3]).generate():
    print(migration)
print("---")
for migration in FluidMigrationPattern([0,1,1,2,2,3], [1,1,2,1,2,3]).generate():
    print(migration)

print(InitialPattern(5, 16).generate_uniform())
print(InitialPattern(5, 4).generate_half())
# pattern_generator = InitialPattern(20, 32)
pattern_generator = InitialPattern(5, 4)
print("---")
initial_pattern = pattern_generator.generate_uniform()
generator = PatternGenerator(BatchedFluidMigrationPattern, initial_pattern, pattern_generator.generate_uniform_skew())
generator.write(sys.stdout)
