#!/bin/bash
source run_bench.sh

group=4
run_group "wc_exploratory_migrating"

groups=2
run_group "wc_bin_shift"
