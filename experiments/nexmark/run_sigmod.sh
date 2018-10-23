#!/bin/bash
source run_bench.sh

group=1
run_group "sigmod_micro_no_migr"
run_group "sigmod_micro_migr"
run_group "sigmod_nx"
