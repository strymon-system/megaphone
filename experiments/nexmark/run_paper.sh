#!/bin/bash
source run_bench.sh

group=1
run_group "paper_micro_no_migr"
run_group "paper_micro_migr"
run_group "paper_nx"
