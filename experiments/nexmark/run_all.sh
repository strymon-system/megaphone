#!/bin/bash

source run_bench.sh

#group=4
#run_group "non_migrating"
#run_group "exploratory_migrating"
#run_group "exploratory_baseline"
#run_group "exploratory_migrating_single_process"
#run_group "exploratory_bin_shift"

./run_time_dilation.sh

group=1
run_group "exploratory_migrating_mm"
