#!/bin/bash
source run_bench.sh

group=1
run_group "wc_bin_shift"
run_group "wc_bin_shift_vec"
