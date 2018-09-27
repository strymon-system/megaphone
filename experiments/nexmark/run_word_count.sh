#!/bin/bash
source run_bench.sh

group=1
run_group "wc_migrating_mm4"
run_group "wc_non_migrating_mm4"
run_group "wc_bin_shift"
