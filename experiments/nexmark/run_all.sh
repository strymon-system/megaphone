#!/bin/bash

CLUSTERUSER="${CLUSTERUSER:-andreal}"
clusterpath="/home/${CLUSTERUSER}/Src/dynamic-scaling-mechanism/nexmark"
serverprefix="${CLUSTERUSER}@fdr"
group=4

function run { # command index groups
#    xterm +hold -e
    python3 -c "import bench; bench.$1($2, $3)" --clusterpath "${clusterpath}" --serverprefix "${serverprefix}" &
}

function run_group { # name
    for i in $(seq 0 $(($group - 1)))
    do
        run "$1" "$i" "$group"
    done
    wait
}

run_group "non_migrating"
run_group "exploratory_migrating"
run_group "exploratory_baseline"
run_group "exploratory_migrating_single_process"
run_group "exploratory_bin_shift"
