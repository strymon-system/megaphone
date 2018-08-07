#!/bin/bash
trap "exit" INT TERM ERR
trap "kill 0" EXIT

CLUSTERUSER="${CLUSTERUSER:-andreal}"
clusterpath="/home/${CLUSTERUSER}/Src/dynamic-scaling-mechanism/nexmark"
serverprefix="${CLUSTERUSER}@fdr"
group=4

function run { # command index groups additional
#    xterm +hold -e
    python3 -c "import bench; bench.$1($2, $3)" --clusterpath "${clusterpath}" --serverprefix "${serverprefix}" $4 &
}

function run_group { # name
    run "$1" "0" "1" --build-only
    wait
    for i in $(seq 0 $(($group - 1)))
    do
        run "$1" "$i" "$group" --no-build
    done
    wait
}

run_group "non_migrating"
run_group "exploratory_migrating"
run_group "exploratory_baseline"
run_group "exploratory_migrating_single_process"
run_group "exploratory_bin_shift"
run_group "migrating_time_dilation"

group=2
run_group "exploratory_migrating_mp"
