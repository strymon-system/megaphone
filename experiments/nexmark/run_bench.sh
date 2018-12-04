#!/bin/bash
trap "exit" INT TERM ERR
trap "kill 0" EXIT

CLUSTERUSER="${CLUSTERUSER:-$USER}"
clusterpath="${CLUSTERPATH:-`git rev-parse --show-toplevel`}/nexmark"
serverprefix="${CLUSTERUSER}@${SERVER:-fdr}"

function run { # command index groups additional
#    xterm +hold -e
    python3 -c "import bench; bench.$1($2, $3)" --clusterpath "${clusterpath}" --serverprefix "${serverprefix}" $4 || exit $?
}

function run_group { # name
    run "$1" "0" "1" --build-only
    for i in $(seq 0 $(($group - 1)))
    do
        run "$1" "$i" "$group" --no-build &
    done
    wait
}
export -f run_group
