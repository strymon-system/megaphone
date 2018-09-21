#!/bin/bash -e
trap "exit" INT TERM ERR
trap "kill 0" EXIT

REVISION="${REVISION:-`git rev-parse HEAD | cut -c1-16`}"

duration=300
workers=8
bin_shift=8
time_dilation=1

binary='timely'

echo $REVISION

function plot {
    "$@" || exit $?

    local gnuplot_file=`"$@" --gnuplot --terminal pdf | tail -1 || exit $?`
    if [ -f "${gnuplot_file}" ]
    then
        gnuplot "${gnuplot_file}" || exit $?
    fi

#    local gnuplot_file=`"$@" --gnuplot --terminal latex | tee out.txt | tail -1 || exit $?`
#    if [ -f "${gnuplot_file}" ]
#    then
#        gnuplot "${gnuplot_file}" || exit $?
#    else
#        echo "File not found:" ${gnuplot_file}
#    fi
#    json_file=`"$@" --json | tail -1`
#    echo ${json_file}
#    cat "${json_file}" \
#        | schroot -c node -- ./node_modules/vega-lite/bin/vl2vg \
#        | schroot -c node -- ./node_modules/vega/bin/vg2svg \
#        | inkscape /dev/stdin --export-area-drawing --without-gui "--export-pdf=${json_file%.json}.pdf"
}



for i in $(seq 0 8)
do
    plot ./plot_bin_shift_cdf.py "results/$REVISION/" "[ ('duration', ${duration}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('queries', 'q${i}'), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'sudden'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'batched'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
done

#./plot_bin_shift_cdf.py "results/$REVISION/" "[ ('duration', ${duration}), ('machine_local', True), ('processes', 1), ('workers', ${workers}), ('binary', '${binary}'), ('time_dilation', ${time_dilation}), ]"
