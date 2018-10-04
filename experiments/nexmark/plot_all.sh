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

    local gnuplot_file
    gnuplot_file=$("$@" --gnuplot --terminal pdf) || exit $?
    gnuplot_file="${gnuplot_file##*$'\n'}"
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
#    plot ./plot_bin_shift_cdf.py "results/$REVISION/" "[ ('duration', ${duration}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('queries', 'q${i}'), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'fluid'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'sudden'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('duration', ${duration}), ('bin_shift', ${bin_shift}), ('machine_local', True), ('processes', 2), ('workers', ${workers}), ('time_dilation', ${time_dilation}), ('migration', 'batched'), ('rate', 6400000), ('rate', 3200000), ('rate', 1600000), ('queries', 'q${i}-flex'), ]"
done

binary="word_count"
duration=" ('duration', 60), "
processes=" ('processes', 4), "
workers=4
# all rates and domains for a single migration type
for migration in 'fluid' 'batched' 'sudden'
do
    echo $migration
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('bin_shift', ${bin_shift}), ('machine_local', False), ${processes} ('workers', ${workers}), ('migration', '${migration}'), ('final_config', 'uniform_skew'), ('fake_stateful', False),   ]"
done

# all experiments with defined rate
for i in $(seq 0 2 8)
do
    rate=$((2**$i*100000))
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('bin_shift', 8), ('rate', ${rate}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]"

    # defined rate and bin_shift
    for bin_shift in $(seq 6 2 12)
    do
        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('bin_shift', 8), ('rate', ${rate}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]"
    done
done

# experiments with defined domain
for i in $(seq 0 2 6)
do
    domain=$((2**$i*1000000))
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', 8), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]"

    # defined domain and bin_shift
#    for bin_shift in $(seq 6 2 12)
#    do
#        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', ${bin_shift}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]"
#    done

    # experiments with defined domain and rate
    for i in $(seq 0 1 9)
    do
        rate=$((2**$i*100000))
        echo $domain $rate
        # bin_shift experiment
        plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('final_config', 'uniform'), ('migration', 'sudden'), ('domain', ${domain}), ('rate', ${rate}), ]"
#        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ('rate', ${rate}), ]"

        for bin_shift in $(seq 6 2 12)
        do
            # bin_shift ${bin_shift}
            plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', ${bin_shift}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ('rate', ${rate}), ]"
            plot ./plot_latency_timeline.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', ${bin_shift}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ('rate', ${rate}), ]"
            for migration in 'fluid' 'batched' 'sudden'
            do
                echo $migration
                plot ./plot_memory_timeline.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('bin_shift', ${bin_shift}), ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('migration', '${migration}'), ('final_config', 'uniform_skew'), ('fake_stateful', False), ('rate', ${rate}), ]"
            done
#                plot ./plot_memory_timeline.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('bin_shift', ${bin_shift}), ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('migration', 'sudden'), ('final_config', 'uniform'), ('fake_stateful', False), ('rate', ${rate}), ]"
                plot ./plot_memory_timeline.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('bin_shift', ${bin_shift}), ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('rate', ${rate}), ]"

        done
    done
done
#./plot_bin_shift_cdf.py "results/$REVISION/" "[ ('duration', ${duration}), ('machine_local', True), ('processes', 1), ('workers', ${workers}), ('binary', '${binary}'), ('time_dilation', ${time_dilation}), ]"
