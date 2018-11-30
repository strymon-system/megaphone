#!/bin/bash -e
trap "exit" INT TERM ERR
trap "kill 0" EXIT

REVISION="${REVISION:-`git rev-parse HEAD | cut -c1-16`}"

function plot {
#    "$@" || exit $?

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

binary="('binary', 'word_count'), "
processes="('processes', 4), "
duration="('duration', 30), "
migration="('migration', 'sudden'), "
initial_config="('initial_config', 'uniform'), "
final_config="('final_config', 'uniform'), "
fake_stateful="('fake_stateful', False), "
rate="('rate', 4000000), "
backend="" #"('backend', 'vec'), "
breakdown_rename="{'sudden': 'all-at-once', 'bin_shift': 'bins', 4: 2**4, 5: 2**5, 6: 2**6, 7: 2**7, 8: 2**8, 9: 2**9, 10: 2**10, 11: 2**11, 12: 2**12, 13: 2**13, 14: 2**14}"
timeline_rename="{'sudden' : ('all-at-once', 1), 'batched' : ('Megaphone (batched)', 3),}"

for domain in "('domain', 256000000), " "('domain', 8192000000), "
do
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ $binary $processes $duration $migration $initial_config $final_config $fake_stateful $domain $rate $backend ]" --name vec --filter \
    "[ ('backend',  'vec'), ]" \
    "[ ('backend',  'vecnative'), ]"

         ./plot_migration_queries_latency.py "results/$REVISION/" "[ $binary $processes $duration $migration $initial_config $final_config $fake_stateful $domain $rate $backend ]" --table --name vec --filter \
    "[ ('backend',  'vec'), ]" \
    "[ ('backend',  'vecnative'), ]"

done

rate="('rate', 4000000), "
domain="('domain', 256000000), "
plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ $binary $processes $duration $migration $initial_config $final_config $fake_stateful $domain $rate $backend ]" --name hashmap --filter \
"[ ('backend',  'hashmap'), ]" \
"[ ('backend',  'hashmapnative'), ]"

     ./plot_migration_queries_latency.py "results/$REVISION/" "[ $binary $processes $duration $migration $initial_config $final_config $fake_stateful $domain $rate $backend ]" --table --name hashmap --filter \
"[ ('backend',  'hashmap'), ]" \
"[ ('backend',  'hashmapnative'), ]"

rate="('rate', 4000000), "
final_config="('final_config', 'uniform_skew'), "
bin_shift="('bin_shift', 12), "
duration="('duration', 1200), "
backend="('backend', 'vec'), "


plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $rate $backend $bin_shift ]" migration domain --rename "$breakdown_rename"
plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $backend ]" migration bin_shift --rename "$breakdown_rename" --filter \
    "[ ('bin_shift',  6), ('domain',   256000000) ]" \
    "[ ('bin_shift',  7), ('domain',   512000000) ]" \
    "[ ('bin_shift',  8), ('domain',  1024000000) ]" \
    "[ ('bin_shift',  9), ('domain',  2048000000) ]" \
    "[ ('bin_shift', 10), ('domain',  4096000000) ]" \
    "[ ('bin_shift', 11), ('domain',  8192000000) ]" \
    "[ ('bin_shift', 12), ('domain', 16384000000) ]" \
    "[ ('bin_shift', 13), ('domain', 32768000000) ]"

duration="('duration', 1200), "
plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $rate $backend $bin_shift ]" migration domain --rename "$breakdown_rename"

rate="('rate', 2000000), "
backend="('backend', 'hashmap'), "
plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $rate $backend $bin_shift ]" migration domain --rename "$breakdown_rename"

rate="('rate', 4000000), "
final_config="('final_config', 'uniform_skew'), "
bin_shift="('bin_shift', 12), "
duration="('duration', 1200), "

plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $rate $backend $bin_shift ]" migration domain --rename "$breakdown_rename"

domain="('domain', 4096000000), "
backend="('backend', 'vec'), "
plot ./plot_latency_breakdown.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $domain $backend ]" migration bin_shift --rename "$breakdown_rename"

backend="('backend', 'vec'), "
duration="('duration', 1200), "
for domain in "('domain', 256000000), " "('domain', 512000000), " "('domain', 1024000000), " "('domain', 2048000000), " "('domain', 4096000000), " "('domain', 8192000000), "  "('domain', 16384000000), "  "('domain', 32768000000), " "('domain', 65536000000), "
do
    true
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --rename "$breakdown_rename"
done

bin_shift="('bin_shift', 12), "
domain="('domain', 4096000000), "
plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --name intro --rename "{'sudden' : ('All-at-once (prior work)', 1), 'fluid': ('Megaphone (fluid)', 2), 'batched': ('Megaphone (optimized)', 3)}"
plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --rename "$breakdown_rename"

rate="('rate', 2000000), "
backend="('backend', 'hashmap'), "
duration="('duration', 1200), "
for domain in "('domain', 32000000), " "('domain', 64000000), " "('domain', 128000000), " "('domain', 256000000), " "('domain', 512000000), "
do
    true
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $domain $backend ]" --rename "$breakdown_rename"
done

## NEXMark

binary="('binary', 'timely'), "
duration="('duration', 1200), "
rate="('rate', 4000000), "
time_dilation="('time_dilation', 1), "
bin_shift="('bin_shift', 12), "

for query in "q3" "q4" "q5" "q6" "q7" "q8" "q3-flex" "q4-flex" "q5-flex" "q6-flex" "q7-flex" "q8-flex"
do
    queries="('queries', '$query'), "
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$breakdown_rename"
    true
done

duration="('duration', 30), "
for query in "q1" "q2" "q1-flex" "q2-flex"
do
    queries="('queries', '$query'), "
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$breakdown_rename"
    true
done

duration="('duration', 1200), "
rate="('rate', 50632), "
time_dilation="('time_dilation', 79), "

for query in "q3" "q4" "q5" "q6" "q7" "q8" "q3-flex" "q4-flex" "q5-flex" "q6-flex" "q7-flex" "q8-flex"
do
    queries="('queries', '$query'), "
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$breakdown_rename"
    true
done

duration="('duration', 30), "
for query in "q1" "q2" "q1-flex" "q2-flex"
do
    queries="('queries', '$query'), "
    plot ./plot_latency_timeline.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$timeline_rename"
    plot ./plot_memory_timeline.py  "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $bin_shift $rate $time_dilation $queries ]" --rename "$breakdown_rename"
    true
done

exit


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
        true
        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('bin_shift', 8), ('rate', ${rate}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]" "migration" "domain"
    done
done

# experiments with defined domain
for i in $(seq 0 2 6)
do
    domain=$((2**$i*1000000))
    plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', 8), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]"

    # defined domain and bin_shift
    for bin_shift in $(seq 6 2 12)
    do
        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('bin_shift', ${bin_shift}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ]" rate migration
    done

    # experiments with defined domain and rate
    for i in $(seq 0 1 9)
    do
        rate=$((2**$i*100000))
        # bin_shift experiment
        plot ./plot_migration_queries_latency.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('final_config', 'uniform'), ('migration', 'sudden'), ('domain', ${domain}), ('rate', ${rate}), ]"
        plot ./plot_latency_breakdown.py "results/$REVISION/" "[ ('binary', '${binary}'), ${duration} ('machine_local', False), ${processes} ('workers', ${workers}), ('domain', ${domain}), ('final_config', 'uniform_skew'), ('fake_stateful', False), ('rate', ${rate}), ]" migration bin_shift

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
