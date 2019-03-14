#!/bin/bash -ex
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
duration="('duration', 1200), "
initial_config="('initial_config', 'uniform'), "
final_config="('final_config', 'uniform_skew'), "
fake_stateful="('fake_stateful', False), "
backend="('backend', 'vec'), "
breakdown_rename="{'sudden': 'all-at-once', 'bin_shift': 'bins', 4: 2**4, 5: 2**5, 6: 2**6, 7: 2**7, 8: 2**8, 9: 2**9, 10: 2**10, 11: 2**11, 12: 2**12, 13: 2**13, 14: 2**14}"
timeline_rename="{'sudden' : ('all-at-once', 1), 'batched' : ('Megaphone (batched)', 3),}"

plot ./plot_rate_latency.py "results/$REVISION/" "[ $binary $processes $duration $initial_config $final_config $fake_stateful $backend $bin_shift ]" migration --rename "$breakdown_rename"
