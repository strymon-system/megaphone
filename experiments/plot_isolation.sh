#! /bin/bash

#set -e
set -o pipefail

COMMIT=`git rev-parse HEAD`

mkdir -p plots/$COMMIT

backends=("native" "scaling")

out_format=pdf
out_ext=pdf

common="set terminal ${out_format} font \", 8\""

function cdf() {
    mode=""
    cat <<-EOF
    set grid xtics ytics
    plot \
        "${file}-A" using 3:(1) smooth cnorm t "Before", \
        "${file}-C" using 3:(1) smooth cnorm t "After"
EOF
}

function cdf99() {
    mode=""
    cat <<-EOF
    set yrange [0.00:.99999]
    set nonlinear y via -log10(1-y) inverse 1-10**(-y)
    $(cdf)
EOF
}

# === one-two ===
experiment=results/${COMMIT}/isolation-one-two
(
  cd $experiment;
  for f in `ls isolation*`; do
    cat $f | grep latency | cut -f3- | tee latency-$f | tee >(grep A > latency-$f-A) | tee >(grep B > latency-$f-B) | (grep C > latency-$f-C)
  done
)

keys=10240000
batch=1000000

for backend in ${backends[@]}; do
    file="$experiment/latency-isolation_n2_w1_rounds10_batch${batch}_keys${keys}_${backend}"
    plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"${file}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
    gnuplot -p -e "$common; set terminal ${out_format} size 2.3,1.1; $plot" > plots/$COMMIT/isolation_scaling_n02_w01_${mode}_batch${batch}_keys${keys}_${backend}.${out_ext}
    gnuplot -persist <<-EOFMarker
        set title "Constant ${batch}/s, ${keys} keys, ${backend}"
        set logscale x
        set xrange [10:*]
        set xlabel "Latency [ns]"
        set ylabel "CDF"
        set terminal ${out_format} size 9cm,5cm
        set output "plots/$COMMIT/isolation_scaling_n02_w01_${mode}_batch${batch}_keys${keys}_${backend}-cdf.${out_ext}"
        $(cdf)
        set output "plots/$COMMIT/isolation_scaling_n02_w01_${mode}_batch${batch}_keys${keys}_${backend}-cdf99.${out_ext}"
        $(cdf99)
EOFMarker
done
