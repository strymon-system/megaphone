#! /bin/bash

COMMIT=`git rev-parse HEAD`

experiment=results/dirtya1594ca2e0d1353e7c2acfe695db3604519368fd_word_count-open-loop-square-half-all-half-all
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=$2
batch=$1

mkdir -p plots

plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w4_rounds30_batch${batch}_keys${keys}_open-loop-square_half-all-halfsudden-all\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
gnuplot -p -e "set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w01_batch${batch}_keys${keys}_$COMMIT.pdf

gnuplot -p -e "set terminal png; $plot" | imgcat


