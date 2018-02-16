#! /bin/bash

COMMIT=`git rev-parse HEAD`

# lines=""
experiment=results/word_count_$COMMIT
for f in `cat $experiment.txt`; do
  cat $experiment/$f | grep latency | cut -f3- > $experiment/$f-latency
#   lines=$lines"\"$1/$f-latency\" using (\$1/1000000000):(\$2/1000) with lines, "
done
# echo $lines
# gnuplot -p -e "set terminal png; set logscale y; plot $lines" | imgcat

keys=10240000

# gnuplot -p -e "set terminal png; set logscale y; plot \"$experiment/word_count_n02_w01_rounds10_batch100000_keys${keys}_open-loop_$COMMIT-latency\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\"," | imgcat
gnuplot -p -e "set terminal pdf size 2.3,1.1; set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/word_count_n02_w01_rounds10_batch100000_keys${keys}_open-loop_$COMMIT-latency\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\"," > word_count_scaling_n02_w01_batch100000_keys${keys}_$COMMIT.pdf


