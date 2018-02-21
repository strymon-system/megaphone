#! /bin/bash

set -e


COMMIT=`git rev-parse HEAD`
echo commit: $COMMIT

mkdir -p results/word_count_${COMMIT}

ssh -t andreal@sgs-r820-01 "cd /home/andreal/Src/dynamic-scaling-mechanism; hwloc-bind socket:0 -- cargo build --release --example word_count"

keys_base=40000
keys=$keys_base

rounds=10
batch=100000
open_loop="open-loop"
map_mode="one-all-one"
processes=2

echo "" > results/word_count_${COMMIT}.txt

for i in `seq 1 10`; do
  keys=`expr $keys \* 2`

  filename="word_count_n02_w01_rounds${rounds}_batch${batch}_keys${keys}_${open_loop}_${COMMIT}"
  echo $i '->' $keys 'keys' 'filename: ' $filename

  echo $filename >> results/word_count_${COMMIT}.txt
  for p in `seq 0 1`; do
    ssh -t andreal@sgs-r820-01 "cd /home/andreal/Src/dynamic-scaling-mechanism; hwloc-bind socket:$p -- cargo run --release --example word_count -- $rounds $batch $keys ${open_loop} ${map_mode} -n 2 -p $p -w 1" > results/word_count_${COMMIT}/$filename &
  done
  wait
done

# cat results/word_count_${COMMIT}/word_count_n02_w01_rounds${rounds}_batch${batch}_keys${keys}_${open_loop}_${COMMIT} | grep latency | cut -f3- | gnuplot -p -e 'set terminal png; set logscale y; plot "/dev/stdin" using 1:2 with lines' | imgcat


