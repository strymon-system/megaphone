#! /bin/bash

COMMIT=`git rev-parse HEAD`

mkdir -p plots

modes=("sudden" "one-by-one" "fluid")

# === one-two ===
experiment=results/${COMMIT}_word_count-constant-one-two
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=10240000
batch=1000000

for mode in ${modes[@]}; do
  plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w1_rounds10_batch${batch}_keys${keys}_constant_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
  gnuplot -p -e "set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w01_constant_${mode}_batch${batch}_keys${keys}_$COMMIT.pdf
done

multiplot="set logscale y; set ylabel \"latency (µsec)\"; set key off; set yrange [10:10000000]; set multiplot; "

multiplot="$multiplot; unset ylabel; set format y \"\"; set xtics 5; set size 0.3,0.95; "

for mode in ${modes[@]}; do
  if [ $mode == "sudden" ]; then
    multiplot="$multiplot; set origin 0.1,0.05; "
  fi
  if [ $mode == "one-by-one" ]; then
    multiplot="$multiplot; set origin 0.4,0.05; "
  fi
  if [ $mode == "fluid" ]; then
    multiplot="$multiplot; set origin 0.7,0.05; "
  fi
  multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w1_rounds10_batch${batch}_keys${keys}_constant_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\"; "
done

multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

gnuplot -p -e "set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w01_constant_multiplot_batch${batch}_keys${keys}_$COMMIT.pdf

# === half-all constant ===
experiment=results/${COMMIT}_word_count-constant-half-all
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=40960000
batch=1000000

for mode in ${modes[@]}; do
  plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w4_rounds10_batch${batch}_keys${keys}_constant_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
  gnuplot -p -e "set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w04_constant_${mode}_batch${batch}_keys${keys}_$COMMIT.pdf
done

multiplot="set logscale y; set ylabel \"latency (µsec)\"; set key off; set yrange [10:10000000]; set multiplot; "

multiplot="$multiplot; unset ylabel; set format y \"\"; set xtics 5; set size 0.3,0.95; "

for mode in ${modes[@]}; do
  if [ $mode == "sudden" ]; then
    multiplot="$multiplot; set origin 0.1,0.05; "
  fi
  if [ $mode == "one-by-one" ]; then
    multiplot="$multiplot; set origin 0.4,0.05; "
  fi
  if [ $mode == "fluid" ]; then
    multiplot="$multiplot; set origin 0.7,0.05; "
  fi
  multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w4_rounds10_batch${batch}_keys${keys}_constant_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
done

multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

gnuplot -p -e "set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w04_constant_multiplot_batch${batch}_keys${keys}_$COMMIT.pdf

# === half-all square ===
experiment=results/${COMMIT}_word_count-square-half-all
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=10240000
batch=2700000
rounds=10

for mode in ${modes[@]}; do
  plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w4_rounds${rounds}_batch${batch}_keys${keys}_square_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
  gnuplot -p -e "set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w04_square_${mode}_batch${batch}_keys${keys}_$COMMIT.pdf
done

multiplot="set logscale y; set ylabel \"latency (µsec)\"; set key off; set yrange [10:10000000]; set multiplot; "

multiplot="$multiplot; unset ylabel; set format y \"\"; set xtics 5; set size 0.3,0.95; "

for mode in ${modes[@]}; do
  if [ $mode == "sudden" ]; then
    multiplot="$multiplot; set origin 0.1,0.05; "
  fi
  if [ $mode == "one-by-one" ]; then
    multiplot="$multiplot; set origin 0.4,0.05; "
  fi
  if [ $mode == "fluid" ]; then
    multiplot="$multiplot; set origin 0.7,0.05; "
  fi
  multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w4_rounds${rounds}_batch${batch}_keys${keys}_square_${mode}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
done

multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

gnuplot -p -e "set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w04_square_multiplot_batch${batch}_keys${keys}_$COMMIT.pdf
