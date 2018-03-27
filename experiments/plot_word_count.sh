#! /bin/bash

COMMIT=`git rev-parse HEAD`

mkdir -p plots

modes=("sudden" "one-by-one" "fluid")
backends=("native" "scaling")

common="set terminal pdf font \", 8\""

# === closed one-two ===
experiment=results/${COMMIT}/word_count-closed-one-two
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=10240000
batch=3000000
rounds=100

for backend in ${backends[@]}; do
    for mode in "sudden"; do
      plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w1_rounds${rounds}_batch${batch}_keys${keys}_closed_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
      gnuplot -p -e "$common; set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w01_closed_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
    done
done

# === closed one-two state throughput ===
experiment=results/${COMMIT}/word_count-closed-one-two-state
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep redistr | cut -f3- > latency-$f
  done
)

keys=(1 10 100 1000 10000 100000 1000000 10000000)
batch=1
rounds=100

for backend in "scaling"; do
    for mode in "tp"; do
        gnuplot -persist <<-EOFMarker
            set border 2 front lt black linewidth 1.000 dashtype solid
            set boxwidth 0.5 absolute
            set style fill   solid 0.50 border lt -1
            unset key
            set pointsize 0.5
            set logscale y
#            set logscale x
            set style data boxplot
#            set style boxplot nooutliers
            set xtics border in scale 0,0 nomirror norotate  autojustify
            set xtics  norangelimit
            set xtics  0,1,7
            set ytics border in scale 1,0.5 nomirror norotate  autojustify
            set xlabel "State size [\$10^x\$]"
            set ylabel "Throughput [keys/s]"
            filename(n) = sprintf("$experiment/latency-word_count_n2_w1_rounds${rounds}_batch${batch}_keys%d_closed_${mode}_${backend}", n)
            # set terminal pdf size 2.3,1.1
            set terminal tikz size 8cm,5cm
            set output "plots/word_count_scaling_n02_w01_state_throughput_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.tex"
            plot for [i=0:7] filename(10**i) using (i):(10**i/\$2*1000000000/2)
EOFMarker
        gnuplot -persist <<-EOFMarker
            set border 2 front lt black linewidth 1.000 dashtype solid
            set boxwidth 0.5 absolute
            set style fill   solid 0.50 border lt -1
            unset key
            set pointsize 0.5
            set logscale y
#            set logscale x
            set style data boxplot
#            set style boxplot nooutliers
            set xtics border in scale 0,0 nomirror norotate  autojustify
            set xtics  norangelimit
            set xtics  0,1,7
            set ytics border in scale 1,0.5 nomirror norotate  autojustify
            set xlabel "State size [\$10^x\$]"
            set ylabel "Latency [ns/key]"
            filename(n) = sprintf("$experiment/latency-word_count_n2_w1_rounds${rounds}_batch${batch}_keys%d_closed_${mode}_${backend}", n)
            # set terminal pdf size 2.3,1.1
#            set terminal tikz size 8cm,5cm
#            set output "plots/word_count_scaling_n02_w01_state_latency_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.tex"
            plot for [i=0:7] filename(10**i) using (i):(\$2/10**i)
EOFMarker
    done
done

# === one-two ===
experiment=results/${COMMIT}/word_count-constant-one-two
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=10240000
batch=1000000

for backend in ${backends[@]}; do
    for mode in ${modes[@]}; do
      plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w1_rounds10_batch${batch}_keys${keys}_constant_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
      gnuplot -p -e "$common; set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w01_constant_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
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
      multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w1_rounds10_batch${batch}_keys${keys}_constant_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\"; "
    done

    multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

    gnuplot -p -e "$common; set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w01_constant_multiplot_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
done
# === half-all constant ===
experiment=results/${COMMIT}/word_count-constant-half-all
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=40960000
batch=1000000

for backend in ${backends[@]}; do
    for mode in ${modes[@]}; do
      plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w4_rounds10_batch${batch}_keys${keys}_constant_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
      gnuplot -p -e "$common; set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w04_constant_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
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
      multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w4_rounds10_batch${batch}_keys${keys}_constant_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
    done

    multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

    gnuplot -p -e "$common; set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w04_constant_multiplot_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
done

# === half-all square ===
experiment=results/${COMMIT}/word_count-square-half-all
(
  cd $experiment;
  for f in `ls word_count*`; do
    cat $f | grep latency | cut -f3- > latency-$f
  done
)

keys=10240000
batch=2600000
rounds=10

for backend in ${backends[@]}; do
    for mode in ${modes[@]}; do
      plot="set logscale y; set xlabel \"sec (wall clock)\"; set ylabel \"latency (µsec)\"; set key off; plot \"$experiment/latency-word_count_n2_w4_rounds${rounds}_batch${batch}_keys${keys}_square_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
      gnuplot -p -e "$common; set terminal pdf size 2.3,1.1; $plot" > plots/word_count_scaling_n02_w04_square_${mode}_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
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
      multiplot="$multiplot; plot \"$experiment/latency-word_count_n2_w4_rounds${rounds}_batch${batch}_keys${keys}_square_${mode}_${backend}\" using (\$1/1000000000):(\$2/1000) with lines lt rgb \"black\","
    done

    multiplot="$multiplot; set origin 0,0; set size 1,1; set ylabel; unset format y;  unset border; set tic scale 0; set xlabel \"sec (wall clock)\"; set format x \"\"; plot (1/0); "

    gnuplot -p -e "$common; set terminal pdf size 3.5,1.1; $multiplot" > plots/word_count_scaling_n02_w04_square_multiplot_batch${batch}_keys${keys}_${backend}_$COMMIT.pdf
done
