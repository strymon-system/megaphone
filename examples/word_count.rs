extern crate fnv;
extern crate rand;
extern crate zipf;

extern crate timely;
extern crate dynamic_scaling_mechanism;
use std::hash::{Hash, Hasher};
use rand::{Rng, SeedableRng, StdRng};
use rand::distributions::Sample;
use zipf::ZipfDistribution;

use timely::ExchangeData;
use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Filter, Input, Map, Probe};
use timely::dataflow::operators::aggregation::StateMachine;

use dynamic_scaling_mechanism::{BIN_SHIFT, ControlInst, Control};
use dynamic_scaling_mechanism::distribution::ControlStateMachine;
use dynamic_scaling_mechanism::stateful::Stateful;
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;

// include!(concat!(env!("OUT_DIR"), "/words.rs"));

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

enum WordGenerator {
    Uniform(StdRng, usize),
    Zipf(StdRng, ZipfDistribution),
}

impl WordGenerator {

    fn new_uniform(index: usize, keys: usize) -> Self {
        let seed: &[_] = &[1, 2, 3, index];
        WordGenerator::Uniform(SeedableRng::from_seed(seed), keys)
    }

    fn new_zipf(index: usize, keys: usize, s: f64) -> Self {
        let seed: &[_] = &[1, 2, 3, index];
        WordGenerator::Zipf(SeedableRng::from_seed(seed), ZipfDistribution::new(keys, s).unwrap())
    }

    #[inline(always)]
    pub fn word_rand(&mut self) -> u64 {
        let index = match *self {
            WordGenerator::Uniform(ref mut rng, ref keys) => rng.gen_range(0, *keys),
            WordGenerator::Zipf(ref mut rng, ref mut zipf) => Sample::<isize>::sample(zipf, rng) as usize,
        };
        self.word_at(index)
    }

    #[inline(always)]
    pub fn word_at(&mut self, k: usize) -> u64 {
        k as u64
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ExperimentMode {
    ClosedLoop,
    OpenLoopConstant,
    OpenLoopSquare,
}

#[derive(Debug, PartialEq, Eq)]
enum ExperimentMapMode {
    Sudden,
    OneByOne,
    Fluid,
    StateThroughput,
}

#[derive(Debug, PartialEq, Eq)]
enum Backend {
    Native,
    Noop,
    Scaling,
    Generic,
}

const HDHISTOGRAM_BITS: usize = 4;

#[derive(Clone, Debug)]
struct HDHistogram {
    counts: Vec<[u64; 1 << HDHISTOGRAM_BITS]>,
}

impl HDHistogram {
    pub fn add_value(&mut self, value: u64) {
        let index = value.next_power_of_two().trailing_zeros() as usize;
        let low_bits = (value >> (index - HDHISTOGRAM_BITS - 1)) & ((1 << HDHISTOGRAM_BITS) - 1);
        self.counts[index][low_bits as usize] += 1;
    }

    pub fn flatten(&self) -> Vec<(u64, f64, u64)> {
        let mut results = Vec::new();
        let total = self.counts.iter().map(|x| x.iter().sum::<u64>()).sum();
        let mut sum = 0;
        for index in (0 .. self.counts.len()).rev() {
            for sub in (0 .. (1 << HDHISTOGRAM_BITS)).rev() {
                if sum > 0 && sum < total && self.counts[index][sub] > 0 {
                    let latency = (1 << (index - 1)) + (sub << (index - HDHISTOGRAM_BITS - 1));
                    let fraction = (sum as f64) / (total as f64);
                    results.push((latency as u64, fraction, self.counts[index][sub]));
                }
                sum += self.counts[index][sub];
            }
        }
        results.reverse();
        results
    }
}

impl Default for HDHistogram {
    fn default() -> Self {
        HDHistogram {
            counts: vec![[0u64; 16]; 64],
        }
    }
}

fn duration_to_nanos(duration: ::std::time::Duration) -> u64 {
    (duration.as_secs() * 1_000_000_000).checked_add(duration.subsec_nanos() as u64).unwrap()
}

fn verify<S: Scope, T: ExchangeData+Ord+::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    use timely::dataflow::operators::Binary;
    use timely::dataflow::channels::pact::Exchange;
    use std::collections::HashMap;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    correct.binary_notify::<_, (), _, _, _>(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data.drain(..));
                not.notify_at(time.retain());
            });
            not.for_each(|time, _, _| {
                let mut v1 = in1_pending.remove(time.time()).unwrap_or_default();
                let mut v2 = in2_pending.remove(time.time()).unwrap_or_default();
                v1.sort();
                v2.sort();
                assert_eq!(v1.len(), v2.len());
                let i1 = v1.iter();
                let i2 = v2.iter();
                for (a, b) in i1.zip(i2) {
//                    println!("a: {:?}, b: {:?}", a, b);
                    assert_eq!(a, b, " at {:?}", time.time());
                }
            })
        }
    )
}

fn main() {
    println!("args: {:?}", std::env::args());
    println!("bin-shift: {:?}", ::dynamic_scaling_mechanism::BIN_SHIFT);

    let mut args = std::env::args();
    let _cmd = args.next();

    // How many rounds at each key distribution strategy.
    let rounds: usize = args.next().unwrap().parse().unwrap();
    // How many updates to perform in each round.
    let batch: usize = args.next().unwrap().parse().unwrap();
    // Number of distinct keys.
    let keys: usize = args.next().unwrap().parse().unwrap();
    // Open-loop?
    let mode = match args.next().unwrap().as_str() {
        "closed" => ExperimentMode::ClosedLoop,
        "constant" => ExperimentMode::OpenLoopConstant,
        "square" => ExperimentMode::OpenLoopSquare,
        _ => panic!("invalid mode"),
    };
    let map_mode = match args.next().unwrap().as_str() {
        "sudden" => ExperimentMapMode::Sudden,
        "one-by-one" => ExperimentMapMode::OneByOne,
        "fluid" => ExperimentMapMode::Fluid,
        "tp" => ExperimentMapMode::StateThroughput,
        _ => panic!("invalid mode"),
    };

    let backend = match args.next().unwrap().as_str() {
        "native" => Backend::Native,
        "noop" => Backend::Noop,
        "scaling" => Backend::Scaling,
        "generic" => Backend::Generic,
        _ => panic!("invalid backend"),
    };

    let random = args.next().unwrap();

    println!("parameters: rounds: {}, batch: {}, keys: {}, mode: {:?}, map_mode: {:?}, backend: {:?}",
             rounds, batch, keys, mode, map_mode, backend);

    timely::execute_from_args(args, move |worker| {

        let mut text_gen = match random.as_str() {
            "uniform" => WordGenerator::new_uniform(worker.index(), keys),
            "zipf" => WordGenerator::new_zipf(worker.index(), keys, 1.07f64),
            random => panic!("invalid rng: {}", random),
        };

        let index = worker.index();
        let peers = worker.peers();

        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = scope.input_from(&mut control_input).broadcast();
            let input = scope.input_from(&mut input);

            let fold = |_key: &_, val, agg: &mut u64| {
                *agg += val;
                (false, Some((*_key, *agg)))
            };

            let input: Stream<_, (_, u64)> = input
                .map(|x| (x, 1));
            let output = match backend {
                Backend::Native => input
                    .state_machine( fold, |key| calculate_hash(key)),
                Backend::Noop => input.filter(|_| false),
                Backend::Scaling => input
                    .control_state_machine(
                        fold,
                        |key| calculate_hash(key),
                        &control
                    ),
                Backend::Generic => {
                    let mut stateful = input
                        .stateful(|key| calculate_hash(&key.0), &control);
                    stateful.state_machine(fold)
                },
            };
            let validate = false;
            if validate {
                let correct = input
                    .state_machine(|_key: &_, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((*_key, *agg)))
                    }, |key| calculate_hash(key));
                verify(&output, &correct).probe_with(&mut probe);
            }
            output.probe_with(&mut probe);
        });

        let mut control_counter = 0;
        let mut map = vec![0; 1 << BIN_SHIFT];
        // TODO(moritzo) HAAAACCCCKKK
        if peers != 2 {
            for (i, v) in map.iter_mut().enumerate() {
                *v = ((i / 2) * 2 + (i % 2) * peers / 2) % peers;
            }
        }
        if index == 0 {
            eprintln!("debug: initial configuration: {:?}", map);
        }

        if index == 0 {
            control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
            control_counter += 1;
        }
        control_input.advance_to(1);

        let requests_per_sec = batch;
        let ns_per_request = 1_000_000_000 / requests_per_sec;

        // -- square --
        let square_period = 2_000_000_000; // 2 sec

        let square_height = 100000;
        let half_period = square_period / 2;
        let hi_ns_per_request = 1_000_000_000 / (requests_per_sec + square_height);
        let lo_ns_per_request = 1_000_000_000 / (requests_per_sec.checked_sub(square_height).unwrap_or(1));

        let ns_times_in_period = {
            let mut ns_times_in_period = Vec::with_capacity(square_period / ns_per_request);
            if mode == ExperimentMode::OpenLoopSquare {
                let mut cur_ns = 0;
                while cur_ns < square_period && ns_times_in_period.len() < ns_times_in_period.capacity() {
                    ns_times_in_period.push(cur_ns);
                    cur_ns += if cur_ns < half_period { lo_ns_per_request } else { hi_ns_per_request };
                }
                eprintln!("debug: period length: {} {}", ns_times_in_period.len(), square_period / ns_per_request);
            }
            ns_times_in_period
        };
        // ------------

        let mut control_plan = Vec::new();

        let num_migrations = match map_mode {
            ExperimentMapMode::Sudden => 1,
            ExperimentMapMode::OneByOne => map.len(),
            ExperimentMapMode::Fluid => map.len() / peers,
            ExperimentMapMode::StateThroughput => 2 * (rounds / 2 - 1),
        };
        let batches_per_migration = map.len() / num_migrations;

        if index == 0 {
            match map_mode {
                ExperimentMapMode::StateThroughput => {
                    assert_eq!(mode, ExperimentMode::ClosedLoop, "ExperimentMapMode::StateThroughput only works with closed loop.");
                    let initial_map = map.clone();
                    for i in 0..map.len() {
                        map[i] = i % peers;
                    }
                    eprintln!("debug: setting up reconfiguration: {:?}", map);

                    for round in 1..rounds/2 {
                        control_plan.push((round * 1_000_000_000, Control::new(control_counter, 1, ControlInst::Map(map.clone()))));
                        control_counter += 1;
                        control_plan.push((round * 1_000_000_000, Control::new(control_counter, 1, ControlInst::Map(initial_map.clone()))));
                        control_counter += 1;
                    }

                },
                _ => {
                    for i in 0..map.len() {
                        map[i] = i % peers;

                        if i % batches_per_migration == batches_per_migration - 1 {
                            eprintln!("debug: setting up reconfiguration: {:?}", map);
                            control_plan.push((rounds * 1_000_000_000, Control::new(control_counter, 1, ControlInst::Map(map.clone()))));
                            control_counter += 1;
                        }
                    }
                }
            }
            assert_eq!(control_counter as usize, num_migrations + 1);
        }

        // rounds: number of seconds until reconfiguration.
        // batch: target number of records per second.
        eprintln!("debug: mode: {:?}, map mode: {:?}", mode, map_mode);
        let mut request_counter = peers + index;    // skip first request for each.

        // we will run for 2 * rounds seconds, with 1 reconfiguration.
        let mut measurements = if mode == ExperimentMode::ClosedLoop {
            Vec::with_capacity(2 * rounds)
        } else {
            Vec::with_capacity(2 * rounds * requests_per_sec / peers)
        };
        let mut to_print = Vec::with_capacity(measurements.capacity());
        let mut redistribution_end = Vec::with_capacity(1024);
        let mut requests_produced = Vec::with_capacity(measurements.capacity());

        // introduce data and watch!
        for i in 0 .. keys {
            input.send(text_gen.word_at(i));
        }
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
        eprintln!("debug: data loaded");

        let timer = ::std::time::Instant::now();

        let mut just_redistributed = false;
        let mut redistributions = Vec::with_capacity(256);

        while measurements.len() < measurements.capacity() {

            // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
            let elapsed = timer.elapsed();
            let elapsed_ns = duration_to_nanos(elapsed);

            let skip_redistribution = just_redistributed;
            just_redistributed = false;
            // If the next planned migration can now be effected, ...
            if !skip_redistribution && match mode {
                ExperimentMode::ClosedLoop =>
                    control_plan.get(0).is_some() && measurements.len() * 2 >= measurements.capacity(),
                _ =>
                    control_plan.get(0).map(|&(time, _)| time < elapsed_ns as usize).unwrap_or(false),
            } {
                redistributions.push(elapsed_ns);
                control_input.send(control_plan.remove(0).1);
                just_redistributed = true;
            }

            match mode {
                ExperimentMode::ClosedLoop => {
                    let requested = timer.elapsed();
                    let requested_at = requested.as_secs() * 1_000_000_000 + (requested.subsec_nanos() as u64);

                    for i in 0..batch/peers {
                        input.send(text_gen.word_rand());
                        request_counter += peers;
                    }
                    input.advance_to(measurements.len() + 2);
                    control_input.advance_to(measurements.len() + 2);

                    while probe.less_than(input.time()) {
                        worker.step();
                    }

                    let elapsed = timer.elapsed();
                    let elapsed_ns = duration_to_nanos(elapsed);

                    if just_redistributed {
                        redistribution_end.push(elapsed_ns);
                    }

                    // any un-recorded measurements that are complete should be recorded.
                    measurements.push(elapsed_ns - requested_at);
                    to_print.push((requested_at, elapsed_ns - requested_at));
                    requests_produced.push(request_counter);
                },
                ExperimentMode::OpenLoopConstant => {

                    // Introduce any requests that have "arrived" since last we were here.
                    // Request i "arrives" at `index + ns_per_request * (i + 1)`.
                    while ((request_counter * ns_per_request) as u64) < elapsed_ns {
                        // input.send(text_gen.generate());
                        input.send(text_gen.word_rand());
                        request_counter += peers;
                    }
                    input.advance_to(elapsed_ns as usize);
                    control_input.advance_to(elapsed_ns as usize);

                    while probe.less_than(input.time()) {
                        worker.step();
                    }

                    // Determine completed ns.
                    let acknowledged_ns: u64 = probe.with_frontier(|frontier| frontier[0].inner as u64);

                    let elapsed = timer.elapsed();
                    let elapsed_ns = duration_to_nanos(elapsed);

                    if just_redistributed {
                        redistribution_end.push(elapsed_ns);
                    }

                    // any un-recorded measurements that are complete should be recorded.
                    while (((index + peers * (measurements.len() + 1)) * ns_per_request) as u64) < acknowledged_ns && measurements.len() < measurements.capacity() {
                        let requested_at = ((index + peers * (measurements.len() + 1)) * ns_per_request) as u64;
                        measurements.push(elapsed_ns - requested_at);
                        to_print.push((requested_at, elapsed_ns - requested_at));
                        requests_produced.push(request_counter);
                    }
                },
                ExperimentMode::OpenLoopSquare => {
                    // ---|   |---|    <- hi_requests_per_sec
                    //    |   |   |
                    //    |---|   |--- <- lo_requesrs_per_sec
                    // <------>
                    //  period

                    let ns_times_in_period_index = |counter| counter % ns_times_in_period.len();
                    let time_base = |counter| counter / ns_times_in_period.len() * square_period;

                    while time_base(request_counter + peers) + ns_times_in_period[ns_times_in_period_index(request_counter + peers)] < (elapsed_ns as usize) {
                        input.send(text_gen.word_rand());
                        request_counter += peers;
                    }
                    input.advance_to(elapsed_ns as usize);
                    control_input.advance_to(elapsed_ns as usize);

                    while probe.less_than(input.time()) {
                        worker.step();
                    }

                    // Determine completed ns.
                    let acknowledged_ns: u64 = probe.with_frontier(|frontier| frontier[0].inner as u64);

                    let elapsed = timer.elapsed();
                    let elapsed_ns = duration_to_nanos(elapsed);

                    if just_redistributed {
                        redistribution_end.push(elapsed_ns);
                    }

                    // any un-recorded measurements that are complete should be recorded.
                    while (time_base(index + peers * (measurements.len() + 1)) + ns_times_in_period[ns_times_in_period_index(index + peers * (measurements.len() + 1))]) < acknowledged_ns as usize && measurements.len() < measurements.capacity() {
                        let requested_at = (time_base(index + peers * (measurements.len() + 1)) + ns_times_in_period[ns_times_in_period_index(index + peers * (measurements.len() + 1))]) as u64;
                        measurements.push(elapsed_ns - requested_at);
                        to_print.push((requested_at, elapsed_ns - requested_at));
                        requests_produced.push(request_counter);
                    }
                },
            }
        }

        let total_duration_ns = duration_to_nanos(timer.elapsed());

        measurements.sort();

        let min = measurements[0];
        let p01 = measurements[measurements.len() / 100];
        let p25 = measurements[25*measurements.len() / 100];
        let med = measurements[measurements.len() / 2];
        let p75 = measurements[75*measurements.len() / 100];
        let p99 = measurements[99 * measurements.len() / 100];
        let max = measurements[measurements.len() - 1];

        let mut requests_sum = 0;
        for i in 0..requests_produced.len() {
            requests_produced[i] -= requests_sum;
            requests_sum += requests_produced[i];
        }

        if index == 0 {
            let l2tp = |count: usize, latency: u64| {
                count as f64 / latency as f64 * 1_000_000_000f64 / 1_000_000f64
            };

            println!("#worker id:\tmin\tp01\tp25\tmed\tp75\tp99\tmax");
            println!("worker {:02}:\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t(of {} measurements)", index, min, p01, p25, med, p75, p99, max, measurements.len());
            println!("worker {:02} overall tp: {:.3}", index, l2tp(request_counter, total_duration_ns));
            if mode == ExperimentMode::ClosedLoop {
                println!("worker {:02} tp:\t{:.1}\t{:.1}\t{:.1}\t{:.1}\t{:.1}\t{:.1}\t{:.1}\t(of {} measurements)", index, l2tp(batch, min), l2tp(batch, p01), l2tp(batch, p25), l2tp(batch, med), l2tp(batch, p75), l2tp(batch, p99), l2tp(batch, max), measurements.len());
            }

            let sample_frequency = to_print.len() / ::std::cmp::min(1000, to_print.len());

            let mut hdhist: Vec<HDHistogram> = vec![Default::default(); 3];
            for i in 0 .. to_print.len() {

                let (phase, hdhist) = if redistributions.first().map(|end| to_print[i].0 < *end).unwrap_or(false) {
                    ('A', &mut hdhist[0])
                } else if redistribution_end.last().map(|end| to_print[i].0 > *end).unwrap_or(false) {
                    ('C', &mut hdhist[2])
                } else {
                    ('B', &mut hdhist[1])
                };

                hdhist.add_value(to_print[i].1);

                if i % sample_frequency == 0 {
                    println!("{:02}\tlatency\t{:?}\t{:?}\t{:?}", index, to_print[i].0, to_print[i].1, phase);
                }
            }

            println!();
            for (elt, end) in redistributions.iter().zip(redistribution_end.iter()) {
                println!("{:02}\tredistr\t{:?}\t{:?}", index, elt, end - elt);
            }

            println!();
            for rede in &redistribution_end {
                println!("{:02}\tred_end\t{:?}", index, rede);
            }

            println!("latencies: latency franction");
            for (i, phase) in vec!['A', 'B', 'C'].iter().enumerate() {
                for (latency, fraction, sum) in hdhist[i].flatten() {
                    println!("ccdf {}\t{}\t{:0.5}\t{}", phase, latency, fraction, sum);
                }
            }

        }

    }).unwrap();
}
