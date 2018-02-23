extern crate fnv;
extern crate rand;

extern crate timely;
extern crate dynamic_scaling_mechanism;
use std::time::Instant;
use std::hash::{Hash, Hasher};
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Input, Map, Probe};

use dynamic_scaling_mechanism::distribution::{BIN_SHIFT, ControlInst, Control, ControlStateMachine};

// include!(concat!(env!("OUT_DIR"), "/words.rs"));

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    // let mut s = DefaultHasher::new();
    // t.hash(&mut s);
    // s.finish()
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

struct SentenceGenerator {
    rng: StdRng,
}

impl SentenceGenerator {

    fn new(index: usize) -> Self {
        let seed: &[_] = &[1, 2, 3, index];
        Self {
            rng: SeedableRng::from_seed(seed),
        }
    }

    #[inline(always)]
    pub fn word_rand(&mut self, keys: usize) -> String {
        let index = self.rng.gen_range(0, keys);
        self.word_at(index)
    }

    #[inline(always)]
    pub fn word_at(&mut self, k: usize) -> String {
        format!("{}", k)
    }

    // pub fn generate(&mut self) -> String {
    //     let sentence_length = 10;
    //     let mut sentence = String::with_capacity(sentence_length + sentence_length / 10);
    //     while sentence.len() < sentence_length {
    //         let index = self.rng.gen_range(0, WORDS.len());
    //         sentence.push_str(WORDS[index]);
    //     }
    //     sentence
    // }
}

#[derive(Debug, PartialEq, Eq)]
enum ExperimentMode {
    OpenLoopConstant,
    OpenLoopSquare,
}

#[derive(Debug, PartialEq, Eq)]
enum ExperimentMapMode {
    OneAllOne,
    HalfAllHalfSuddenAll,
    HalfAllHalfAll,
}

fn main() {
    println!("args: {:?}", std::env::args());

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
        "open-loop" => ExperimentMode::OpenLoopConstant,
        "open-loop-square" => ExperimentMode::OpenLoopSquare,
        _ => panic!("invalid mode"),
    };
    let map_mode = match args.next().unwrap().as_str() {
        "one-all-one" => ExperimentMapMode::OneAllOne,
        "half-all-halfsudden-all" => ExperimentMapMode::HalfAllHalfSuddenAll,
        "half-all-half-all" => ExperimentMapMode::HalfAllHalfAll,
        _ => panic!("invalid mode"),
    };

    println!("parameters: rounds: {}, batch: {}, keys: {}, mode: {:?}, map_mode: {:?}",
             rounds, batch, keys, mode, map_mode);

    timely::execute_from_args(args, move |worker| {

        let mut text_gen = SentenceGenerator::new(worker.index());

        let index = worker.index();
        let peers = worker.peers();

        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = scope.input_from(&mut control_input).broadcast();
            let input = scope.input_from(&mut input);

            input
                // .flat_map(|sentence: String| sentence.split_whitespace()
                //     .map(move |word| (word.to_owned(), 1))
                //     .collect::<Vec<_>>()
                // )
                .map(|x| (x, 1))
                .control_state_machine(
                    |_key: &_, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some(*agg))
                    },
                    |key| calculate_hash(key)
                    ,
                    &control
                )
                .probe_with(&mut probe);
        });

        let mut control_counter = 0;
        let mut map = vec![0; 1 << BIN_SHIFT];

        if index == 0 {
            match map_mode {
                ExperimentMapMode::OneAllOne => {
                    // Start with an initial distribution of data to worker zero.
                    control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
                },
                ExperimentMapMode::HalfAllHalfAll | ExperimentMapMode::HalfAllHalfSuddenAll => {
                    for (i, v) in map.iter_mut().enumerate() {
                        *v = (((i / 2) * 2 + (i % 2) * worker.peers() / 2) % worker.peers());
                    }
                    //eprintln!("debug: half map {:?}", map);
                    control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
                },
            }
            control_counter += 1;
        }
        control_input.advance_to(1);

        let requests_per_sec = batch;
        let ns_per_request = 1_000_000_000 / requests_per_sec;

        // -- square --
        let SQUARE_PERIOD = 2_000_000_000; // 2 sec

        let SQUARE_HEIGHT = 100000;
        let half_period = SQUARE_PERIOD / 2;
        let hi_ns_per_request = 1_000_000_000 / (requests_per_sec + SQUARE_HEIGHT);
        let lo_ns_per_request = 1_000_000_000 / (requests_per_sec - SQUARE_HEIGHT);

        let ns_times_in_period = {
            let mut ns_times_in_period = Vec::with_capacity(SQUARE_PERIOD / ns_per_request);
            if mode == ExperimentMode::OpenLoopSquare {
                let mut cur_ns = 0;
                while cur_ns < SQUARE_PERIOD && ns_times_in_period.len() < ns_times_in_period.capacity() {
                    ns_times_in_period.push(cur_ns);
                    cur_ns += if cur_ns < half_period { lo_ns_per_request } else { hi_ns_per_request };
                }
                // assert_eq!(ns_times_in_period.len(), SQUARE_PERIOD / ns_per_request);
            }
            ns_times_in_period
        };
        // ------------


        // introduce data and watch!
        for i in 0 .. keys / peers {
            input.send(text_gen.word_at(i * peers + index));
        }
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
        eprintln!("debug: data loaded");

        // rounds: number of seconds until reconfiguration.
        // batch: target number of records per second.
        eprintln!("debug: mode: {:?}, map mode: {:?}", mode, map_mode);
        let mut request_counter = peers + index;    // skip first request for each.

        // we will run for k * rounds seconds, with r reconfigurations.
        let reconfigs = match map_mode {
            ExperimentMapMode::OneAllOne => 2,
            ExperimentMapMode::HalfAllHalfAll | ExperimentMapMode::HalfAllHalfSuddenAll => 3,
        };
        let mut measurements = Vec::with_capacity((reconfigs + 1) * rounds * requests_per_sec / peers);
        let mut to_print = Vec::with_capacity((reconfigs + 1) * rounds * requests_per_sec / peers);
        let mut redistribution_end = Vec::with_capacity(5);

        let timer = ::std::time::Instant::now();

        let mut control_plan = Vec::new();

        if index == 0 {
            for (i, v) in map.iter_mut().enumerate() {
                *v = i % worker.peers();
            }
            let first_round_map = map.clone();
            control_plan.push((rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
            control_counter += 1;

            match map_mode {
                ExperimentMapMode::OneAllOne => {
                    for i in 0 .. map.len() {
                        map[i] = 0;
                        control_plan.push((2 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                        control_counter += 1;
                    }
                },
                ExperimentMapMode::HalfAllHalfAll => {
                    // all to half, one-by-one
                    map = first_round_map;
                    //eprintln!("debug: second migration plan");

                    for i in 0 .. map.len() {
                        map[i] = (((i / 2) * 2 + (i % 2) * worker.peers() / 2) % worker.peers());
                        //eprintln!("debug: all to half {:?}", map);
                       control_plan.push((2 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                       control_counter += 1;
                    }

                    // half to all, two-by-two
                    for b in 0 .. (map.len() / worker.peers()) {
                        for i in 0 .. worker.peers() {
                            let cur = (b * worker.peers()) + i;
                            map[cur] = cur % worker.peers();
                        }
                        //eprintln!("debug: half to all {:?}", map);
                        control_plan.push((3 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                        control_counter += 1;
                    }
                },
                ExperimentMapMode::HalfAllHalfSuddenAll => {
                    // all to half, one-by-one
                    map = first_round_map;
                    //eprintln!("debug: second migration plan");

                    for i in 0 .. map.len() {
                        map[i] = (((i / 2) * 2 + (i % 2) * worker.peers() / 2) % worker.peers());
                        //eprintln!("debug: all to half {:?}", map);
                    }
                    control_plan.push((2 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                    control_counter += 1;

                    // half to all, two-by-two
                    for b in 0 .. (map.len() / worker.peers()) {
                        for i in 0 .. worker.peers() {
                            let cur = (b * worker.peers()) + i;
                            map[cur] = cur % worker.peers();
                        }
                        //eprintln!("debug: half to all {:?}", map);
                        control_plan.push((3 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                        control_counter += 1;
                    }
                },
            }
        }

        let mut just_redistributed = false;
        let mut redistributions = Vec::with_capacity(256);

        while measurements.len() < measurements.capacity() {

            // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
            let elapsed = timer.elapsed();
            let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

            let skip_redistribution = just_redistributed;
            just_redistributed = false;
            // If the next planned migration can now be effected, ...
            if control_plan.get(0).map(|&(time, _)| time < elapsed_ns as usize).unwrap_or(false) {
                if !skip_redistribution {
                    redistributions.push(elapsed_ns);
                    control_input.send(control_plan.remove(0).1);
                    just_redistributed = true;
                }
            }

            match mode {
                ExperimentMode::OpenLoopConstant => {

                    // Introduce any requests that have "arrived" since last we were here.
                    // Request i "arrives" at `index + ns_per_request * (i + 1)`.
                    while ((request_counter * ns_per_request) as u64) < elapsed_ns {
                        // input.send(text_gen.generate());
                        input.send(text_gen.word_rand(keys));
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
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                    if just_redistributed {
                        redistribution_end.push(elapsed_ns);
                    }

                    // any un-recorded measurements that are complete should be recorded.
                    while (((index + peers * (measurements.len() + 1)) * ns_per_request) as u64) < acknowledged_ns && measurements.len() < measurements.capacity() {
                        let requested_at = ((index + peers * (measurements.len() + 1)) * ns_per_request) as u64;
                        measurements.push(elapsed_ns - requested_at);
                        to_print.push((requested_at, elapsed_ns - requested_at))
                    }
                },
                ExperimentMode::OpenLoopSquare => {
                    // ---|   |---|    <- hi_requests_per_sec
                    //    |   |   |
                    //    |---|   |--- <- lo_requesrs_per_sec
                    // <------>
                    //  period

                    let ns_times_in_period_index = |counter| counter % ns_times_in_period.len();
                    let time_base = |counter| counter / ns_times_in_period.len() * SQUARE_PERIOD;

                    while time_base(request_counter + peers) + ns_times_in_period[ns_times_in_period_index(request_counter + peers)] < (elapsed_ns as usize) {
                        input.send(text_gen.word_rand(keys));
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
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                    if just_redistributed {
                        redistribution_end.push(elapsed_ns);
                    }

                    // any un-recorded measurements that are complete should be recorded.
                    while (time_base(index + peers * (measurements.len() + 1)) + ns_times_in_period[ns_times_in_period_index(index + peers * (measurements.len() + 1))]) < acknowledged_ns as usize && measurements.len() < measurements.capacity() {
                        let requested_at = (time_base(index + peers * (measurements.len() + 1)) + ns_times_in_period[ns_times_in_period_index(index + peers * (measurements.len() + 1))]) as u64;
                        measurements.push(elapsed_ns - requested_at);
                        to_print.push((requested_at, elapsed_ns - requested_at))
                    }
                },
            }
        }

        measurements.sort();

        let min = measurements[0];
        let med = measurements[measurements.len() / 2];
        let p99 = measurements[99 * measurements.len() / 100];
        let max = measurements[measurements.len() - 1];

        if index == 0 {
            println!("worker {:02}:\t{}\t{}\t{}\t{}\t(of {} measurements)", index, min, med, p99, max, measurements.len());

            let thing = to_print.len() / 1000;
            for i in 0 .. to_print.len() {
                if i % thing == 0 {
                    println!("{:02}\tlatency\t{:?}\t{:?}", index, to_print[i].0, to_print[i].1);
                }
            }

            println!();
            for elt in redistributions.iter() {
                println!("{:02}\tredistr\t{:?}\t10000000", index, elt);
            }

            println!();
            for rede in redistribution_end.iter() {
                println!("{:02}\tred_end\t{:?}", index, rede);
            }
        }

    }).unwrap();
}
