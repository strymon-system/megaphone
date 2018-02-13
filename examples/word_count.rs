extern crate fnv;
extern crate rand;

extern crate timely;
extern crate dynamic_scaling_mechanism;

use std::time::Instant;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Input, Map, Probe, Inspect};

use dynamic_scaling_mechanism::distribution::{BIN_SHIFT, ControlInst, Control, ControlStateMachine};


include!(concat!(env!("OUT_DIR"), "/words.rs"));

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

    pub fn word(&mut self) -> String {
        let index = self.rng.gen_range(0, WORDS.len());
        WORDS[index].to_string()
    }

    pub fn generate(&mut self) -> String {
        let sentence_length = 10;
        let mut sentence = String::with_capacity(sentence_length + sentence_length / 10);
        while sentence.len() < sentence_length {
            let index = self.rng.gen_range(0, WORDS.len());
            sentence.push_str(WORDS[index]);
        }
        sentence
    }
}

fn main() {
    timely::execute_from_args(std::env::args().skip(4), |worker| {

        let mut text_gen = SentenceGenerator::new(worker.index());

        // How many rounds at each key distribution strategy.
        let rounds: usize = std::env::args().nth(1).unwrap().parse().unwrap();
        // How many updates to perform in each round.
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        // Number of distinct keys.
        let keys: usize = std::env::args().nth(3).unwrap().parse().unwrap();
        let open_loop = std::env::args().any(|x| x == "open-loop");

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

        // Start with an initial distribution of data to worker zero.
        if index == 0 {
            control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
            control_counter += 1;
        }
        control_input.advance_to(1);

        // introduce data and watch!
        for _ in 0 .. keys / worker.peers() {
            input.send(text_gen.word());
        }
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
        println!("data loaded");

        if !open_loop {

            if index == 0 {
                // Redistribute bins to all workers at once.
                control_input.advance_to(rounds);
                for i in 0 .. map.len() {
                    map[i] = i % worker.peers();
                }
                control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
                control_counter += 1;

                // Attempt progressive migration.
                for i in 0 .. map.len() {
                    map[i] = 0;
                    control_input.advance_to(2 * rounds + i);
                    control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
                    control_counter += 1;
                }
            }

            control_input.close();

            let mut durations = Vec::with_capacity(3 * rounds);
            for _round in 0 .. 3 * rounds {
                let start = Instant::now();
                for _ in 0 .. batch {
                    input.send(text_gen.word());
                }
                let time = input.time().inner;
                input.advance_to(time + 1);
                while probe.less_than(input.time()) {
                    worker.step();
                }

                durations.push(start.elapsed());
            }

            for (iter, duration) in durations.drain(..).enumerate() {
                if iter < 5 || (iter > rounds - 5 && iter < rounds + 5) || (iter > 2*rounds - 5 && iter < 2*rounds + 5) {
                    println!("iter: {:?}, duration: {:?}", 2 + iter, duration);
                }
            }
        }
        else {

            // rounds: number of seconds until reconfiguration.
            // batch: target number of records per second.


            let requests_per_sec = batch;
            let ns_per_request = 1_000_000_000 / requests_per_sec;
            let mut request_counter = peers + index;    // skip first request for each.

            // we will run for 3 * rounds seconds, with two reconfigurations.
            let mut measurements = Vec::with_capacity(3 * rounds * requests_per_sec / peers);
            let mut to_print = Vec::with_capacity(3 * rounds * requests_per_sec / peers);

            let timer = ::std::time::Instant::now();

            let mut control_plan = Vec::new();

            if index == 0 {
                for i in 0 .. map.len() {
                    map[i] = i % worker.peers();
                }
                control_plan.push((rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                control_counter += 1;

                for i in 0 .. map.len() {
                    map[i] = 0;//i % peers;
                    control_plan.push((2 * rounds * 1_000_000_000, Control::new(control_counter,  1, ControlInst::Map(map.clone()))));
                    control_counter += 1;
                }
            }

            let mut just_redistributed = false;
            let mut redistributions = Vec::with_capacity(256);

            while measurements.len() < measurements.capacity() {

                // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                let elapsed = timer.elapsed();
                let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                // If the next planned migration can now be effected, ...
                if control_plan.get(0).map(|&(time, _)| time < elapsed_ns as usize).unwrap_or(false) {
                    if just_redistributed {
                        just_redistributed = false;
                    }
                    else {
                        redistributions.push(elapsed_ns);
                        control_input.send(control_plan.remove(0).1);
                        just_redistributed = true;
                    }
                }

                // Introduce any requests that have "arrived" since last we were here.
                // Request i "arrives" at `index + ns_per_request * (i + 1)`.
                while ((request_counter * ns_per_request) as u64) < elapsed_ns {
                    // input.send(text_gen.generate());
                    input.send(text_gen.word());
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

                // any un-recorded measurements that are complete should be recorded.
                while (((index + peers * (measurements.len() + 1)) * ns_per_request) as u64) < acknowledged_ns && measurements.len() < measurements.capacity() {
                    let requested_at = ((index + peers * (measurements.len() + 1)) * ns_per_request) as u64;
                    measurements.push(elapsed_ns - requested_at);
                    to_print.push((requested_at, elapsed_ns - requested_at))
                }
            }

            measurements.sort();

            let min = measurements[0];
            let med = measurements[measurements.len() / 2];
            let p99 = measurements[99 * measurements.len() / 100];
            let max = measurements[measurements.len() - 1];

            if index == 0 {
                println!("worker {}:\t{}\t{}\t{}\t{}\t(of {} measurements)", index, min, med, p99, max, measurements.len());

                let thing = to_print.len() / 1000;
                for i in 0 .. to_print.len() {
                    if i % thing == 0 {
                        println!("{:?}\t{:?}", to_print[i].0, to_print[i].1);
                    }
                }

                println!();
                for elt in redistributions.iter() {
                    println!("{:?}\t10000000", elt);
                }
            }
        }

    }).unwrap();
}
