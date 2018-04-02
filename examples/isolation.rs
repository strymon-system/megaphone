extern crate fnv;
extern crate rand;

extern crate timely;
extern crate dynamic_scaling_mechanism;
use std::hash::{Hash, Hasher};
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Input, Map, Probe};
use timely::dataflow::operators::aggregation::StateMachine;

use dynamic_scaling_mechanism::{BIN_SHIFT, ControlInst, Control};
use dynamic_scaling_mechanism::distribution::ControlStateMachine;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
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
}

#[derive(Debug, PartialEq, Eq)]
enum Backend {
    Native,
    Scaling,
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

    let backend = match args.next().unwrap().as_str() {
        "native" => Backend::Native,
        "scaling" => Backend::Scaling,
        _ => panic!("invalid backend"),
    };

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

            let fold = |_key: &_, val, agg: &mut u64| {
                *agg += val;
                (false, Some(*agg))
            };

            let input = input
                .map(|x| (x, 1));
            let output = match backend {
                Backend::Native => input
                    .state_machine( fold, |key| calculate_hash(key)),
                Backend::Scaling => input
                    .control_state_machine(
                        fold,
                        |key| calculate_hash(key),
                        &control
                    ),
            };
            output.probe_with(&mut probe);
        });

        if index == 0 {
            let map1 = (0.. (1 << BIN_SHIFT)).map(|x| x % peers).collect();
            let map2 = vec![0; 1 << BIN_SHIFT];
            control_input.send(Control::new(0, 1, ControlInst::Map(map1)));
            control_input.advance_to(1_000_000_000 * rounds);
            control_input.send(Control::new(1, 1, ControlInst::Map(map2)));
        }
        control_input.close();

        let requests_per_sec = batch;
        let ns_per_request = 1_000_000_000 / requests_per_sec;

        // introduce data and watch!
        for i in 0 .. keys {
            if i % peers == index {
                input.send(text_gen.word_at(i));
            }
        }
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }

        let timer = ::std::time::Instant::now();
        let mut elapsed = timer.elapsed();

        let mut request_counter = 1;
        let mut ack_counter = 1;
        let mut counts = vec![0usize; 64];

        let mut redistributed = false;
        let mut previous_sec = elapsed.as_secs();

        let mut latencies: Vec<(usize, usize, usize)> = Vec::with_capacity(8<<20);

        while (elapsed.as_secs() as usize) < 2 * rounds {

            // worker zero keeps working; others are lazy.
            if index != 0 {
                ::std::thread::sleep(::std::time::Duration::from_millis(10));
            }

            // at the moment of redistribution, have other workers swear off input.
            if !redistributed && (elapsed.as_secs() as usize) >= rounds {
                redistributed = true;
                if index != 0 { input.advance_to(2_000_000_000 * rounds); }
            }

            if index == 0 && (previous_sec != elapsed.as_secs()) {
                previous_sec = elapsed.as_secs();
                println!("latencies:");
                for index in 1 .. counts.len() {
                    if counts[index] > 0 {
                        println!("\tcount[{}]:\t{}", index, counts[index]);
                    }
                }
                counts = vec![0; 64];
            }

            let elapsed_ns = (elapsed.as_secs() as usize) * 1_000_000_000 + (elapsed.subsec_nanos() as usize);
            let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier[0].inner);

            while ((ack_counter * ns_per_request)) < acknowledged_ns {
                let requested_at = ack_counter * ns_per_request;
                let count_index = (elapsed_ns - requested_at).next_power_of_two().trailing_zeros() as usize;
                counts[count_index] += 1;
                ack_counter += 1;
            }

            let inserted_ns = input.time().inner;

            let target_ns =
                if acknowledged_ns >= inserted_ns {
                    elapsed_ns
                }
                else {
                    inserted_ns
                };

            if inserted_ns < target_ns {
                while (request_counter * ns_per_request) < target_ns {
                    input.send(text_gen.word_rand(keys));
                    request_counter += 1;
                }
                assert!(target_ns >= input.time().inner);
                input.advance_to(target_ns);
                if index == 0 {
                    latencies.push((acknowledged_ns, elapsed_ns - acknowledged_ns, request_counter - ack_counter));
                }
            }

            worker.step();
            elapsed = timer.elapsed();
        }

        if index == 0 {
            println!("latencies:");
            for index in 1 .. counts.len() {
                if counts[index] > 0 {
                    println!("\tcount[{}]:\t{}", index, counts[index]);
                }
            }
        }

        let mut sum_latency = 0;

        for (ts, latency, cnt) in latencies {
            sum_latency += latency;

            if sum_latency > 1_000_000 {
                let phase = if ((ts/1_000_000_000) as usize) < rounds {
                    'A'
                } else {
                    'C'
                };

                println!("00\tlatency\t{:?}\t{:?}\t{:?}\t{:?}", ts, latency, cnt, phase);
                sum_latency = 0;
            }

        }

    }).unwrap();
}
