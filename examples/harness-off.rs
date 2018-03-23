extern crate fnv;
extern crate rand;
extern crate timely;

use std::collections::HashMap;
use std::hash::Hash;

use std::sync::{Arc, Mutex};

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::{Input, Map, Probe, Operator};
use timely::dataflow::operators::FrontierNotificator;

use timely::dataflow::channels::pact::Exchange;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    use std::hash::Hasher;
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

#[derive(Copy, Clone, Debug)]
enum Contestants {
    Andrea,
    Frank,
    Moritz,
    Baseline,
}

fn main() {

    let mut args = std::env::args();
    let _cmd = args.next();

    // How many rounds.
    let secs: usize = args.next().expect("must specify number of seconds").parse().unwrap();
    // How many updates to perform in each round.
    let tps: usize = args.next().expect("must specify records per second").parse().unwrap();
    // Number of distinct keys.
    let keys: usize = args.next().expect("must specify number of keys").parse().unwrap();

    let contestant = match args.next().expect("must specify contestant").as_str() {
        "Andrea" => Contestants::Andrea,
        "Frank"  => Contestants::Frank,
        "Moritz" => Contestants::Moritz,
        _ => Contestants::Baseline,
    };

    let global_counts = Arc::new(Mutex::new(vec![0; 64]));
    let global_counts2 = global_counts.clone();

    println!("parameters: secs: {}, tps: {}, keys: {}, contestant: {:?}", secs, tps, keys, contestant);

    timely::execute_from_args(args, move |worker| {

        let global = global_counts.clone();

        let mut text_gen = SentenceGenerator::new(worker.index());

        let index = worker.index();
        let peers = worker.peers();

        let mut input: InputHandle<usize, String> = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let input = scope.input_from(&mut input);

            let inputs = input
                .map(|x: String| (x, 1u64));

            let outputs = inputs
                    .unary_frontier::<(String, u64), _, _, _>(Exchange::new(|k| calculate_hash(k)),
                                    "word_count",
                                    |_cap| {
                        let mut notificator = FrontierNotificator::new();
                        let mut counts = HashMap::new();
                        let mut stash: HashMap<Capability<_>, Vec<Vec<(String, u64)>>> = HashMap::new();

                        move |input, output| {
                            input.for_each(|time, data| {
                                let time = time.clone();
                                stash.entry(time.clone()).or_insert_with(Vec::new).push(data.replace_with(Vec::new()));
                                notificator.notify_at(time);
                            });

                            notificator.for_each(&[input.frontier()], |time, _| {
                                let mut affected = HashMap::with_capacity(2048);
                                let mut data = stash.remove(&time).unwrap();
                                for d in data.drain(..) {
                                    for (k, c) in d.into_iter() {
                                        let mut new_count = c;
                                        if counts.contains_key(&k) {
                                            let v = counts.get_mut(&k).unwrap();
                                            new_count = *v + c;
                                            *v = new_count;
                                        } else {
                                            counts.insert(k.clone(), c);
                                        }
                                        affected.insert(k, new_count);
                                    }
                                }
                                output.session(&time).give_iterator(affected.into_iter());
                            });
                        }
                    });
            outputs.probe_with(&mut probe);
        });

        for i in 0 .. keys / peers {
            input.send(text_gen.word_at(i * peers + index));
        }
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
        input.advance_to(2);
        while probe.less_than(input.time()) {
            worker.step();
        }
        let timer = ::std::time::Instant::now();

        let ns_per_request = 1_000_000_000 / tps;
        let mut request_counter = peers + index;
        let mut ack_counter = peers + index;
        let mut counts = vec![0usize; 64];

        // tracks the ns through which input has been inserted.
        let mut inserted_ns: usize = 2;

        let mut elapsed = timer.elapsed();

        while (elapsed.as_secs() as usize) < secs {

            let elapsed_ns = (elapsed.as_secs() as usize) * 1_000_000_000 + (elapsed.subsec_nanos() as usize);

            // Determine completed ns.
            let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier[0].inner);

            while ((ack_counter * ns_per_request)) < acknowledged_ns {
                let requested_at = ack_counter * ns_per_request;
                let count_index = (elapsed_ns - requested_at).next_power_of_two().trailing_zeros() as usize;
                counts[count_index] += 1;
                ack_counter += peers;
            }

            let target_ns: usize = match contestant {
                Contestants::Andrea => { unimplemented!() },
                Contestants::Frank => { unimplemented!() },
                Contestants::Moritz => { unimplemented!() },
                Contestants::Baseline => {
                    if acknowledged_ns >= inserted_ns {
                        elapsed_ns
                    }
                    else {
                        inserted_ns
                    }
                },
            };

            if inserted_ns < target_ns {

                while (request_counter * ns_per_request) < target_ns {
                    input.send(text_gen.word_rand(keys));
                    request_counter += peers;
                }
                input.advance_to(target_ns);
                inserted_ns = target_ns;
            }

            worker.step();
            elapsed = timer.elapsed();
        }

        // Determine completed ns.
        let mut acknowledged_ns: usize = probe.with_frontier(|frontier| frontier[0].inner);
        while acknowledged_ns < inserted_ns {
            acknowledged_ns = probe.with_frontier(|frontier| frontier[0].inner);
            worker.step();
        }

        let guard = global.lock();
        if let Ok(mut lock) = guard {
            for index in 0 .. counts.len() {
                lock[index] += counts[index];
            }
        }

    }).expect("failed to exit cleanly");

    let guard = global_counts2.lock();
    if let Ok(counts) = guard {
        println!("latencies:");
        for index in 0 .. counts.len() {
            if counts[index] > 0 {
                println!("{:?}\tcount[{}]:\t{}", contestant, index, counts[index]);
            }
        }
    }

}