extern crate clap;
extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate streaming_harness;
extern crate hdrhist;
extern crate dynamic_scaling_mechanism;
extern crate abomonation;

use std::alloc::System;

#[global_allocator]
static GLOBAL: System = System;

use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use clap::{Arg, App};

use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Broadcast, Operator, Probe};

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::Control;
use dynamic_scaling_mechanism::notificator::{Notify, TotalOrderFrontierNotificator};
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;

use nexmark::tools::ExperimentMapMode;
use timely::dataflow::operators::input::Handle;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

enum WordGenerator {
    Uniform(SmallRng, usize),
}

impl WordGenerator {

    fn new_uniform(index: usize, keys: usize) -> Self {
        let seed: [u8; 16] = [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, index as u8];
        WordGenerator::Uniform(SeedableRng::from_seed(seed), keys)
    }

    #[inline(always)]
    pub fn word_rand(&mut self) -> usize {
        let index = match *self {
            WordGenerator::Uniform(ref mut rng, ref keys) => rng.gen_range(0, *keys),
        };
        self.word_at(index)
    }

    #[inline(always)]
    pub fn word_at(&mut self, k: usize) -> usize {
        k
    }
}

#[allow(dead_code)]
fn verify<S: Scope, T: ExchangeData+Ord+::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    use timely::dataflow::channels::pact::Exchange;
    use std::collections::HashMap;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
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

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug, Hash)]
enum Backend {
    HashMap,
    HashMapNative,
    Vector,
    VectorNative,
}

fn main() {

    let matches = App::new("word_count")
        .arg(Arg::with_name("rate").long("rate").takes_value(true).required(true))
        .arg(Arg::with_name("duration").long("duration").takes_value(true).required(true))
        .arg(Arg::with_name("migration").long("migration").takes_value(true).required(true))
        .arg(Arg::with_name("domain").long("domain").takes_value(true).required(true))
        .arg(Arg::with_name("validate").long("validate"))
        .arg(Arg::with_name("timely").multiple(true))
        .arg(Arg::with_name("backend").long("backend").takes_value(true).possible_values(&["hashmap", "hashmapnative", "vec", "vecnative"]).default_value("hashmap"))
        .get_matches();

    let rate: u64 = matches.value_of("rate").expect("rate absent").parse::<u64>().expect("couldn't parse rate");

    let duration_ns: u64 = matches.value_of("duration").expect("duration absent").parse::<u64>().expect("couldn't parse duration") * 1_000_000_000;

    let map_mode: ExperimentMapMode = matches.value_of("migration").expect("migration file absent").parse().unwrap();

    let key_space: usize = matches.value_of("domain").expect("key_space absent").parse::<usize>().expect("couldn't parse key_space");

    let validate: bool = matches.is_present("validate");

    let backend: Backend = match matches.value_of("backend").expect("backend missing") {
        "hashmap" => Backend::HashMap,
        "hashmapnative" => Backend::HashMapNative,
        "vec" => Backend::Vector,
        "vecnative" => Backend::VectorNative,
        _ => panic!("Unknown backend"),
    };
    println!("backend\t{:?}", backend);

    let timely_args = matches.values_of("timely").map_or(Vec::new(), |vs| vs.map(String::from).collect());
    // Read and report RSS every 100ms
    let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(timely_args.into_iter(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // Declare re-used input, control and probe handles.
        let mut input: Handle<_, ()> = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let probe2 = ProbeHandle::clone(&mut probe);

        // Generate the times at which input should be produced
        let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
            1, 1_000_000_000 / rate, duration_ns);

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let element_hdr = Rc::new(RefCell::new(::hdrhist::HDRHist::new()));
        let element_hdr2 = Rc::clone(&element_hdr);

        // Calculate how many initialization steps are required to populate all data
        // The 'staggering' shift parameter, Will stagger at 0xFFF words
        let stagger = 24;
        // Calculate the number of initializtion rounds. Vector completes in one round.
        let count = 1 + match backend {
            Backend::Vector => 0,
            _ => (key_space / peers) >> stagger,
        };

        // Construct the dataflow
        worker.dataflow(|scope: &mut ::timely::dataflow::scopes::Child<_, usize>| {
            let control = control_input.to_stream(scope).broadcast();

            // Construct the data generator
            let input = input
                .to_stream(scope)
                .unary_frontier(Pipeline, "Data generator", |cap, _info| {
                    let mut word_generator = WordGenerator::new_uniform(index, key_space);
                    // Cap is used to track input frontier
                    let mut cap = Some(cap);
                    // word count, used during initialization
                    let mut word = 0;
                    let mut last_production_time = 0;
                    move |input, output| {
                        // Input closed, we're done
                        if input.frontier().is_empty() {
                            cap.take();
                        } else if let Some(cap) = cap.as_mut() {
                            let current_time = input.frontier().frontier()[0];
                            // Are we initializing?
                            if current_time < count {
                                // Yes, select based on backend
                                match backend {
                                    Backend::Vector => {
                                        let mut session = output.session(cap);
                                        let max_number = (key_space >> ::dynamic_scaling_mechanism::BIN_SHIFT).next_power_of_two();
                                        println!("max_number: {}", max_number);
                                        let bin_count = 1 << ::dynamic_scaling_mechanism::BIN_SHIFT;
                                        for bin in index * bin_count / peers..(index + 1) * bin_count / peers {
                                            let number = word_generator.word_at((max_number << ::dynamic_scaling_mechanism::BIN_SHIFT) + bin);
                                            assert!(number < 2 * key_space);
                                            session.give((number, 1));
                                        }
                                    },
                                    _ => {
                                        let mut session = output.session(cap);
                                        for i in index * key_space / peers + word..(index + 1) * key_space / peers {
                                            session.give((word_generator.word_at(key_space - i - 1), 1));
                                            word += 1;
                                            if (word & ((1 << stagger) - 1)) == 0 {
                                                break;
                                            }
                                        }
                                    }
                                };
                                cap.downgrade(&current_time);
                            } else {
                                // Produce data, benchmark running
                                let probe_time = probe2.with_frontier(|f| f[0]);
                                let delta_probe = current_time - probe_time;
                                let delta_production = current_time - last_production_time;
                                // if delta to probe is smaller than half of delta to production, consider to produce more data
                                if delta_probe <= delta_production * 2 {
                                    if let Some(mut it) = input_times_gen.iter_until((current_time - count) as u64) {
                                        // `it` is some => we are still running!
                                        // If there are actual elements to be produced, open a session and produce them
                                        if let Some(_) = it.next() {
                                            let mut session = output.session(cap);
                                            session.give((word_generator.word_rand(), 1));
                                            let mut word_count = 1;
                                            for _t in it {
                                                session.give((word_generator.word_rand(), 1));
                                                word_count += 1;
                                            }
                                            element_hdr2.borrow_mut().add_value(word_count);
                                            last_production_time = current_time;
                                        }
                                    }
                                    cap.downgrade(&current_time);
                                }
                            }
                        }
                    }
                });

            let sst_output = match backend {
                Backend::HashMap => {
                    Some(input
                        .stateful_state_machine(|key: &_, val, agg: &mut u64| {
                            *agg += val;
                            (false, Some((*key, *agg)))
                        }, |key| calculate_hash(key), &control)
                        .probe_with(&mut probe))
                },
                Backend::HashMapNative => {
                    Some(input
                         .unary_frontier(Exchange::new(move |(x, _)| *x as u64),
                                         "WordCount", |_cap, _| {
                             let mut drain_buffer = Vec::new();
                             let mut states = ::std::collections::HashMap::<usize, u64>::new();
                             let mut notificator = TotalOrderFrontierNotificator::new();
                             move |input, output| {
                                 while let Some((time, data)) = input.next() {
                                     let cap = time.retain();
                                     for d in data.iter() {
                                         notificator.notify_at_data(&cap, cap.time().clone(), *d);
                                     }
                                 }
                                 if let Some(cap) = notificator.drain(&[input.frontier], &mut drain_buffer) {
                                     let mut session_cap = cap.clone();
                                     for (time, (key, val)) in drain_buffer.drain(..) {
                                         if *session_cap.time() != time {
                                             session_cap = cap.delayed(&time);
                                         }
                                         let mut session = output.session(&session_cap);
                                         let entry = states.entry(key).or_insert(0);
                                         *entry += val;
                                         session.give((key, *entry));
                                     }
                                 }
                             }
                         })
                         .probe_with(&mut probe))
                }
                _ => None,
            };
            use dynamic_scaling_mechanism::operator::StatefulOperator;
            let vec_output = match backend {
                Backend::Vector => {
                    Some(input
                        .stateful_unary(&control, move |(k, _v)| (*k as u64) << (64 - ::dynamic_scaling_mechanism::BIN_SHIFT), "StateMachine", move |cap, data, bin, output| {
                            let states: &mut Vec<u64> = bin.state();
                            let mut session_cap = cap.clone();
                            for (time, (key, val)) in data.drain(..) {
                                if *session_cap.time() != time {
                                    session_cap = cap.delayed(&time);
                                }
                                let mut session = output.session(&session_cap);
                                let states_len = states.len();
                                let position = key >> ::dynamic_scaling_mechanism::BIN_SHIFT;
                                if states.len() <= position {
                                    states.extend(::std::iter::repeat(0).take(position - states_len + 1))
                                }
                                states[position] += val;
                                session.give((key, states[position]));
                            }
                        })
                        .probe_with(&mut probe))
                },
                Backend::VectorNative => {
                    Some(input
                         .unary_frontier(Exchange::new(move |(x, _)| *x as u64),
                                         "WordCount", |_cap, _| {
                             let mut states = Vec::<u64>::new();
                             let mut drain_buffer = Vec::new();
                             let mut notificator = TotalOrderFrontierNotificator::new();
                             move |input, output| {
                                 while let Some((time, data)) = input.next() {
                                     let cap = time.retain();
                                     for d in data.iter() {
                                         notificator.notify_at_data(&cap, cap.time().clone(), *d);
                                     }
                                 }
                                 if let Some(cap) = notificator.drain(&[input.frontier], &mut drain_buffer) {
                                     let mut session_cap = cap.clone();
                                     for (time, (key, val)) in drain_buffer.drain(..) {
                                         if *session_cap.time() != time {
                                             session_cap = cap.delayed(&time);
                                         }
                                         let mut session = output.session(&session_cap);
                                         let states_len = states.len();
                                         let position = key / peers;
                                         if states.len() <= position {
                                             states.extend(::std::iter::repeat(0).take(position - states_len + 1))
                                         }
                                         states[position] += val;
                                         session.give((key, states[position]));
                                     }
                                 }
                             }
                         })
                         .probe_with(&mut probe))
                },
                _ => None,
            };

            if validate {
                use timely::dataflow::operators::aggregation::StateMachine;
                let correct = input
                    .state_machine(|_key: &_, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((*_key, *agg)))
                    }, |key| calculate_hash(key));
                if let Some(sst_output) = sst_output {
                    verify(&sst_output, &correct).probe_with(&mut probe);
                }
                if let Some(vec_output) = vec_output {
                    verify(&vec_output, &correct).probe_with(&mut probe);
                }
            }
        });

        let mut instructions = map_mode.instructions(peers, duration_ns).unwrap();

        if index == 0 {
            println!("bin_shift\t{}", ::dynamic_scaling_mechanism::BIN_SHIFT);

            for instruction in instructions.iter().take(10) {
                // Format instructions first to be able to truncate the string representation
                eprintln!("instructions\t{:.120}", format!("{:?}", instruction));
            }
        }

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 2_000_000_000, duration_ns - 2_000_000_000, duration_ns,
                250_000_000);

        let mut control_sequence = 0;
        let mut control_input = Some(control_input);
        if index != 0 {
            control_input.take().unwrap().close();
        } else {
            let control_input = control_input.as_mut().unwrap();
            if instructions.get(0).map_or(false, |(ts, _)| *ts == 0) {
                let (_ts, ctrl_instructions) = instructions.remove(0);
                let count = ctrl_instructions.len();

                for instruction in ctrl_instructions {
                    control_input.send(Control::new(control_sequence, count, instruction));
                }
                control_sequence += 1;
            }
        }

        // Wait for initialization in `count` rounds
        for i in 1..=count {
            input.advance_to(i);
            if let Some(control_input) = control_input.as_mut() {
                control_input.advance_to(count);
            }
            while probe.less_than(&i) { worker.step(); }
        }

        input.advance_to(count);
        if let Some(control_input) = control_input.as_mut() {
            control_input.advance_to(count);
        }
        while probe.less_than(&count) { worker.step(); }
        println!("loading_done\t{}\t{}", index, count);

        let mut input = Some(input);

        let timer = ::std::time::Instant::now();

        let mut last_migrated = None;

        let mut did_migrate = false;

        loop {

            if index != 0 {
                // Only measure on worker 0, all others close inputs etc.
                break;
            }

            let elapsed_ns = timer.elapsed().to_nanos();

            if index == 0 {
                // was a migration supplied and the time captured? Is the migration definitely over?
                if let Some(last_migrated) = last_migrated {
                    if did_migrate && probe.with_frontier(|f| !f.less_than(&(last_migrated as usize + count))) {
                        println!("migration_done\t{}\t{}", elapsed_ns, elapsed_ns - last_migrated);
                        did_migrate = false;
                    }
                }
                if let Some(control_input) = control_input.as_mut() {
                    if last_migrated.map_or(true, |time| probe.with_frontier(|f| !f.less_equal(&(time as usize + count)))) {
                        if instructions.get(0).map(|&(ts, _)| ts as usize + count <= *control_input.time()).unwrap_or(false) {
                            let (_ts, ctrl_instructions) = instructions.remove(0);

                            println!("control_time\t{}", control_input.time() - count);

                            let count = ctrl_instructions.len();
                            for instruction in ctrl_instructions {
                                control_input.send(Control::new(control_sequence, count, instruction));
                            }

                            control_sequence += 1;
                            last_migrated = Some(elapsed_ns);
                            // Mark that we supplied migration instructions, will be picked up further down
                            did_migrate = true;
                        }
                    }
                }
            }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&(t as usize + count))
                });

            if input.is_none() {
                break;
            }

            if elapsed_ns < duration_ns {
                let input = input.as_mut().unwrap();
                input.advance_to(elapsed_ns as usize + count);
                if let Some(control_input) = control_input.as_mut() {
                    if *control_input.time() < elapsed_ns as usize + count {
                        control_input.advance_to(elapsed_ns as usize + count);
                    }
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                worker.step();
//                while probe.less_than(&(wait_ns as usize + count)) { worker.step(); }
            } else {
                while worker.step() { }
            }
        }

        let element_hdr = element_hdr.borrow();
        for (value, prob, count) in element_hdr.ccdf() {
            println!("count_ccdf\t{}\t{}\t{}", value, prob, count);
        }
        output_metric_collector.into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst);

    let ::streaming_harness::timeline::Timeline { timeline, latency_metrics, .. } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();
//    println!("DEBUG_summary\t{}", latency_metrics.summary_string().replace("\n", "\nDEBUG_summary\t"));
//    println!("{}",
//              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
//                    format!("DEBUG_timeline\t-- {} ({} samples) --\nDEBUG_timeline\t{}", time, samples, metrics.summary_string().replace("\n", "\nDEBUG_timeline\t"))).collect::<Vec<_>>().join("\n"));

    for (value, prob, count) in latency_metrics.ccdf() {
        println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    }
    println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
}
