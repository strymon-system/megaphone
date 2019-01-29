extern crate clap;
extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate streaming_harness;
extern crate streaming_harness_hdrhist;
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
use timely::dataflow::operators::{Broadcast, Concat, Input, Operator, Probe};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::Control;
use dynamic_scaling_mechanism::notificator::{Notify, TotalOrderFrontierNotificator};
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;

use nexmark::tools::ExperimentMapMode;

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
        .arg(Arg::with_name("tick").long("tick").takes_value(true).default_value("1"))
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

    let tick: u64 = (matches.value_of("tick").expect("tick absent").parse::<f64>().expect("couldn't parse tick") * 1_000_000f64) as u64;

    let timely_args = matches.values_of("timely").map_or(Vec::new(), |vs| vs.map(String::from).collect());
    // Read and report RSS every 100ms
    let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(timely_args.into_iter(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let mut init_probe = ProbeHandle::new();

        // Generate the times at which input should be produced
        let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
            1, 1_000_000_000 / rate, duration_ns);

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let element_hdr = Rc::new(RefCell::new(::streaming_harness_hdrhist::HDRHist::new()));
        let element_hdr2 = Rc::clone(&element_hdr);

        // Construct the dataflow
        let mut init_handle = worker.dataflow(|scope: &mut ::timely::dataflow::scopes::Child<_, usize>| {
            let control = control_input.to_stream(scope).broadcast();

            // Construct the data generator
            let input = input
                .to_stream(scope)
                .unary_frontier(Pipeline, "Data generator", |cap, _info| {
                    let mut word_generator = WordGenerator::new_uniform(index, key_space);
                    // Cap is used to track input frontier
                    let mut cap = Some(cap);
                    // Observe count value. Only start producing data once `count` initialized
                    let mut count = None;
                    move |input, output| {
                        // Input closed, we're done
                        if input.frontier().is_empty() {
                            cap.take();
                        } else if let Some(cap) = cap.as_mut() {
                            let time = input.frontier().frontier()[0];
                            if !input.frontier().frontier().less_equal(cap) {
                                cap.downgrade(&time);
                            }
                            // Check if there is some initialization data for `count`
                            if count.is_none() {
                                while let Some(data) = input.next() {
                                    count = Some(*data.0);
                                    break;
                                }
                            } else {
                                // Produce data, `count` is initialized.
                                if let Some(mut it) = input_times_gen.iter_until((time - count.unwrap()) as u64 ) {
                                    if let Some(_) = it.next() {
                                        let mut session = output.session(cap);
                                        let mut count = 1;
                                        session.give((word_generator.word_rand(), 1));
                                        for _t in it {
                                            session.give((word_generator.word_rand(), 1));
                                            count += 1;
                                        }
                                        element_hdr2.borrow_mut().add_value(count);
                                    }
                                }
                            }
                        }
                    }
                });

            // Construct initializing operator
            // The operator has two outputs:
            // 1. the data output
            // 2. a `count` output, which will be advanced to `count` when done
            let (init_handle, init_input) : (_, Stream<_, ()>) = scope.new_input();
            let mut builder = OperatorBuilder::new("Data initializer".to_owned(), init_input.scope());
            let _generator_input = builder.new_input(&init_input, Pipeline);
            let (output, init_input) = builder.new_output();
            let (_count_output_handle, count_output_stream): (_, Stream<_, ()>) = builder.new_output();
            let mut output = Some(output);
            builder.build(move |mut capabilities| {
                let mut count_output_cap = Some(capabilities.pop().unwrap());
                let mut count = Some(capabilities.pop().unwrap());
                count.as_mut().unwrap().downgrade(&1);

                let mut word_generator = WordGenerator::new_uniform(index, key_space);
                let mut word = 0;
                move |frontiers| {
                    let take = if let Some(count) = count.as_mut() {
                        if !frontiers[0].less_than(count) {
                            let mut output = output.as_mut().unwrap().activate();
                            let done = match backend {
                                Backend::Vector => {
                                    let mut session = output.session(count);
                                    let max_number = (key_space >> ::dynamic_scaling_mechanism::BIN_SHIFT).next_power_of_two();
                                    println!("max_number: {}", max_number);
                                    let bin_count = 1 << ::dynamic_scaling_mechanism::BIN_SHIFT;
                                    for bin in index * bin_count / peers..(index + 1) * bin_count / peers {
                                        let number = word_generator.word_at((max_number << ::dynamic_scaling_mechanism::BIN_SHIFT) + bin);
                                        assert!(number < 2 * key_space);
                                        session.give((number, 1));
                                    }
                                    true
                                },
                                _ => {
                                    let mut session = output.session(count);
                                    for i in index * key_space / peers + word..(index + 1) * key_space / peers {
                                        session.give((word_generator.word_at(key_space - i - 1), 1));
                                        word += 1;
                                        if (word & 0xFFF) == 0 {
                                            break;
                                        }
                                    }
                                    word == key_space / peers
                                }
                            };
                            if done {
                                true
                            } else {
                                let new_time = **count + 1;
                                count.downgrade(&new_time);
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if take {
                        // Indicate on count output what our count is
                        count_output_cap.as_mut().unwrap().downgrade(&*count.as_ref().unwrap());
                        // Drop `count` cap because we don't want to produce more output
                        count.take();
                    }
                    // Next time (once the input is closed), we can drop the cap for the count output
                    if frontiers[0].is_empty() {
                        count_output_cap.take();
                    }
                }
            });

            count_output_stream.probe_with(&mut init_probe);
            let input = input.concat(&init_input);

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
            init_handle
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

        let mut count: usize = 0;

        // Wait for initialization, init_probe will jump to `count` once initialization is done
        while init_probe.with_frontier(|f| f[0]) == 0 {
            input.advance_to(count);
            init_handle.advance_to(count);
            if let Some(control_input) = control_input.as_mut() {
                control_input.advance_to(count);
            }
            while probe.less_than(&count) { worker.step(); }
//            probe.with_frontier(|f| println!("probe: {:?}", f.to_vec()));
//            init_probe.with_frontier(|f| println!("init_probe: {:?}", f.to_vec()));
            count += 1;
        }
        ::std::mem::drop(init_handle);

        let count = key_space / 0xFFF;
        input.advance_to(count);
        input.send(());
        if let Some(control_input) = control_input.as_mut() {
            control_input.advance_to(count);
        }
        while probe.less_than(&count) { worker.step(); }
        println!("loading_done\t{}\t{}", index, count);

        let mut input = Some(input);

        let timer = ::std::time::Instant::now();

        let mut last_migrated = None;

        let mut last_ns = 0;

        let mut migration_separation = 0;

        let mut did_migrate = 0;
        let mut migration_time = 0;

        loop {

            if index != 0 {
                // Only measure on worker 0, all others close inputs etc.
                break;
            }

            let elapsed_ns = timer.elapsed().to_nanos();
            let wait_ns = last_ns;
            let target_ns = (elapsed_ns + 1) / tick * tick;
            last_ns = target_ns;

            if index == 0 {
                // was a migration supplied and the time captured? Is the migration definitely over?
                if did_migrate == 1 && migration_time < wait_ns {
                    if did_migrate == 1 {
                        println!("migration_done\t{}\t{}", target_ns, target_ns - migration_time);
                    }
                    did_migrate -= 1;
                }
                if let Some(control_input) = control_input.as_mut() {
                    if last_migrated.map_or(true, |time| *control_input.time() != time) {
                        if instructions.get(0).map(|&(ts, _)| ts as usize + count <= *control_input.time()).unwrap_or(false) {
                            if migration_separation == 0 {
                                let (_ts, ctrl_instructions) = instructions.remove(0);

                                println!("control_time\t{}", control_input.time() - count);

                                let count = ctrl_instructions.len();
                                for instruction in ctrl_instructions {
                                    control_input.send(Control::new(control_sequence, count, instruction));
                                }

                                control_sequence += 1;
                                last_migrated = Some(*control_input.time());
                                migration_separation = 2;
                                // Mark that we supplied migration instructions, will be picked up further down
                                did_migrate = 2;
                            } else {
                                migration_separation -= 1;
                            }
                        }
                    }
                }

                // FIXME: We have to remember control_input's time, and could use a different variable?!
//                if instructions.is_empty() {
//                    control_input.take();
//                }
            }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&(t as usize + count))
                });

            if input.is_none() {
                break;
            }

            if target_ns < duration_ns {
                let input = input.as_mut().unwrap();
                input.advance_to(target_ns as usize + count);
                if let Some(control_input) = control_input.as_mut() {
                    if *control_input.time() < target_ns as usize + count {
                        control_input.advance_to(target_ns as usize + count);
                        // A migration was recorded, note down the time and step state
                        if did_migrate == 2 {
                            migration_time = target_ns;
                            did_migrate -= 1;
                        }
                    }
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                worker.step();
                while probe.less_than(&(wait_ns as usize + count)) { worker.step(); }
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
