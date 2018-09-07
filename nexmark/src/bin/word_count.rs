extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate streaming_harness;
extern crate dynamic_scaling_mechanism;
extern crate abomonation;

use std::hash::Hash;
use std::hash::Hasher;

use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Probe};

use timely::dataflow::operators::Operator;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::{Accumulate, Broadcast, Inspect};
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::{ControlInst, Control};
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
    pub fn word_rand(&mut self) -> String {
        let index = match *self {
            WordGenerator::Uniform(ref mut rng, ref keys) => rng.gen_range(0, *keys),
        };
        self.word_at(index)
    }

    #[inline(always)]
    pub fn word_at(&mut self, k: usize) -> String {
        format!("word {}", k)
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

fn main() {

    // Read and report RSS every 100ms
    let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(std::env::args(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = control_input.to_stream(scope).broadcast();

            let input = input.to_stream(scope);
//            input
//                .count()
//                .inspect_time(move |time, data| println!("[{} {:?}] A count: {}", index, time.inner, data))
//                .probe_with(&mut probe);

            input
                .map(|x: String| (x, 1))
                .stateful_state_machine(|key: &_, val, agg: &mut u64| {
                    *agg += val;
                    (false, Some((key.clone(), *agg)))
                }, |key| calculate_hash(key), &control)
//                .count()
//                .inspect_time(move |time, data| println!("[{} {:?}] B count: {}", index, time.inner, data))
                .probe_with(&mut probe);
        });

        let rate: u64 = std::env::args().nth(1).expect("rate absent").parse::<u64>().expect("couldn't parse rate");

        let duration_ns: u64 = std::env::args().nth(2).expect("duration absent").parse::<u64>().expect("couldn't parse duration") * 1_000_000_000;

        let map_mode: ExperimentMapMode = std::env::args().nth(3).expect("migration file absent").parse().unwrap();

        let key_space: usize = std::env::args().nth(4).expect("key_space absent").parse::<usize>().expect("couldn't parse key_space");

        let mut instructions = map_mode.instructions(peers, duration_ns).unwrap();

        if index == 0 {
            println!("bin_shift\t{}", ::dynamic_scaling_mechanism::BIN_SHIFT);

            for instruction in instructions.iter().take(10) {
                // Format instructions first to be able to truncate the string representation
                eprintln!("instructions\t{:.120}", format!("{:?}", instruction));
            }
        }

        let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
            1, 1_000_000_000 / rate, duration_ns);

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 2_000_000_000, duration_ns - 2_000_000_000, duration_ns,
                250_000_000);

        let mut word_generator = WordGenerator::new_uniform(index, key_space);

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let mut input = Some(input);

        let mut control_sequence = 0;
        let mut control_input = Some(control_input);
        if index != 0 {
            control_input.take().unwrap().close();
        } else {
            control_input.as_mut().unwrap().advance_to(1);
        }
        worker.step();

        let input_count = 1;
        for i in index * key_space .. (index + 1) * key_space {
            input.as_mut().unwrap().send(word_generator.word_at(i));
            if (i & 0xFFF) == 0 {
                worker.step();
            }
        }

        if index == 0 {
            if let Some((visible, _, _)) = instructions.get(0) {
                control_input.as_mut().unwrap().advance_to(*visible as usize);
            }
        }

        input.as_mut().unwrap().advance_to(1);
        while probe.less_than(&RootTimestamp::new(1)) { worker.step(); }
        eprintln!("Loading done");

        let timer = ::std::time::Instant::now();

        let mut last_migrated = None;

        let mut last_ns = 0;

        loop {
            let elapsed_ns = timer.elapsed().to_nanos();
            let wait_ns = last_ns;
            let target_ns = (elapsed_ns + 1) / 1_000_000 * 1_000_000 + 2;
            last_ns = target_ns;

            if index == 0 {

                if last_migrated.map_or(true, |time| control_input.as_ref().map_or(false, |t| t.time().inner != time))
                    && instructions.get(0).map(|&(visible, _ts, _)| visible < target_ns).unwrap_or(false)
                    {
                        let (_visible, ts, ctrl_instructions) = instructions.remove(0);
                        let count = ctrl_instructions.len();

                        let control_input = control_input.as_mut().unwrap();
                        control_input.advance_to(ts as usize);

                        for instruction in ctrl_instructions {
                            control_input.send(Control::new(control_sequence, count, instruction));
                        }
                        control_sequence += 1;
                        last_migrated = Some(control_input.time().inner);
                        if let Some((visible, _, _)) = instructions.get(0) {
                            if control_input.time().inner < *visible as usize {
                                control_input.advance_to(*visible as usize);
                            }
                        }
                    }

                if instructions.is_empty() {
                    control_input.take();
                }
            }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&RootTimestamp::new(t as usize))
                });

            if input.is_none() {
                break;
            }

            if let Some(it) = input_times_gen.iter_until(target_ns) {
                let mut input = input.as_mut().unwrap();
                for _t in it {
                    input.send(word_generator.word_rand());
                }
                input.advance_to(target_ns as usize);
                if let Some(control_input) = control_input.as_mut() {
                    if control_input.time().inner < target_ns as usize {
                        control_input.advance_to(target_ns as usize);
                    }
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                while probe.less_than(&RootTimestamp::new(wait_ns as usize)) { worker.step(); }
            } else {
                while worker.step() { }
            }
        }

        output_metric_collector.into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst);

    let ::streaming_harness::timeline::Timeline { timeline, latency_metrics, .. } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();
    println!("DEBUG_summary\t{}", latency_metrics.summary_string().replace("\n", "\nDEBUG_summary\t"));
    println!("{}",
              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
                    format!("DEBUG_timeline\t-- {} ({} samples) --\nDEBUG_timeline\t{}", time, samples, metrics.summary_string().replace("\n", "\nDEBUG_timeline\t"))).collect::<Vec<_>>().join("\n"));

    for (value, prob, count) in latency_metrics.ccdf() {
        println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    }
    println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
}
