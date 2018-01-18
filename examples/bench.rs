extern crate timely;
extern crate dynamic_scaling_mechanism;

use std::time::Instant;

use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Input, Probe, Inspect};
// use timely::dataflow::operators::aggregation::StateMachine;
// use timely::progress::timestamp::RootTimestamp;

use dynamic_scaling_mechanism::distribution::{BIN_SHIFT, ControlInst, Control, ControlStateMachine};

// fn factor(mut n: u64) -> u64 {
//     let mut factor = 2;
//     let mut max_factor = 0;
//     while n > 1 {
//         if n % factor == 0 {
//             max_factor = factor;
//             n /= factor;
//         } else {
//             factor += 1;
//         }
//     }
//     max_factor
// }

fn main() {
    timely::execute_from_args(std::env::args().skip(4), |worker| {

        // How many rounds at each key distribution strategy.
        let rounds: usize = std::env::args().nth(1).unwrap().parse().unwrap();
        // How many updates to perform in each round.
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        // No clue.
        let behind: usize = std::env::args().nth(3).unwrap().parse().unwrap();
        // Number of distinct keys.
        let keys: usize = std::env::args().nth(4).unwrap().parse().unwrap();

        let index = worker.index();
        let peers = worker.peers();

        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = scope.input_from(&mut control_input).broadcast();
            let input = scope.input_from(&mut input);

            input
                .control_state_machine(
                    |_key: &_, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some(*agg))
                    },
                    |key| *key as u64
                    ,
                    &control
                )
                // .inspect_batch(move |t,xs| {
                //     println!("{:?}:\tcontrol_state_machine[{:?}]: {:?}", t, index, xs.len());
                // })
                .probe_with(&mut probe);
        });

        if index == 0 {

            let open_loop = true;

            let mut control_counter = 0;
            let mut map = vec![0; 1 << BIN_SHIFT];

            // Start with an initial distribution of data to worker zero.
            control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
            control_counter += 1;

            // introduce data and watch!
            input.advance_to(1);
            for i in 0 .. keys {
                input.send((i | (i << 56), 1));
            }
            input.advance_to(2);
            control_input.advance_to(2);
            while probe.less_than(input.time()) {
                worker.step();
            }
            println!("data loaded");

            if !open_loop {

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

                control_input.close();

                let mut counter = 0;
                let mut durations = Vec::with_capacity(3 * rounds);
                for _round in 0 .. 3 * rounds {
                    let start = Instant::now();
                    for i in counter .. (counter + batch) {
                        input.send((i | (i << 56), 1));
                    }
                    counter += batch;
                    counter = counter % keys;
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

                let mut just_redistributed = false;
                let mut redistributions = Vec::with_capacity(256);

                while measurements.len() < measurements.capacity() {

                    // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                    let elapsed = timer.elapsed();
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

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
                        input.send((request_counter | (request_counter << 56), 1));
                        request_counter += peers;
                    }
                    input.advance_to(elapsed_ns as usize);
                    // println!("{:?} >? {:?}", elapsed_ns, control_input.time());
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
