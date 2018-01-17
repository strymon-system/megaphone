extern crate timely;
extern crate dynamic_scaling_mechanism;

use std::time::Instant;

use timely::dataflow::*;
use timely::dataflow::operators::{Broadcast, Input, Probe, Inspect, Filter};
use timely::dataflow::operators::aggregation::StateMachine;
use timely::progress::timestamp::RootTimestamp;

use dynamic_scaling_mechanism::distribution::{BIN_SHIFT, ControlInst, Control, ControlStateMachine};

fn factor(mut n: u64) -> u64 {
    let mut factor = 2;
    let mut max_factor = 0;
    while n > 1 {
        if n % factor == 0 {
            max_factor = factor;
            n /= factor;
        } else {
            factor += 1;
        }
    }
    max_factor
}

fn main() {
    timely::execute_from_args(std::env::args().skip(4), |worker| {

        let rounds: usize = std::env::args().nth(1).unwrap().parse().unwrap();
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        let behind: usize = std::env::args().nth(3).unwrap().parse().unwrap();


        let index = worker.index();
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = scope.input_from(&mut control_input).broadcast();
            let input = scope.input_from(&mut input);
            input
                .filter(|_| false)
                .state_machine(
                    |_key, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((*_key, factor(*agg))))
                    },
                    |key| *key as u64
                )
                .filter(|_| false)
                .inspect(move |x| {
                    println!("[{:?}] state_machine x: {:?}", index, x);
                })
                .probe_with(&mut probe);

            input
                .control_state_machine(
                    |_key, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((*_key, factor(*agg))))
                    },
                    |key| *key as u64
                    ,
                    &control
                )
                .filter(|_| false)
                .inspect(move |x| {
                    println!("[{:?}] control_state_machine x: {:?}", index, x);
                })
                .probe_with(&mut probe);
        });

        if index == 0 {
            control_input.send(Control::new(0,  1, ControlInst::Map(vec![0; 1 << BIN_SHIFT])));

            control_input.advance_to(rounds + behind);
            let mut map = vec![0; 1 << BIN_SHIFT];
            let span = (1 << BIN_SHIFT) / worker.peers();
            for p in 0..worker.peers() {
                for i in 0..span {
                    map[p*span + i] = p;
                }
            }
            println!("{:?}", map);
            control_input.send(Control::new(1,  1, ControlInst::Map(map)));
            control_input.advance_to(rounds * 2 + behind);
            // introduce data and watch!

            for round in behind..(rounds*2 + behind) {
                for i in 0..batch {
                    input.send((i | (i << 56), i as u64));
                }
                input.advance_to(round + 1);
                let start = Instant::now();
                while probe.less_than(&RootTimestamp::new(input.time().inner - behind)) {
                    worker.step();
                }
                let duration = start.elapsed();
                println!("[{:?}] {:?}", round, duration)
            }
        }

    }).unwrap();
}
