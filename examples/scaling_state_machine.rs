extern crate timely;
extern crate dynamic_scaling_mechanism;


use timely::dataflow::*;
use timely::dataflow::operators::{Input, Probe, Map, Inspect};
use timely::dataflow::operators::aggregation::StateMachine;

use dynamic_scaling_mechanism::distribution::{ControlInst, ControlStateMachine};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // these results happen to be right, but aren't guaranteed.
        // the system is at liberty to re-order within a timestamp.
        let result = vec![(0, 0), (0, 2), (0, 6), (0, 12), (0, 20),
                          (1, 1), (1, 4), (1, 9), (1, 16), (1, 25)];
        let result2 = result.clone();

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            let control = scope.input_from(&mut control_input);
            let input = scope.input_from(&mut input);
            input
                .map(|x| (x % 2, x))
                .state_machine(
                    |_key, val, agg| {
                        *agg += val;
                        (false, Some((*_key, *agg)))
                    },
                    |key| *key as u64
                )
                .inspect(move |x| {
                    assert!(result.contains(x));
                    println!("state_machine x: {:?}", x)
                })
                .probe_with(&mut probe);

            input
                .map(|x| (x % 2, x))
                .control_state_machine(
                    |_key, val, agg| {
                        *agg += val;
                        (false, Some((*_key, *agg)))
                    },
                    |key| *key as u64
                    ,
                    &control
                )
                .inspect(move |x| {
                    assert!(result2.contains(x));
                    println!("control_state_machine x: {:?}", x)
                })
                .probe_with(&mut probe);
        });

        control_input.send(ControlInst::new(0, vec![0; 256]));
        control_input.advance_to(5);
        control_input.send(ControlInst::new(1, vec![0; 256]));
        control_input.advance_to(10);
        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }

    }).unwrap();
}
