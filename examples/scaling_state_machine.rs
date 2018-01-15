extern crate timely;
extern crate dynamic_scaling_mechanism;


use timely::dataflow::operators::{ToStream, Map, Inspect};
use timely::dataflow::operators::aggregation::StateMachine;
use dynamic_scaling_mechanism::distribution::{ControlInst, ControlStateMachine};

fn main() {
    timely::example(|scope| {

        // these results happen to be right, but aren't guaranteed.
        // the system is at liberty to re-order within a timestamp.
        let result = vec![(0, 0), (0, 2), (0, 6), (0, 12), (0, 20),
                          (1, 1), (1, 4), (1, 9), (1, 16), (1, 25)];
        let result2 = result.clone();

        let control = vec![ControlInst::new(0, vec![0; 256])].to_stream(scope);

        (0..10).to_stream(scope)
            .map(|x| (x % 2, x))
            .state_machine(
                |_key, val, agg| {
                    *agg += val;
                    (false, Some((*_key, *agg)))
                },
                |key| *key as u64
            )
            .inspect(move |x| {assert!(result.contains(x)); println!("state_machine x: {:?}", x)});

        (0..10).to_stream(scope)
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
            .inspect(move |x| {assert!(result2.contains(x)); println!("control_state_machine x: {:?}", x)});
    });
}
