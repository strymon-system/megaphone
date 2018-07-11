//! General purpose state transition operator.
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::Probe;
use timely::Data;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::operators::generic::OutputHandle;

use ::{Control, Key};
use stateful::{Stateful, State, StateHandle};

pub trait StatefulOperator<S: Scope, V: ExchangeData+Hash+Eq> {
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&V)->u64+'static,
        D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<S::Timestamp>, Vec<(Key, V)>, &mut State<S::Timestamp, D, V>, &mut OutputHandle<S::Timestamp, D2, Tee<S::Timestamp, D2>>) + 'static,    // state update logic
    >(&mut self, name: &str, key: B, fold: F, control: &Stream<S, Control>) -> Stream<S, D2> where S::Timestamp: Hash + Eq
    ;

}

impl<S, V> StatefulOperator<S, V> for Stream<S, V>
    where
        S: Scope, // The containing scope
        V: ExchangeData+Hash+Eq, // Input data
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&V)->u64+'static,
        D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<S::Timestamp>, Vec<(Key, V)>, &mut State<S::Timestamp, D, V>, &mut OutputHandle<S::Timestamp, D2, Tee<S::Timestamp, D2>>) + 'static,    // state update logic
    >(&mut self, name: &str, key: B, fold: F, control: &Stream<S, Control>) -> Stream<S, D2> where S::Timestamp: Hash + Eq
    {
        let mut stateful = self.stateful(move |v| key(&v), control);
        let states = stateful.state.clone();

        stateful.stream.unary_frontier(Pipeline, name, |_cap, _info| {
            move |input, output| {
                let mut states = states.borrow_mut();
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    states.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                };

                // go through each time with data
                while let Some((time, data)) = states.notificator().next(&[input.frontier()]) {
                    fold(time, data, &mut states, output);
                }
            }
        }).probe_with(&mut stateful.probe)
    }
}
