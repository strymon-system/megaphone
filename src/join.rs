//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::Operator;

use stateful::{StateHandle, StateStream};

trait BinarySkeleton<S, K, V>
    where
        S: Scope, // The containing scope
        K: ExchangeData+Hash+Eq,
        V: ExchangeData, // Input data
{
    fn left_join<V2>(&mut self, other: &mut StateStream<S, (K, V2), HashMap<K, Vec<V2>>, (K, Vec<V2>)>, name: &str) -> Stream<S, (K, V, V2)>
        where
            V2: ExchangeData,
            K: ExchangeData+Hash+Eq,
;
}

impl<S, K, V> BinarySkeleton<S, K, V> for StateStream<S, (K, V), HashMap<K, V>, (K, V)>
where
    S: Scope, // The containing scope
    K: ExchangeData+Hash+Eq,
    V: ExchangeData, // Input data
{
    fn left_join<V2>(&mut self, other: &mut StateStream<S, (K, V2), HashMap<K, Vec<V2>>, (K, Vec<V2>)>, name: &str) -> Stream<S, (K, V, V2)>
        where
            V2: ExchangeData,
    {
        let state1 = self.state.clone();
        let state2 = other.state.clone();
        self.stream.binary_frontier(&other.stream, Pipeline, Pipeline, name, |_cap, _info| {

            let mut pending1: HashMap<_, Vec<(_, _, _)>> = Default::default();
            let mut pending2: HashMap<_, Vec<(_, _, _)>> = Default::default();

            move |input1, input2, output| {

                let mut state1 = state1.borrow_mut();
                let mut state2 = state2.borrow_mut();


                input1.for_each(|time, data| {
                    pending1.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    state1.notificator().notify_at(time.retain());
                });

                input2.for_each(|time, data| {
                    pending2.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    state2.notificator().notify_at(time.retain());
                });

                while let Some(time) = state1.notificator().next(&[input1.frontier(), input2.frontier()]) {
                    let mut session = output.session(&time);
                    for (_target, key_id, (key, value)) in pending1.remove(&time.time()).into_iter().flat_map(|v| v.into_iter()) {
                        if let Some(mut d2) = state2.get_state(key_id).remove(&key) {
                            session.give_iterator(d2.drain(..).map(|d| (key.clone(), value.clone(), d)));
                        }
                        state1.get_state(key_id).insert(key, value);
                    };
                };

                while let Some(time) = state2.notificator().next(&[input1.frontier(), input2.frontier()]) {
                    let mut session = output.session(&time);
                    for (_target, key_id, (key, value)) in pending2.remove(&time.time()).into_iter().flat_map(|v| v.into_iter()) {
                        if let Some(d1) = state1.get_state(key_id).get(&key) {
                            session.give((key.clone(), d1.clone(), value.clone()));
                        } else {
                            state2.get_state(key_id).entry(key).or_insert_with(Vec::new).push(value);
                        }
                    };
                };
            }
        }).probe_with(&mut self.probe).probe_with(&mut other.probe)
    }
}
