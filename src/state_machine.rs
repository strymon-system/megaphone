//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::Data;

use operator::StatefulOperator;
use ::Control;

pub trait BinnedStateMachine<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData, D: ExchangeData + Default + 'static> {
    /// Tracks a state for each presented key, using user-supplied state transition logic.
    ///
    /// The transition logic `fold` may mutate the state, and produce both output records and
    /// a `bool` indicating that it is appropriate to deregister the state, cleaning up once
    /// the state is no longer helpful.
    ///
    /// #Examples
    /// ```
    /// ```
    fn stateful_state_machine<
        R: Data,                                    // output type
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq;
}

impl<S, K, V, D> BinnedStateMachine<S, K, V, D> for Stream<S, (K, V)>
where
    S: Scope,
    K: ExchangeData+Hash+Eq,
    V: ExchangeData+Eq,
    D: ExchangeData + Default + 'static,
{
    fn stateful_state_machine<
        R: Data,                                    // output type
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D) -> (bool, I) + 'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq {

        self.stateful_unary(control, move |(k, _v)| hash(&k), "StateMachine", move |time, data, states, output| {
            let mut session = output.session(&time);
            for (key_id, (key, val)) in data {
                let states: &mut HashMap<_, _> = states.get_state(key_id);
                let (remove, output) = {
                    let state = states.entry(key.clone()).or_insert_with(Default::default);
                    fold(&key, val.clone(), state)
                };
                if remove { states.remove(&key); }
                session.give_iterator(output.into_iter());
            }
        })

//        self.stream.unary_frontier(Pipeline, "StateMachine", |cap, _info| {
//            move |input, output| {
//
//                let frontier = input.frontier();
//                // go through each time with data, process each (key, val) pair.
//                let mut states = states.borrow_mut();
//                let mut notificator = notificator.borrow_mut();
//                while let Some((time, pend)) = notificator.next(&[frontier]) {
//                    let mut session = output.session(&time);
//                    for (key_id, (key, val)) in pend {
//                        let mut states = states.get_state(key_id);
//                        let (remove, output) = {
//                            let state = states.entry(key.clone()).or_insert_with(Default::default);
//                            fold(&key, val.clone(), state)
//                        };
//                        if remove { states.remove(&key); }
//                        session.give_iterator(output.into_iter());
//                    }
//                }
//
//                // stash each input and request a notification when ready
//                while let Some((time, data)) = input.next() {
//                    // stash if not time yet
//                    if frontier.less_equal(time.time()) {
//                        states.notificator().notify_at_data(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)));
//                    } else {
//                        // else we can process immediately
//                        let mut session = output.session(&time);
//                        for (_target, key_id, (key, val)) in data.drain(..) {
//                            let mut states = states.get_state(key_id);
//                            let (remove, output) = {
//                                let state = states.entry(key.clone()).or_insert_with(Default::default);
//                                fold(&key, val.clone(), state)
//                            };
//                            if remove { states.remove(&key); }
//                            session.give_iterator(output.into_iter());
//                        }
//                    }
//                };
//            }
//        }).probe_with(&mut self.probe)
    }
}
