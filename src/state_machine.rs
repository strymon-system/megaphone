//! General purpose state transition operator, implemented with Megaphone.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::Data;

use operator::StatefulOperator;
use ::Control;

/// Provide a general-purpose state machine operator that can be migrated without changes to the
/// `fold` implementation.
pub trait BinnedStateMachine<S, K, V, D>
where
    S: Scope,
    S::Timestamp: ::timely::order::TotalOrder,
    K: ExchangeData+Hash+Eq,
    V: ExchangeData+Eq,
    D: ExchangeData + Default + 'static,
{
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
    S::Timestamp: ::timely::order::TotalOrder,
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

        self.stateful_unary(control, move |(k, _v)| hash(&k), "StateMachine", move |cap, iter, bin, output| {
            let mut session = output.session(&cap);
            let states: &mut HashMap<_, _> = bin.state();
            for (_time, (key, val)) in iter.drain(..) {
                let (remove, output) = {
                    if !states.contains_key(&key) {
                        states.insert(key.clone(), Default::default());
                    }
                    let state = states.get_mut(&key).unwrap();
                    fold(&key, val.clone(), state)
                };
                if remove { states.remove(&key); }
                session.give_iterator(output.into_iter());
            }
        })
    }
}
