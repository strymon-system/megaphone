//! General purpose state transition operator.
use std::hash::Hash;
// use std::collections::HashMap;
use std::rc::Rc;

use abomonation::Abomonation;
use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope, ProbeHandle};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Probe;
use timely::Data;
use timely::dataflow::operators::generic::Unary;

use stateful::{State, StateHandle};

pub trait BinnedStateMachine<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData, D: Default + 'static> {
    /// Tracks a state for each presented key, using user-supplied state transition logic.
    ///
    /// The transition logic `fold` may mutate the state, and produce both output records and
    /// a `bool` indicating that it is appropriate to deregister the state, cleaning up once
    /// the state is no longer helpful.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::StateMachine;
    ///
    /// timely::example(|scope| {
    ///
    ///     // these results happen to be right, but aren't guaranteed.
    ///     // the system is at liberty to re-order within a timestamp.
    ///     let result = vec![(0,0), (0,2), (0,6), (0,12), (0,20),
    ///                       (1,1), (1,4), (1,9), (1,16), (1,25)];
    ///
    ///         (0..10).to_stream(scope)
    ///                .map(|x| (x % 2, x))
    ///                .state_machine(
    ///                    |_key, val, agg| { *agg += val; (false, Some((*_key, *agg))) },
    ///                    |key| *key as u64
    ///                )
    ///                .inspect(move |x| assert!(result.contains(x)));
    /// });
    /// ```
    fn state_machine<
        R: Data,                                    // output type
//        D: Default+'static,                         // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&mut self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
}

impl<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData, D: Abomonation + Default + 'static> BinnedStateMachine<S, K, V, D> for (Stream<S, (K, V)>, State<(K, D), HashMap<K, D>>, ProbeHandle<S::Timestamp>) {
    fn state_machine<
        R: Data,                                    // output type
//        D: Default + 'static,                         // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D) -> (bool, I) + 'static,    // state update logic
        H: Fn(&K) -> u64 + 'static,                     // "hash" function for keys
    >(&mut self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp: Hash + Eq {
        let mut pending: HashMap<_, _> = Default::default();   // times -> (keys -> state)

        let mut bin_states1 = self.1.clone();
        let mut bin_states2 = self.1.clone();

        let fold = Rc::new(fold);
        let fold2 = Rc::clone(&fold);

        self.0.unary_notify(Pipeline, "StateMachine", vec![], move |input, output, notificator| {

            let fold = fold.clone();
            let fold2 = fold2.clone();
            // stash each input and request a notification when ready
            input.for_each(|time, data| {
                // stash if not time yet
                if notificator.frontier(0).less_than(time.time()) {
                    pending.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    notificator.notify_at(time.retain());
                }
                    else {
                        // else we can process immediately
                        let mut session = output.session(&time);
                        for (key, val) in data.drain(..) {
                            let output = {
                                bin_states1.with_state(hash(&key) as usize, |states_bin| {
                                    if states_bin.is_none() {
                                        *states_bin = Some(Default::default());
                                    }
                                    let mut states = states_bin.take().unwrap();
                                    let (remove, output) = {
                                        let state = states.entry(key.clone()).or_insert_with(Default::default);
                                        fold(&key, val.clone(), state)};
                                    if remove { states.remove(&key); }
                                    *states_bin = Some(states);
                                    output
                                })
                            };
                            session.give_iterator(output.into_iter());
                        }
                    }
            });

            // go through each time with data, process each (key, val) pair.
            notificator.for_each(|time,_,_| {
                if let Some(pend) = pending.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (key, val) in pend {
                        let output = {
                            bin_states2.with_state(hash(&key) as usize, |states_bin| {
                                if states_bin.is_none() {
                                    *states_bin = Some(Default::default());
                                }
                                let mut states = states_bin.take().unwrap();
                                let (remove, output) = {
                                    let state = states.entry(key.clone()).or_insert_with(Default::default);
                                    fold2(&key, val.clone(), state)};
                                if remove { states.remove(&key); }
                                *states_bin = Some(states);
                                output
                            })
                        };
                        session.give_iterator(output.into_iter());
                    }
                }
            });
        }).probe_with(&mut self.2)
    }
}
