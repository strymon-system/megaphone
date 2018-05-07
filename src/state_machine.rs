//! General purpose state transition operator.
use std::hash::Hash;
use std::rc::Rc;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Probe;
use timely::Data;
use timely::dataflow::operators::Operator;

use stateful::{StateHandle, StateStream};

pub trait BinnedStateMachine<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData, D: ExchangeData + Default + 'static> {
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
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
    >(&mut self, fold: F) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
}

impl<S, K, V, D> BinnedStateMachine<S, K, V, D> for StateStream<S, (K, V), HashMap<K, D>, (K, D)>
where
    S: Scope,
    K: ExchangeData+Hash+Eq,
    V: ExchangeData,
    D: ExchangeData + Default + 'static,
{
    fn state_machine<
        R: Data,                                    // output type
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D) -> (bool, I) + 'static,    // state update logic
    >(&mut self, fold: F) -> Stream<S, R> where S::Timestamp: Hash + Eq {
        let mut pending: HashMap<_, _> = Default::default();   // times -> (keys -> state)

        let states = self.state.clone();

        let fold = Rc::new(fold);
        let fold2 = Rc::clone(&fold);

        self.stream.unary_frontier(Pipeline, "StateMachine", |_cap, _info| {
            move |input, output| {
                let frontier = input.frontier();
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    // stash if not time yet
                    if frontier.less_than(time.time()) {
                        pending.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                        let mut states = states.borrow_mut();
                        states.notificator().notify_at(time.retain());
                    } else {
                        // else we can process immediately
                        let mut session = output.session(&time);
                        let mut states = states.borrow_mut();
                        for (_target, bin, (key, val)) in data.drain(..) {
                            let output = {
                                states.with_state(bin, |states| {
                                    let (remove, output) = {
                                        let state = states.entry(key.clone()).or_insert_with(Default::default);
                                        fold(&key, val.clone(), state)
                                    };
                                    if remove { states.remove(&key); }
                                    output
                                })
                            };
                            session.give_iterator(output.into_iter());
                        }
                    }
                };

            // go through each time with data, process each (key, val) pair.
            states.borrow_mut().notificator().for_each(&[frontier], |time, _not| {
                if let Some(pend) = pending.remove(time.time()) {
                    let mut session = output.session(&time);
                    let mut states = states.borrow_mut();
                    for (_target, bin, (key, val)) in pend {
                        let output = {
                            states.with_state(bin, |states| {
                                let (remove, output) = {
                                    let state = states.entry(key.clone()).or_insert_with(Default::default);
                                    fold2(&key, val.clone(), state)};
                                if remove { states.remove(&key); }
                                output
                            })
                        };
                        session.give_iterator(output.into_iter());
                    }
                }
            });
            }
        }).probe_with(&mut self.probe)
    }
}
