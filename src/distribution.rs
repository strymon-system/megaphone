//! General purpose state transition operator.
use std::hash::Hash;
use std::collections::HashMap;
use std::rc::Rc;

use timely::{Data, ExchangeData};
use timely::order::PartialOrder;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::channels::pact::{Exchange, Pipeline};

#[derive(Abomonation, Clone, Debug)]
pub struct ControlInst {
    sequence: u64,
    map: Vec<usize>,
}

impl ControlInst {
    pub fn new(sequence: u64, map: Vec<usize>) -> Self {
        Self { sequence, map }
    }
}

const BIN_SHIFT: usize = 8;

/// Generic state-transition machinery: each key has a state, and receives a sequence of events.
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent.
///
/// `control_state_machine` will buffer inputs if earlier inputs may still arrive. it will directly apply
/// updates for the current time reflected in the notificator, though. In the case of partially
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).

/// Provides the `control_state_machine` method.
pub trait ControlStateMachine<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> {
    /// Tracks a state for each presented key, using user-supplied state transition logic.
    ///
    /// The transition logic `fold` may mutate the state, and produce both output records and
    /// a `bool` indicating that it is appropriate to deregister the state, cleaning up once
    /// the state is no longer helpful.
    ///
    /// #Examples
    /// ```rust,ignore
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
    fn control_state_machine<
        R: Data,                                    // output type
        D: ExchangeData+Default+'static,            // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, ControlInst>) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
}

impl<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> ControlStateMachine<S, K, V> for Stream<S, (K, V)> {
    fn control_state_machine<
        R: Data,                                    // output type
        D: ExchangeData+Default+'static,            // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, ControlInst>) -> Stream<S, R> where S::Timestamp : Hash+Eq {

        let hash = Rc::new(hash);
        let hash2 = Rc::clone(&hash);

        let mut states: Vec<HashMap<K, D>> = vec![HashMap::new(); 1 << BIN_SHIFT];    // bin -> keys -> state

        let stream = self.binary_frontier(control, Pipeline, Pipeline, "StateMachine S", |_cap| {

            let mut notificator = FrontierNotificator::new();

            // Data input stash, time -> Vec<D>
            let mut data_stash = HashMap::new();

            // Control input stash, time -> Vec<ControlInstr>
            let mut pending_control = HashMap::new();

            // Active configurations: Vec<(T, ControlInstr)>
            let mut configurations: Vec<(S::Timestamp, ControlInst)> = Vec::new();

            // Number of bits to use as container number
            let bin_shift = ::std::mem::size_of::<usize>() - BIN_SHIFT;

            // Handle input data
            move |data_in, control_in, output| {

                // Read data from the main data channel
                data_in.for_each(|time, data| {
                    // Test if we're permitted to send out data at `time`
                    if control_in.frontier().less_than(time.time()) {
                        // Control is behind, stash data
                        data_stash.entry(time.clone()).or_insert_with(Vec::new).extend(data.drain(..));
                        notificator.notify_at(time)
                    } else {
                        // Control input is ahead of `time`, pass data
                        let mut session = output.session(&time);
                        // Determine bin: Find the last one that is `less_than`
                        if let Some(ref config_inst) = configurations.iter().rev().find(|&&(ref t, _)| t.less_equal(time.time())) {
                            let map = &config_inst.1.map;
                            session.give_iterator(data.drain(..).map(|d| (map[(hash2(&d.0) >> bin_shift) as usize], d)))
                        }
                    }
                });

                // Read control input
                control_in.for_each(|time, data| {
                    // Stash configuration commands and notify on time
                    pending_control.entry(time.clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    notificator.notify_at(time)
                });

                // Analyze control frontier
                notificator.for_each(&[control_in.frontier()], |time, _not| {
                    // Check for stashed data - now control input has to have advanced
                    if let Some(mut vec) = data_stash.remove(&time) {
                        let mut session = output.session(&time);
                        // Determine bin
                        if let Some(ref config_inst) = configurations.iter().rev().find(|&&(ref t, _)| t.less_equal(time.time())) {
                            let map = &config_inst.1.map;
                            session.give_iterator(vec.drain(..).map(|d| (map[(hash2(&d.0) >> bin_shift) as usize], d)))
                        }
                    }
                    // Check if there are pending control instructions
                    if let Some(mut vec) = pending_control.remove(&time) {
                        // Extend the configurations with (T, ControlInst) tuples
                        configurations.extend(vec.drain(..).map(|d| (time.time().clone(), d)));
                        configurations.sort_by_key(|d| d.1.sequence);
                        // Configurations are well-formed if a bigger sequence number implies that
                        // actions are not reversely ordered.
                        for cs in configurations.windows(2) {
                            debug_assert!(!cs[1].0.less_than(&cs[0].0));
                        }
                    }
                });

                // Analyze data frontier
                notificator.for_each(&[data_in.frontier()], |time, _not| {
                    // Redistribute bins
                    // TODO
                });
            }
        });

        let mut pending = HashMap::new();   // times -> (keys -> state)

        stream.unary_notify(Exchange::new(move |&(bin, _)| bin as u64), "StateMachine", vec![], move |input, output, notificator| {

            // stash each input and request a notification when ready
            input.for_each(|time, data| {
                // stash if not time yet
                if notificator.frontier(0).iter().any(|x| x.less_than(time.time())) {
                    pending.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    notificator.notify_at(time);
                }
                    else {
                        // else we can process immediately
                        let mut session = output.session(&time);
                        for (bin, (key, val)) in data.drain(..) {
                            let (remove, output) = {
                                let state = states[bin].entry(key.clone()).or_insert_with(Default::default);
                                fold(&key, val, state)
                            };
                            if remove { states[bin].remove(&key); }
                            session.give_iterator(output.into_iter());
                        }
                    }
            });

            // go through each time with data, process each (key, val) pair.
            notificator.for_each(|time,_,_| {
                if let Some(pend) = pending.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (bin, (key, val)) in pend {
                        let (remove, output) = {
                            let state = states[bin].entry(key.clone()).or_insert_with(Default::default);
                            fold(&key, val, state)
                        };
                        if remove { states[bin].remove(&key); }
                        session.give_iterator(output.into_iter());
                    }
                }
            });
        })
    }
}
