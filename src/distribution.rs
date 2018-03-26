//! General purpose state transition operator.
use std::hash::Hash;
use std::cell::RefCell;
// use std::collections::HashMap;
use std::rc::Rc;

use fnv::FnvHashMap as HashMap;

use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope, ProbeHandle};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{FrontierNotificator, Probe};
use timely::dataflow::operators::generic::binary::Binary;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

use ::{BIN_SHIFT, Bin, Control, ControlSetBuilder, ControlSet};

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
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq ;

    fn control_timed_state_machine<
        R: Data,                                    // output type
        D: ExchangeData+Default+'static,            // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&S::Timestamp, &K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
}

impl<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> ControlStateMachine<S, K, V> for Stream<S, (K, V)> {
    fn control_state_machine<
        R: Data,                                    // output type
        D: ExchangeData+Default+'static,            // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq {
        self.control_timed_state_machine(
            move |_time, key, val, agg| fold(key, val, agg),
            hash,
            control
        )
    }

    fn control_timed_state_machine<
        R: Data,                                    // output type
        D: ExchangeData+Default+'static,            // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&S::Timestamp, &K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H, control: &Stream<S, Control>) -> Stream<S, R> where S::Timestamp : Hash+Eq {

        let hash = Rc::new(hash);
        let hash2 = Rc::clone(&hash);

        let index = self.scope().index();
        let peers = self.scope().peers();

        // bin -> keys -> state
        let states: Rc<RefCell<Vec<HashMap<K, D, >>>> = Rc::new(RefCell::new(vec![Default::default(); 1 << BIN_SHIFT]));
        let states_f = Rc::clone(&states);

        let mut builder = OperatorBuilder::new("StateMachine F".into(), self.scope());

        let mut data_in = builder.new_input(self, Pipeline);
        let mut control_in = builder.new_input(control, Pipeline);
        let (mut data_out, stream) = builder.new_output();
        let (mut state_out, state) = builder.new_output();

        // Number of bits to shift a hash to determine bin index
        let bin_shift = ::std::mem::size_of::<usize>() * 8 - BIN_SHIFT;

        let mut probe1 = ProbeHandle::new();
        let probe2 = probe1.clone();

        builder.build(move |_capability| {

            let mut data_notificator = FrontierNotificator::new();
            let mut control_notificator = FrontierNotificator::new();

            // Data input stash, time -> Vec<D>
            let mut data_stash: HashMap<_,_> = Default::default();

            // Control input stash, time -> Vec<ControlInstr>
            let mut pending_control: HashMap<_,_> = Default::default();

            // Active configurations: Vec<(T, ControlInstr)>
            let mut pending_configurations: Vec<ControlSet<S::Timestamp>> = Vec::new();

            // TODO : default configuration may be poorly chosen.
            let mut active_configuration: ControlSet<S::Timestamp> = ControlSet { 
                sequence: 0, 
                frontier: Antichain::from_elem(Default::default()),
                map: vec![0; 1 << BIN_SHIFT],
            };

            // Handle input data
            move |frontiers| {
                let mut data_out = data_out.activate();
                let mut state_out = state_out.activate();

                // Read control input
                control_in.for_each(|time, data| {
                    pending_control.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    let cap = time.retain();
                    control_notificator.notify_at(cap.clone());
                    data_notificator.notify_at(cap);
                });

                // Analyze control frontier
                control_notificator.for_each(&[&frontiers[1]], |time, _not| {
                    // Check if there are pending control instructions
                    if let Some(mut vec) = pending_control.remove(time.time()) {
                        // Extend the configurations with (T, ControlInst) tuples
                        let mut builder: ControlSetBuilder<S::Timestamp> = Default::default();
                        for update in vec.drain(..) {
                            builder.apply(update);
                        }
                        // TODO: We don't know the frontier at the time the command was received.
                        builder.frontier(vec![time.time().clone()].into_iter());
                        let config = builder.build(pending_configurations.last().unwrap_or(&active_configuration));
                        pending_configurations.push(config);
                        pending_configurations.sort_by_key(|d| d.sequence);

                        // Configurations are well-formed if a bigger sequence number implies that
                        // actions are not reversely ordered. Each configuration has to dominate its
                        // successors.
                        for cs in pending_configurations.windows(2) {
                            debug_assert!(cs[0].frontier.dominates(&cs[1].frontier));
                        }
                        // Assert that the currently active configuration dominates the first pending
                        if let Some(config) = pending_configurations.first() {
                            debug_assert!(active_configuration.frontier.dominates(&config.frontier));
                        }
                    }
                });

                // Read data from the main data channel
                data_in.for_each(|time, data| {
                    if frontiers[1].less_equal(time.time()) {
                        if !data_stash.contains_key(time.time()) {
                            data_stash.insert(time.time().clone(), Vec::new());
                        }
                        data_stash.get_mut(time.time()).unwrap().push(data.replace_with(Vec::new()));
                    } else {
                        let map =
                            pending_configurations
                                .iter()
                                .rev()
                                .find(|&c| c.frontier.less_equal(time.time()))
                                .unwrap_or(&active_configuration)
                                .map();

                        let mut session = data_out.session(&time);
                        let data_iter = data.drain(..).into_iter().map(|d| (map[(hash2(&d.0) >> bin_shift) as usize], d));
                        session.give_iterator(data_iter);
                    }
                    data_notificator.notify_at(time.retain());
                });

                data_notificator.for_each(&[&frontiers[0], &frontiers[1]], |time, _not| {
                    // Check for stashed data - now control input has to have advanced
                    if let Some(vec) = data_stash.remove(time.time()) {

                        let map = 
                        pending_configurations
                            .iter()
                            .rev()
                            .find(|&c| c.frontier.less_equal(time.time()))
                            .unwrap_or(&active_configuration)
                            .map();

                        let mut session = data_out.session(&time);
                        for data in vec {
                            let data_iter = data.into_iter().map(|d| (map[(hash2(&d.0) >> bin_shift) as usize], d));
                            session.give_iterator(data_iter);
                        }
                    }

                    // Did we cross a frontier?
                    // Here we can't really express frontier equality yet ):
                    // What we really want is to know if we can apply a configuration change or not.
                    // let data_frontier_f = data_frontier_f.borrow();
                    // if let Some(ref config) = configurations.iter().rev().find(|&c| c.frontier.dominates(&data_frontier_f)) {

                    // TODO : Perhaps we keep an active config and a queue of pending configs, because the *only*
                    // transition that can happen is to install the config with the next sequence number. That is
                    // the only test to perform, rather than scanning all pending configs.

                    // If the next configuration to install is no longer at all ahead of the state machine output,
                    // then there can be no more records or state updates for any configuration prior to the next.
                    if pending_configurations.get(0).is_some() && pending_configurations[0].frontier.elements().iter().all(|t| !probe2.less_than(t)) {

                        // We should now install `pending_configurations[0]` into `active_configuration`!
                        let to_install = pending_configurations.remove(0);

                        {   // Scoped to let `old_map` and `new_map` borrows drop.
                            let old_map = active_configuration.map();
                            let new_map = to_install.map();

                            let mut states = states_f.borrow_mut();
                            let mut session = state_out.session(&time);
                            for (bin, (old, new)) in old_map.iter().zip(new_map.iter()).enumerate() {
                                // Migration is needed if a bin is to be moved (`old != new`) and the state
                                // actually contains data. Also, we must be the current owner of the bin.
                                if (*old % peers == index) && (old != new) {
                                    // Capture bin's values as a `Vec` of (key, state) pairs
                                    session.give_iterator(states[bin].drain().map(|d| (*new, Bin(bin), d)));
                                    // Release the local state memory
                                    states[bin].shrink_to_fit();
                                }
                            }
                        }

                        // Promote the pending config to active
                        active_configuration = to_install;
                    }
                });
            }
        });

        let mut pending: HashMap<_,_> = Default::default();   // times -> Vec<Vec<(keys -> state)>>
        let mut pending_states: HashMap<_,_> = Default::default();

        stream.binary_notify(&state, Exchange::new(move |&(target, _)| target as u64), Exchange::new(move |&(target, _, _)| target as u64), "StateMachine", vec![], move |input, state, output, notificator| {

            // stash each input and request a notification when ready
            input.for_each(|time, data| {
                if notificator.frontier(0).iter().any(|x| x.less_equal(time.time()))
                    || notificator.frontier(1).iter().any(|x| x.less_equal(time.time())) {
                    if !pending.contains_key(time.time()) {
                        pending.insert(time.time().clone(), Vec::new());
                    }
                    pending.get_mut(time.time()).unwrap().push(data.replace_with(Vec::new()));
                    notificator.notify_at(time.retain());
                } else {
                    let mut session = output.session(&time);
                    let mut states = states.borrow_mut();

                    for (_target, (key, val)) in data.drain(..) {
                        let bin = (hash(&key) >> bin_shift) as usize;
                        let (remove, output) = {
                            if !states[bin].contains_key(&key) {
                                states[bin].insert(key.clone(), Default::default());
                            }
                            let state =  states[bin].get_mut(&key).unwrap();
                            fold(time.time(), &key, val, state)
                        };
                        if remove { states[bin].remove(&key); }
                        session.give_iterator(output.into_iter());
                    }
                }
            });

            state.for_each(|time, data| {
                if !pending_states.contains_key(time.time()) {
                    pending_states.insert(time.time().clone(), Vec::new());
                }
                pending_states.get_mut(time.time()).unwrap().push(data.replace_with(Vec::new()));
                notificator.notify_at(time.retain());
            });

            // go through each time with data, process each (key, val) pair.
            notificator.for_each(|time,_,_| {
                if let Some(state_updates) = pending_states.remove(time.time()) {
                    let mut states = states.borrow_mut();

                    for state_update in state_updates {
                        for (_target, bin, (key, state)) in state_update {
                            states[*bin].insert(key, state);
                        }
                    }
                }

                if let Some(pend) = pending.remove(time.time()) {
                    // let sum = states.borrow().iter().map(|x| x.len()).sum::<usize>();
                    // println!("at {:?}, current sum: {:?}; about to add: {:?}", time.time(), sum, pend.len());

                    let mut session = output.session(&time);
                    let mut states = states.borrow_mut();
                    for chunk in pend {
                        for (_, (key, val)) in chunk {
                            let bin = (hash(&key) >> bin_shift) as usize;
                            let (remove, output) = {
                                if !states[bin].contains_key(&key) {
                                    states[bin].insert(key.clone(), Default::default());
                                }
                                let state =  states[bin].get_mut(&key).unwrap();
                                fold(time.time(), &key, val, state)
                            };
                            if remove { states[bin].remove(&key); }
                            session.give_iterator(output.into_iter());
                        }
                    }
                }
            });
        })
        .probe_with(&mut probe1)
    }
}
