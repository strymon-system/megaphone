//! General purpose state transition operator.
//!
//! The module provides the [`Stateful`] trait and [`StateStream`], a wrapper for stateful streams.
//!
//! [`Stateful`]: trait.Stateful.html
//! [`StateStream`]: struct.StateStream.html
//!
use std::hash::Hash;
use std::cell::RefCell;
use std::rc::Rc;

use std::marker::PhantomData;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Feedback;
use timely::dataflow::operators::feedback::Handle as FeedbackHandle;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::frontier::Antichain;

use ::{BIN_SHIFT, Bin, BinId, Control, ControlSetBuilder, ControlSet, Key, key_to_bin, State};
use num::One;
use abomonation::Abomonation;
use ControlInst;
use std::collections::HashSet;

const BUFFER_CAP: usize = 16;

pub type Notificator<T, D> = ::notificator::TotalOrderFrontierNotificator<T, D>;

/// Generic state-transition machinery: each key has a state, and receives a sequence of events.
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent.
///
/// `control_state_machine` will buffer inputs if earlier inputs may still arrive. it will directly apply
/// updates for the current time reflected in the notificator, though. In the case of partially
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).

/// Datatype to multiplex state and timestamps on the state update channel.
#[derive(Abomonation, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum StateProtocol<T, S, D> {
    /// Provide a piece of state for a bin
    State(BinId, Vec<S>),
    /// Announce an outstanding time stamp
    Pending(BinId, T, D),
    /// Prepare for receiving state
    Prepare(BinId),
}

/// A timely `Stream` with an additional state handle and a probe.
pub struct StateStream<S, V, D, W, M> where
    S: Scope, // The containing scope
    S::Timestamp: TotalOrder,
    V: ExchangeData, // Input data
    D: IntoIterator<Item=W>+Extend<W>+Default+'static,    // per-bin state (data)
    W: ExchangeData,                            // State format on the wire
    M: ExchangeData,
{
    /// The wrapped stream. The stream provides tuples of the form `(usize, Key, V)`. The first two
    /// parameters are the target worker and the key identifier. Implementations are encouraged to
    /// ignore the target worker. The key identifier has to be used to obtain the associated state
    /// from [`StateHandle`].
    ///
    /// [`StateHandle`]: trait.StateHandle.html
    pub stream: Stream<S, (usize, Key, V)>,
    /// Stream of state updates
    pub state_stream: Stream<S, (usize, StateProtocol<S::Timestamp, W, M>)>,
    /// A handle to the shared state object
    pub state: Rc<RefCell<State<S::Timestamp, D, M>>>,
    /// The probe `stateful` uses to determine completion.
    pub feedback: FeedbackHandle<S, ()>,
    _phantom: PhantomData<(*const W)>,
}

impl<S, V, D, W, M> StateStream<S, V, D, W, M>
    where
        S: Scope, // The containing scope
        S::Timestamp: TotalOrder,
        V: ExchangeData, // Input data
        D: IntoIterator<Item=W>+Extend<W>+Default,    // per-key state (data)
        W: ExchangeData,
        M: ExchangeData,
{
    pub fn new(stream: Stream<S, (usize, Key, V)>, state_stream: Stream<S, (usize, StateProtocol<S::Timestamp, W, M>)>, state: Rc<RefCell<State<S::Timestamp, D, M>>>, feedback: FeedbackHandle<S, ()>) -> Self {
        StateStream {
            stream,
            state_stream,
            state,
            feedback,
            _phantom: PhantomData,
        }
    }

}

pub fn apply_state_updates<
    T: Timestamp+TotalOrder, // The containing scope
    D: IntoIterator<Item=W>+Extend<W>+Default,    // per-key state (data)
    W: ExchangeData,
    M: ExchangeData,
    I: Iterator<Item=(usize, StateProtocol<T, W, M>)>>(states: &mut State<T, D, M>, cap: &Capability<T>, data: I) {

    // Apply each state update
    for (_target, state) in data {
        match state {
            StateProtocol::Prepare(bin) => {
                assert!(states.bins[*bin].is_none());
                states.bins[*bin] = Some(Default::default());
            }
            // Extend state
            StateProtocol::State(bin, s) => {
                if let Some(bin) = states.bins[*bin].as_mut() {
                    bin.data.extend(s.into_iter());
                }
            },
            // Request notification
            StateProtocol::Pending(bin, t, data) =>
                states.bins[*bin].as_mut().unwrap().notificator().notify_at_data(cap, t, data),
        }
    }

}

/// Provides the `stateful` method.
pub trait Stateful<S: Scope, V: ExchangeData> {

    /// Provide management and migration logic to stateful operators.
    ///
    /// `stateful` takes a regular data input, a key extractor and a control stream. The control
    /// stream provides key-to-worker assignments. `stateful` applies the configuration changes such
    /// that a correctly-written downstream operator will still function correctly.
    ///
    /// # Parameters
    /// * `W`: State serialization format
    /// * `D`: Data associated with keys
    /// * `B`: Key function
    fn stateful<W, D, B, M>(&self, key: B, control: &Stream<S, Control>) -> StateStream<S, V, D, W, M>
        where
            S::Timestamp: Hash+Eq+TotalOrder+Abomonation,
            <<S as timely::dataflow::ScopeParent>::Timestamp as Timestamp>::Summary: One,
            // State format on the wire
            W: ExchangeData,
            // per-key state (data)
            D: IntoIterator<Item=W>+Extend<W>+Default,
            // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData,
    ;
}

#[cfg(not(feature = "fake_stateful"))]
impl<S: Scope, V: ExchangeData> Stateful<S, V> for Stream<S, V> {

    fn stateful<W, D, B, M>(&self, key: B, control: &Stream<S, Control>) -> StateStream<S, V, D, W, M>
        where
            S::Timestamp: Hash+Eq+TotalOrder+Abomonation,
            <<S as timely::dataflow::ScopeParent>::Timestamp as Timestamp>::Summary: One,
            // State format on the wire
            W: ExchangeData,
            // per-key state (data)
            D: IntoIterator<Item=W>+Extend<W>+Default,
            // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData,
    {
        let index = self.scope().index();
        let peers_rc = self.scope().peers_rc(); // this will change after a rescaling operation
        let mut routing_state_init = !self.scope().is_rescaling(); // a new worker joining the cluster should initialize its routing state

        // TODO(lorenzo): If joining the cluster this is not correct! Migration might have already occurred, we must recv the actual map
        // Options:
        //   1) recv the map as part of the bootstrap protocol
        //        - annoying to implement
        //        - need to make timely aware of "extra state" that needs to be transferred
        //   2) recv the map as a usual rescaling operation
        //        - we must not process any `data_in` until the configuration has been: would result in panic or wrong results
        //
        //        2.1) external controller send the `Map` instruction with the up-to-date map
        //               - the map is copy of the current state, so nothing will change for the others workers
        //               - the controller need to know how many bins, the initial configuration, and apply every change that happened => kinda bad
        //
        //        2.2) external controller send a new instruction `Bootstrap(bootstrap_server_index, new_worker_index)`
        //               - the bootstrap server will send its current map to the new worker => need to use an extra "communication channel"
        //
        let map: Vec<usize> = (0..self.scope().init_peers()).cycle().take(1 << BIN_SHIFT).collect();
        // worker-local state, maps bins to state
        let default_elements: Vec<Option<_>> = map.iter().map(|i| if *i == index {
            Some(Default::default())
        } else {
            None
        }).collect();
        let states: Rc<RefCell<State<S::Timestamp, D, M>>> = Rc::new(RefCell::new(State::new(default_elements)));
        let states_f = Rc::clone(&states);

        let mut builder = OperatorBuilder::new("StateMachine F".into(), self.scope());

        // The data input
        let mut data_in = builder.new_input(self, Pipeline);
        // The control input
        let mut control_in = builder.new_input(control, Pipeline);
        // Data output of the F operator
        let (mut data_out, stream) = builder.new_output();
        // State output of the F operator
        let (mut state_out, state) = builder.new_output_connection(vec![Antichain::new(), Antichain::from_elem(Default::default())]);

        // TODO(lorenzo):
        //    - new worker waits for routing_state_in  for the map to implant instead of the arbitrarily initialized one.
        //    - new worker does not process any input until it has initialized the map
        //    - Bootstrap(bootstrap_server, new_worker) is received in `control_in` => treat it as a special command

        #[derive(Abomonation, Clone)]
        struct RoutingState<T: Timestamp+Hash+Eq+TotalOrder+Abomonation> {
            active_configuration: ControlSet<T>,
            // We cannot transfer capabilities. Thus, we require that during a rescaling operation
            // there are no pending configurations for which the associated control notification has been delivered already
            // pending_configurations: Vec<(Capability<T>, ControlSet<T>)>,
            pending_configuration_data: Vec<(T, ControlSetBuilder<T>)>,
        }

        // Feedback connection to initialize new worker's routing state
        let (mut routing_state_out, routing_state) = builder.new_output_connection::<(usize, RoutingState<S::Timestamp>)>(vec![Antichain::new(), Antichain::from_elem(Default::default())]);
        let one_summary = <<S as timely::dataflow::ScopeParent>::Timestamp as Timestamp>::Summary::one();
        let routing_state_connection = vec![Antichain::new(), Antichain::new(), Antichain::from_elem(one_summary)]; // TODO(lorenzo) try with 1?
        let to_new_worker = Exchange::new(|(new_worker, _map)| *new_worker as u64);
        let mut routing_state_in = builder.new_input_connection(&routing_state, to_new_worker, routing_state_connection);

        let (feedback_handle, feedback_stream) = self.scope().feedback(Default::default());
        let feedback_in_connection = vec![Antichain::new(); 3];
        let _feedback_in = builder.new_input_connection(&feedback_stream, Pipeline, feedback_in_connection);

        // Construct F operator
        builder.build(move |_capability| {

            // distinct notificators for data and control input
            let mut data_notificator = Notificator::new();
            let mut control_notificator = Notificator::new();

            // Data input stash, time -> Vec<Vec<V>>
            let mut data_stash: HashMap<_, Vec<Vec<V>>> = Default::default();

            // Active configurations: Vec<(T, ControlInstr)> sorted by increasing T. Note that
            // we assume the Ts form a total order, i.e. they must dominate each other.
            let mut pending_configurations: Vec<(Capability<S::Timestamp>, ControlSet<S::Timestamp>)> = Vec::new();

            let mut pending_configuration_data: HashMap<S::Timestamp, ControlSetBuilder<S::Timestamp>> = Default::default();

            let mut bootstrap_timestamps: HashSet<S::Timestamp> = HashSet::new();

            // TODO : default configuration may be poorly chosen.
            let mut active_configuration: ControlSet<S::Timestamp> = ControlSet {
                sequence: 0,
                frontier: Antichain::from_elem(Default::default()),
                map,
            };

            // Stash for consumed input buffers
            let mut data_return_buffer = vec![];

            let mut control_data_buffer = vec![];

            // Handle input data
            move |frontiers| {
                let mut data_out = data_out.activate();
                let mut state_out = state_out.activate();

                // TODO(lorenzo) try to use megaphone with nested scopes, does timestamp `Product` implement `num::One` ?
                //               `Product` should implement `one` if the InnerTimestamp::Summary implements `num::One`
                //               => need to change timely code

                routing_state_in.for_each(|bootstrap_time_plus_one, data| {
                    let mut buf = vec![];
                    data.swap(&mut buf);
                    assert_eq!(buf.len(), 1);
                    let (new_worker, routing_state) = &buf[0];

                    assert!(!routing_state_init, "only new worker should receive the routing state (and only once)");
                    assert_eq!(index, *new_worker, "wrong exchange");

                    // implant routing state received by the bootstrap worker
                    active_configuration = routing_state.active_configuration.clone();
                    pending_configuration_data = routing_state.pending_configuration_data.iter().map(|x| x.clone()).collect();

                    // Setup notifications for pending configurations.
                    // `bootstrap_time_plus_one` is the timestamp at which the `Bootstrap` command was received + 1 (timestamp is incremented in the feedback loop).
                    // Since:
                    //   1) we ensure that there are no other control commands associated with the same time of the `Bootstrap` message,
                    //   2) control commands in `pending_configuration_data` are associated with times for which a notification was not delivered yet
                    // then:
                    //   1) `bootstrap_time` is strictly smaller than any other time in `pending_configuration_data`
                    //   2) `bootstrap_time_plus_one` is smaller or equal than any other time in `pending_configuration_data`
                    // which means it can be used to setup notifications.
                    use timely::PartialOrder;
                    assert!(pending_configuration_data.keys().all(|config_time| bootstrap_time_plus_one.time().less_equal(config_time)));

                    pending_configuration_data.keys().for_each(|config_time| {
                        control_notificator.notify_at(&bootstrap_time_plus_one.delayed_for_output(config_time, 1));
                    });

                    // reset `states` to all none bins
                    assert!(active_configuration.map().iter().all(|i| *i != index), "new worker should not own anything");
                    let none_bins = (0..active_configuration.map().len()).map(|_| None).collect();
                    *states_f.borrow_mut() = State::new(none_bins);

                    routing_state_init = true;
                });

                // if initialization is not complete, we should perform any other operation
                if !routing_state_init { return }

                // Read control input
                control_in.for_each(|time, data| {
                    data.swap(&mut control_data_buffer);

                    let contains_bootstrap = control_data_buffer.iter().any(|x| if let ControlInst::Bootstrap(_, _) = x.inst { true } else { false });

                    if contains_bootstrap {
                        // Assert assumptions, some of them could be relaxed if we do things smarter/differently
                        assert_eq!(control_data_buffer.len(), 1, "bootstrap command should be the only command at its timestamp");
                        assert!(bootstrap_timestamps.insert(time.time().clone()), "two bootstrap commands with the same timestamp are not allowed");
                        assert!(!pending_configuration_data.contains_key(time.time()), "no other control instruction is allowed for the same timestamp of a bootstrap instruction");
                        // Since we cannot transfer capability across workers, we also require that there are no pending configuration
                        // for which a notification by the control notificator has already been delivered (and the configuration "built")
                        assert!(pending_configurations.is_empty(), "no pending configurations are allowed for the same timestamp of a bootstrap instruction");

                        let bootstrap = &control_data_buffer[0].inst;
                        if let crate::ControlInst::Bootstrap(bootstrap_server, new_worker) = bootstrap {
                            if index == *bootstrap_server {
                                let mut routing_state_out = routing_state_out.activate();
                                let routing_state = RoutingState {
                                    active_configuration: active_configuration.clone(),
                                    pending_configuration_data: pending_configuration_data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                                };
                                // We require that there are no pending configuration for which a notification by
                                // the control notificator has already been delivered (and the configuration "built")
                                assert!(pending_configurations.is_empty());
                                let cap = &time.delayed_for_output(time.time(), 2); // output_port 2 is routing_state_out
                                routing_state_out.session(cap).give((*new_worker, routing_state));
                            }
                        }
                    } else {
                        assert!(!bootstrap_timestamps.contains(time.time()), "no other control instruction is allowed for the same timestamp of a bootstrap instruction");
                        // Append to pending control instructions
                        let builder = pending_configuration_data.entry(time.time().clone()).or_insert_with(|| {
                            let mut builder: ControlSetBuilder<S::Timestamp> = Default::default();
                            // TODO: We don't know the frontier at the time the command was received.
                            builder.frontier(vec![time.time().clone()].into_iter());
                            builder
                        });
                        for update in control_data_buffer.drain(..) {
                            builder.apply(update);
                        }
                        control_notificator.notify_at(&time.retain_for_output(1));
                    }
                });

                // Analyze control frontier
                control_notificator.for_each(&[&frontiers[1]], |cap, time, _not| {
                    // Check if there are pending control instructions
                    if let Some(builder) = pending_configuration_data.remove(&time) {
                        // Build new configuration
                        let config = builder.build(pending_configurations.last().map_or(&active_configuration, |pending| &pending.1));
                        // Append to list of compiled configuration
                        pending_configurations.push((cap.delayed(&time), config));
                        // Sort by provided sequence number
                        pending_configurations.sort_by_key(|d| d.1.sequence);

                        // Configurations are well-formed if a bigger sequence number implies that
                        // actions are not reversely ordered. Each configuration has to dominate its
                        // successors.
                        for cs in pending_configurations.windows(2) {
                            debug_assert!(cs[0].1.frontier.dominates(&cs[1].1.frontier));
                        }
                        // Assert that the currently active configuration dominates the first pending
                        if let Some(config) = pending_configurations.first() {
                            debug_assert!(active_configuration.frontier.dominates(&config.1.frontier));
                        }
                    }
                });

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
                if pending_configurations.get(0).is_some() {
                    // TODO(lorenzo) double-check new worker sees the correct frontier
                    if pending_configurations[0].1.frontier.elements().iter().all(|t| !frontiers[2].less_than(t)) {

                        // We should now install `pending_configurations[0]` into `active_configuration`!
                        let (time, to_install) = pending_configurations.remove(0);

                        {   // Scoped to let `old_map` and `new_map` borrows drop.
                            let old_map = active_configuration.map();
                            let new_map = to_install.map();

                            // Grab states
                            let mut states = states_f.borrow_mut();
                            let mut session = state_out.session(&time);
                            // Determine if we're to move state
                            for (bin, (old, new)) in old_map.iter().zip(new_map.iter()).enumerate() {
                                // Migration is needed if a bin is to be moved (`old != new`) and the state
                                // actually contains data. Also, we must be the current owner of the bin.
                                let peers = *peers_rc.borrow();
                                // println!("[conf change] index={}, peers={}, bin={}: old={} => new={}", index, peers, bin, old, new);
                                assert!(*old < peers);
                                if (*old % peers == index) && (old != new) {
                                    // Capture bin's values as a stream of data
                                    let state = states.bins[bin].take().expect("Instructed to move bin but it is None");
                                    let Bin { data, notificator } = state;
                                    session.give((*new, StateProtocol::Prepare(BinId(bin))));
                                    let chunk: Vec<_> = data.into_iter().collect();
                                    println!("migration\t{}\t{}\t{}\t{}", bin, old, new, chunk.len());
                                    session.give((*new, StateProtocol::State(BinId(bin), chunk)));
                                    session.give_iterator(notificator.pending().map(|(t, d)| (*new, StateProtocol::Pending(BinId(bin), t, d))));
                                }
                            }
                        }

                        // Promote the pending config to active
                        active_configuration = to_install;
                    }
                }

                data_notificator.for_each(&[&frontiers[0], &frontiers[1]], |cap, time, _not| {
                    // Check for stashed data - now control input has to have advanced
                    if let Some(vec) = data_stash.remove(&time) {
                        let map =
                            pending_configurations
                                .iter()
                                .rev()
                                .map(|c| &c.1)
                                .find(|&c| c.frontier.less_equal(&time))
                                .unwrap_or(&active_configuration)
                                .map();

                        let session_cap = cap.delayed(&time);
                        let mut session = data_out.session(&session_cap);
                        for mut data in vec {
                            {
                                let data_iter = data.drain(..).map(|d| {
                                    let key_id = Key(key(&d));
                                    (map[key_to_bin(key_id)], key_id, d)
                                });
                                session.give_iterator(data_iter);
                            }
                            if data_return_buffer.len() < BUFFER_CAP {
                                data_return_buffer.push(data);
                            }
                        }
                    }
                });

                // Read data from the main data channel
                data_in.for_each(|time, data| {
                    // Can we process data? No if the control frontier is <= `time`
                    if frontiers[1].less_equal(time.time()) {
                        // No, stash data
                        if !data_stash.contains_key(time.time()) {
                            data_stash.insert(time.time().clone(), Vec::new());
                        }
                        let mut data_vec = data_return_buffer.pop().unwrap_or_else(Vec::new);
                        data.swap(&mut data_vec);
                        data_stash.get_mut(time.time()).unwrap().push(data_vec);
                        data_notificator.notify_at(&time.retain_for_output(0));
                    } else {
                        // Yes, control frontier not <= `time`, process right-away

                        // Find the configuration that applies to the input time
                        let map =
                            pending_configurations
                                .iter()
                                .rev()
                                .map(|c| &c.1)
                                .find(|&c| c.frontier.less_equal(time.time()))
                                .unwrap_or(&active_configuration)
                                .map();

                        let mut session = data_out.session(&time);

                        let mut data_vec = data_return_buffer.pop().unwrap_or_else(Vec::new);
                        data.swap(&mut data_vec);
                        let data_iter = data_vec.drain(..).map(|d| {
                            let key_id = Key(key(&d));
                            (map[key_to_bin(key_id)], key_id, d)
                        });
                        session.give_iterator(data_iter);
                    }
                });
            }
        });

        // `stream` is the stateful output stream where data is already correctly partitioned.
        StateStream::new(stream, state, states, feedback_handle)
    }
}

#[cfg(feature = "fake_stateful")]
impl<S: Scope, V: ExchangeData> Stateful<S, V> for Stream<S, V> {
    fn stateful<W, D, B, M>(&self, key: B, _control: &Stream<S, Control>) -> StateStream<S, V, D, W, M>
        where
            S::Timestamp : Hash+Eq+TotalOrder,
        // State format on the wire
            W: ExchangeData,
        // per-key state (data)
            D: IntoIterator<Item=W>+Extend<W>+Default,
        // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData,
    {
        // construct states, we simply construct all bins on each worker
        let states: Rc<RefCell<State<S::Timestamp, D, M>>> = Rc::new(RefCell::new(State::new(::std::iter::repeat_with(|| Some(Default::default())).take(1 << BIN_SHIFT).collect())));

        // Feedback handle to be attached after the last stateful operator
        let (feedback_handle, feedback_stream) = self.scope().feedback(Default::default());

        use timely::dataflow::operators::{Map, Exchange, Filter};

        // `stream` is the stateful output stream where data is already correctly partitioned.
        let stream = self
            .map(move |d| {
                let key = Key(key(&d));
                (key.0 as usize, key, d)
            })
            .exchange(|d| (d.0 ^ d.0.rotate_left(BIN_SHIFT as u32)) as u64);
        let state_stream = _control
            .filter(|_| false)
            .map(|_| (0, StateProtocol::Prepare(BinId(0))));
        StateStream::new(stream, state_stream, states, feedback_handle)
    }
}
