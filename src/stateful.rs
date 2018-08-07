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
use timely::PartialOrder;
use timely::dataflow::{Stream, Scope, ProbeHandle};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Capability, FrontierNotificator as TFN};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::Timestamp;
use timely::progress::frontier::Antichain;

use ::{BIN_SHIFT, Bin, Control, ControlSetBuilder, ControlSet, Key, key_to_bin};
use ::notificator::FrontierNotificator;

const BUFFER_CAP: usize = 16;

/// Generic state-transition machinery: each key has a state, and receives a sequence of events.
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent.
///
/// `control_state_machine` will buffer inputs if earlier inputs may still arrive. it will directly apply
/// updates for the current time reflected in the notificator, though. In the case of partially
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).


/// State abstraction. It encapsulates state assorted by bins and a notificator.
pub struct State<T: Timestamp, S, D: ExchangeData+Eq+PartialEq> {
    bins: Vec<Option<S>>,
    notificator: FrontierNotificator<T, (Key, D)>,
}

impl<T: Timestamp, S, D: ExchangeData+Eq+PartialEq> State<T, S, D> {
    /// Construct a new `State` with the provided vector of bins and a default `FrontierNotificator`.
    fn new(bins: Vec<Option<S>>) -> Self {
        Self { bins, notificator: FrontierNotificator::new() }
    }
}

/// State access functions.
pub trait StateHandle<T: Timestamp, S, D: ExchangeData+Eq+PartialEq> {

    /// Obtain a mutable reference to the state associated with a bin.
    fn get_state(&mut self, key: &Key) -> &mut S;

    /// Call-back to get state and a notificator.
    fn with_state_frontier<
        R,
        F: Fn(&mut S, &FrontierNotificator<T, (Key, D)>) -> R
    >(&mut self, key: &Key, f: F) -> R;

    /// Iterate all bins. This might go away.
    fn scan<F: FnMut(&mut S)>(&mut self, f: F);

    /// Obtain a reference to a notificator.
    fn notificator(&mut self) -> &mut FrontierNotificator<T, (Key, D)>;
}

impl<T: Timestamp, S, D: ExchangeData+Eq+PartialEq> StateHandle<T, S, D> for State<T, S, D> {

    fn get_state(&mut self, key: &Key) -> &mut S {
        assert!(self.bins[key_to_bin(key)].is_some(), "Accessing bin {} for key {:?}", key_to_bin(key), key);
        self.bins[key_to_bin(key)].as_mut().expect("Trying to access non-available bin")
    }

    #[inline(always)]
    fn with_state_frontier<
        R,
        F: Fn(&mut S, &FrontierNotificator<T, (Key, D)>) -> R
    >(&mut self, key: &Key, f: F) -> R {
        f(self.bins[key_to_bin(key)].as_mut().expect("Trying to access non-available bin"), &mut self.notificator)
    }

    fn scan<F: FnMut(&mut S)>(&mut self, mut f: F) {
        for state in &mut self.bins {
            state.as_mut().map(|state| f(state));
        }
    }

    fn notificator(&mut self) -> &mut FrontierNotificator<T, (Key, D)> {
        &mut self.notificator
    }
}

/// Datatype to multiplex state and timestamps on the state update channel.
#[derive(Abomonation, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum StateProtocol<T, S, D> {
    /// Provide a piece of state for a bin
    State(Bin, S),
    /// Announce an outstanding time stamp
    Pending(T, (Key, D)),
    /// Prepare for receiving state
    Prepare(Bin),
}

/// A timely `Stream` with an additional state handle and a probe.
pub struct StateStream<S, V, D, W, M> where
    S: Scope, // The containing scope
    V: ExchangeData, // Input data
    D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,    // per-bin state (data)
    W: ExchangeData,                            // State format on the wire
    M: ExchangeData+Eq+PartialEq,
{
    /// The wrapped stream. The stream provides tuples of the form `(usize, Key, V)`. The first two
    /// parameters are the target worker and the key identifier. Implementations are encouraged to
    /// ignore the target worker. The key identifier has to be used to obtain the associated state
    /// from [`StateHandle`].
    ///
    /// [`StateHandle`]: trait.StateHandle.html
    pub stream: Stream<S, (usize, Key, V)>,
    /// A handle to the shared state object
    pub state: Rc<RefCell<State<S::Timestamp, D, M>>>,
    /// The probe `stateful` uses to determine completion.
    pub probe: ProbeHandle<S::Timestamp>,
    _phantom: PhantomData<(*const W)>,
}

impl<S, V, D, W, M> StateStream<S, V, D, W, M>
    where
        S: Scope, // The containing scope
        V: ExchangeData, // Input data
        D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,    // per-key state (data)
        W: ExchangeData,
        M: ExchangeData+Eq+PartialEq,
{
    pub fn new(stream: Stream<S, (usize, Key, V)>, state: Rc<RefCell<State<S::Timestamp, D, M>>>, probe: ProbeHandle<S::Timestamp>) -> Self {
        StateStream {
            stream,
            state,
            probe,
            _phantom: PhantomData,
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
            S::Timestamp : Hash+Eq,
            // State format on the wire
            W: ExchangeData,
            // per-key state (data)
            D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
            // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData+Eq+PartialEq,
    ;
}

#[cfg(not(feature = "fake_stateful"))]
impl<S: Scope, V: ExchangeData> Stateful<S, V> for Stream<S, V> {

    fn stateful<W, D, B, M>(&self, key: B, control: &Stream<S, Control>) -> StateStream<S, V, D, W, M>
        where
            S::Timestamp : Hash+Eq,
            // State format on the wire
            W: ExchangeData,
            // per-key state (data)
            D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
            // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData+Eq+PartialEq,
    {
        let index = self.scope().index();
        let peers = self.scope().peers();

        // worker-local state, maps bins to state
        let default_element: Option<D> = if self.scope().index() == 0 {
            Some(Default::default())
        } else {
            None
        };
        let states: Rc<RefCell<State<S::Timestamp, D, M>>> = Rc::new(RefCell::new(State::new(vec![default_element; 1 << BIN_SHIFT])));
        let states_f = Rc::clone(&states);
        let states_op = Rc::clone(&states);

        let mut builder = OperatorBuilder::new("StateMachine F".into(), self.scope());

        // The data input
        let mut data_in = builder.new_input(self, Pipeline);
        // The control input
        let mut control_in = builder.new_input(control, Pipeline);
        // Data output of the F operator
        let (mut data_out, stream) = builder.new_output();
        // State output of the F operator
        let (mut state_out, state) = builder.new_output();

        // Probe to be attached after the last stateful operator
        let probe1 = ProbeHandle::new();
        let probe2 = probe1.clone();

        // Construct F operator
        builder.build(move |_capability| {

            // distinct notificators for data and control input
            let mut data_notificator = TFN::new();
            let mut control_notificator = TFN::new();

            // Data input stash, time -> Vec<Vec<V>>
            let mut data_stash: HashMap<_, Vec<Vec<V>>> = Default::default();

            // Active configurations: Vec<(T, ControlInstr)> sorted by increasing T. Note that
            // we assume the Ts form a total order, i.e. they must dominate each other.
            let mut pending_configurations: Vec<(Capability<S::Timestamp>, ControlSet<S::Timestamp>)> = Vec::new();

            let mut pending_configuration_data: HashMap<S::Timestamp, ControlSetBuilder<S::Timestamp>> = Default::default();

            // TODO : default configuration may be poorly chosen.
            let mut active_configuration: ControlSet<S::Timestamp> = ControlSet { 
                sequence: 0, 
                frontier: Antichain::from_elem(Default::default()),
                map: vec![0; 1 << BIN_SHIFT],
            };

            // Stash for consumed input buffers
            let mut data_return_buffer = vec![];

            // Handle input data
            move |frontiers| {
                let mut data_out = data_out.activate();
                let mut state_out = state_out.activate();

                // Read control input
                control_in.for_each(|time, data| {
                    // Append to pending control instructions
                    let builder = pending_configuration_data.entry(time.time().clone()).or_insert_with(|| {
                        let mut builder: ControlSetBuilder<S::Timestamp> = Default::default();
                        // TODO: We don't know the frontier at the time the command was received.
                        builder.frontier(vec![time.time().clone()].into_iter());
                        builder
                    });
                    for update in data.drain(..) {
                        builder.apply(update);
                    }
                    let time = time.retain();
                    control_notificator.notify_at(time.clone());
                });

                // Analyze control frontier
                control_notificator.for_each(&[&frontiers[1]], |time, _not| {
                    // Check if there are pending control instructions
                    if let Some(builder) = pending_configuration_data.remove(time.time()) {
                        // Build new configuration
                        let config = builder.build(pending_configurations.last().map_or(&active_configuration, |pending| &pending.1));
                        // Append to list of compiled configuration
                        pending_configurations.push((time, config));
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
                    if pending_configurations[0].1.frontier.elements().iter().all(|t| !probe2.less_than(t)) {

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
                                if (*old % peers == index) && (old != new) {
                                    // Capture bin's values as a stream of data
                                    let state = states.bins[bin].take().expect("Instructed to move bin but it is None");
                                    session.give((*new, StateProtocol::Prepare(Bin(bin))));
                                    session.give_iterator(state.into_iter().map(|s| (*new, StateProtocol::State(Bin(bin), s))));
                                }
                            }
                            for (cap, data) in states.notificator.pending_mut().iter_mut() {
                                data.retain(|(key_id, meta)| {
                                    let old_worker = old_map[key_to_bin(key_id)];
                                    let new_worker = new_map[key_to_bin(key_id)];
                                    if old_worker != new_worker {
                                        // Pass pending notifications to the new owner
                                        // Note: The receiver will get *all* notifications, so an
                                        // operator can experience spurious wake-ups
                                        session.give((new_worker, StateProtocol::Pending(cap.time().clone(), (*key_id, meta.clone()))));
                                        false
                                    } else {
                                        true
                                    }
                                });
                            }
                        }

                        // Promote the pending config to active
                        active_configuration = to_install;
                    }
                }

                data_notificator.for_each(&[&frontiers[0], &frontiers[1]], |time, _not| {
                    // Check for stashed data - now control input has to have advanced
                    if let Some(vec) = data_stash.remove(time.time()) {

                        let map =
                            pending_configurations
                                .iter()
                                .rev()
                                .map(|c| &c.1)
                                .find(|&c| c.frontier.less_equal(time.time()))
                                .unwrap_or(&active_configuration)
                                .map();

                        let mut session = data_out.session(&time);
                        for mut data in vec {
                            {
                                let data_iter = data.drain(..).map(|d| {
                                    let key = Key(key(&d));
                                    (map[key_to_bin(&key)], key, d)
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
                        data_stash.get_mut(time.time()).unwrap().push(data.replace_with(data_return_buffer.pop().unwrap_or_else(Vec::new)));
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

                        let data_iter = data.drain(..).into_iter().map(|d| {
                            let key = Key(key(&d));
                            (map[key_to_bin(&key)], key, d)
                        });
                        session.give_iterator(data_iter);
                    }

                    // We need to notify in any case as we might be able to drop some configurations
                    data_notificator.notify_at(time.retain());
                });

            }
        });

        // Now, construct the S operator

        // Read data input and state input
        // Route each according to the encoded target worker
        let stream = stream.binary(&state, Exchange::new(move |&(target, _key, _)| target as u64), Exchange::new(move |&(target, _)| target as u64), "State", |_cap, _info| {
            move |input, state, output| {

                // Handle data input
                input.for_each(|time, data| {
                    output.session(&time).give_content(data);
                });

                // Handle state updates
                state.for_each(|_time, data| {
                    let mut states = states.borrow_mut();

                    // Apply each state update
                    for (_target, state) in data.drain(..) {
                        match state {
                            StateProtocol::Prepare(bin) => {
                                assert!(states.bins[*bin].is_none());
                                states.bins[*bin] = Some(Default::default());
                            }
                            // Extend state
                            StateProtocol::State(bin, s) => {
                                states.bins[*bin].as_mut().map(|bin| bin.extend(Some(s)));
                            },
                            // Request notification
                            StateProtocol::Pending(t, data) =>
                                states.notificator.enqueue(t, vec![data]),
                        }
                    }
                });
            }
        });

        // `stream` is the stateful output stream where data is already correctly partitioned.
        StateStream::new(stream, states_op, probe1)
    }
}

#[cfg(feature = "fake_stateful")]
impl<S: Scope, V: ExchangeData> Stateful<S, V> for Stream<S, V> {
    fn stateful<W, D, B, M>(&self, key: B, control: &Stream<S, Control>) -> StateStream<S, V, D, W, M>
        where
            S::Timestamp : Hash+Eq,
        // State format on the wire
            W: ExchangeData,
        // per-key state (data)
            D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        // "hash" function for values
            B: Fn(&V)->u64+'static,
            M: ExchangeData+Eq+PartialEq,
    {
        let states: Rc<RefCell<State<S::Timestamp, D, M>>> = Rc::new(RefCell::new(State::new(vec![Some(Default::default()); 1 << BIN_SHIFT])));
        let states_op = Rc::clone(&states);
        // Probe to be attached after the last stateful operator
        let probe1 = ProbeHandle::new();

        use timely::dataflow::operators::{Map, Exchange};

        // `stream` is the stateful output stream where data is already correctly partitioned.
        StateStream::new(self.map(move |d| {
            let key = Key(key(&d));
            (key.0 as usize, key, d)
        }).exchange(|d| d.0 as u64), states_op, probe1)
    }
}
