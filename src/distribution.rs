//! General purpose state transition operator.
use std::hash::Hash;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::operators::generic::binary::Binary;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

#[derive(Abomonation, Clone, Debug)]
pub struct Control {
    sequence: u64,
    count: usize,

    inst: ControlInst,
}

#[derive(Abomonation, Clone, Debug)]
pub enum ControlInst {
    Map(Vec<usize>),
    Move(/*bin*/ usize, /*worker*/ usize),
    None,
}

impl Control {
    pub fn new(sequence: u64, count: usize, inst: ControlInst) -> Self {
        Self { sequence, count, inst }
    }
}

struct ControlSet<T> {
    sequence: u64,
    frontier: Antichain<T>,
    instructions: Vec<ControlInst>,
}

impl<T> ControlSet<T> {

    fn map(&self) -> &Vec<usize> {
        for inst in &self.instructions {
            if let &ControlInst::Map(ref map) = inst {
                return map;
            }
        };
        unreachable!();
    }
}

struct ControlSetBuilder<T> {
    sequence: Option<u64>,
    frontier: Vec<T>,
    instructions: Vec<ControlInst>,

    count: Option<usize>,
}

impl<T: PartialOrder> ControlSetBuilder<T> {
    fn new() -> Self {
        Self {
            sequence: None,
            frontier: Vec::new(),
            instructions: Vec::new(),
            count: None,
        }
    }

    fn apply(&mut self, control: Control) {
        if self.count.is_none() {
            self.count = Some(control.count);
        }
        if let Some(ref mut count) = self.count {
            assert!(*count > 0);
            *count -= 1;
        }
        if let Some(sequence) = self.sequence {
            assert_eq!(sequence, control.sequence);
        } else {
            self.sequence = Some(control.sequence);
        }
        match control.inst {
            ControlInst::None => {},
            inst => self.instructions.push(inst),
        };

    }

    fn frontier<I: IntoIterator<Item=T>>(&mut self, caps: I) {
        self.frontier.extend(caps);
    }

    fn build(self) -> ControlSet<T> {
        assert_eq!(0, self.count.unwrap_or(0));
        let mut frontier = Antichain::new();
        for f in self.frontier {frontier.insert(f);}
        ControlSet {
            sequence: self.sequence.unwrap(),
            frontier: frontier,
            instructions: self.instructions,
        }
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

        let hash = Rc::new(hash);
        let hash2 = Rc::clone(&hash);

        // bin -> keys -> state
        let states: Rc<RefCell<Vec<HashMap<K, D>>>> = Rc::new(RefCell::new(vec![HashMap::new(); 1 << BIN_SHIFT]));
        let states_f = Rc::clone(&states);

        let mut builder = OperatorBuilder::new("StateMachine F".into(), self.scope());

        let mut data_in = builder.new_input(self, Pipeline);
        let mut control_in = builder.new_input(control, Pipeline);
        let (mut data_out, stream) = builder.new_output();
        let (mut state_out, state) = builder.new_output();

        // Number of bits to use as container number
        let bin_shift = ::std::mem::size_of::<usize>() - BIN_SHIFT;

        builder.build(move |_capability| {

            let mut notificator = FrontierNotificator::new();

            // Data input stash, time -> Vec<D>
            let mut data_stash = HashMap::new();

            // Control input stash, time -> Vec<ControlInstr>
            let mut pending_control = HashMap::new();

            // Active configurations: Vec<(T, ControlInstr)>
            let mut configurations: Vec<ControlSet<S::Timestamp>> = Vec::new();

            let mut map: Vec<usize> = vec![0; 1 << BIN_SHIFT];

            // Handle input data
            move |frontiers| {
                let mut data_out = data_out.activate();
                let mut state_out = state_out.activate();

                // Read data from the main data channel
                data_in.for_each(|time, data| {
                    // Test if we're permitted to send out data at `time`
                    if frontiers[1].less_than(time.time()) {
                        // Control is behind, stash data
                        data_stash.entry(time.clone()).or_insert_with(Vec::new).extend(data.drain(..));
                        notificator.notify_at(time)
                    } else {
                        // Control input is ahead of `time`, pass data
                        let mut session = data_out.session(&time);
                        // Determine bin: Find the last one that is `less_than`
                        if let Some(ref config_inst) = configurations.iter().rev().find(|&c| c.frontier.less_equal(time.time())) {
                            let map = config_inst.map();
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
                notificator.for_each(&[&frontiers[0], &frontiers[1]], |time, _not| {
                    // Check if there are pending control instructions
                    if let Some(mut vec) = pending_control.remove(&time) {
                        // Extend the configurations with (T, ControlInst) tuples
                        let mut builder: ControlSetBuilder<S::Timestamp> = ControlSetBuilder::new();
                        for update in vec.drain(..) {
                            builder.apply(update);
                        }
                        // TODO: We don't know the frontier at the time the command was received.
                        builder.frontier(vec![time.time().clone()].into_iter());
                        configurations.push(builder.build());
                        configurations.sort_by_key(|d| d.sequence);
                        // Configurations are well-formed if a bigger sequence number implies that
                        // actions are not reversely ordered.
                        // TODO: This is not entirely correct - we have to check for dominance
                        for cs in configurations.windows(2) {
                            debug_assert!(cs[0].frontier.dominates(&cs[1].frontier));
                        }

                        // GC old configurations
                        // \forall f \in frontier[0]: \exists t \in config: t \leq f
                        let mut count = 0;
                        for config in &configurations {
                            if config.frontier.less_than(&time) {
                                count += 1;
                            } else {
                                break;
                            }
                        }
                        if count > 1 {
                            while count > 0 { configurations.remove(0); count -= 1; }
                        }
                    }
                    // Check for stashed data - now control input has to have advanced
                    if let Some(mut vec) = data_stash.remove(&time) {
                        let mut session = data_out.session(&time);
                        // Determine bin
                        if let Some(ref config_inst) = configurations.iter().rev().find(|&c| c.frontier.less_equal(time.time())) {
                            let map = config_inst.map();
                            session.give_iterator(vec.drain(..).map(|d| (map[(hash2(&d.0) >> bin_shift) as usize], d)))
                        }
                    }

                    // Did we cross a frontier?
                    // Here we can't really express frontier equality yet ):
                    // What we really want is to know if we can apply a configuration change or not.
                    for config in &configurations {
                        if config.frontier.elements()[0] == *time.time() {

                            let mut new_map = map.clone();
                            for inst in &config.instructions {
                                match *inst {
                                    ControlInst::Map(ref map) => {
                                        new_map.clear();
                                        new_map.extend(map.iter());
                                    },
                                    ControlInst::Move(bin, target) => new_map[bin] = target,
                                    ControlInst::None => {},
                                }
                            }


                            // Redistribute bins
                            let mut states = states_f.borrow_mut();
                            let mut session = state_out.session(&time);
                            for (bin, (old, new)) in map.iter_mut().zip(new_map.iter()).enumerate() {
                                if old != new && !states[bin].is_empty(){
                                    let state = ::std::mem::replace(&mut states[bin], HashMap::new());
                                    session.give((*new, bin, state.into_iter().collect::<Vec<_>>()));
                                    *old = *new;
                                }
                            }
                            break;
                        }
                    }
                });
            }
        });

        let mut pending = HashMap::new();   // times -> (keys -> state)
        let mut pending_states = HashMap::new();

        stream.binary_notify(&state, Exchange::new(move |&(target, _)| target as u64), Exchange::new(move |&(target, _bin, _)| target as u64), "StateMachine", vec![], move |input, state, output, notificator| {

            // stash each input and request a notification when ready
            input.for_each(|time, data| {
                // stash if not time yet
                if notificator.frontier(0).iter().any(|x| x.less_than(time.time()))
                    && notificator.frontier(1).iter().any(|x| x.less_than(time.time())){
                    pending.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                    notificator.notify_at(time);
                }
                    else {
                        // else we can process immediately
                        let mut session = output.session(&time);
                        let mut states = states.borrow_mut();
                        for (_, (key, val)) in data.drain(..) {
                            let bin = (hash(&key) >> bin_shift) as usize;
                            let (remove, output) = {
                                let state = states[bin].entry(key.clone()).or_insert_with(Default::default);
                                fold(&key, val, state)
                            };
                            if remove { states[bin].remove(&key); }
                            session.give_iterator(output.into_iter());
                        }
                    }
            });

            state.for_each(|time, data| {
                pending_states.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                notificator.notify_at(time);
            });

            // go through each time with data, process each (key, val) pair.
            notificator.for_each(|time,_,_| {
                if let Some(state_update) = pending_states.remove(time.time()) {
                    let mut states = states.borrow_mut();
                    for (_target, bin, internal) in state_update {
                        assert!(states[bin].is_empty());
                        states[bin].extend(internal.into_iter());
                    }
                }

                if let Some(pend) = pending.remove(time.time()) {
                    let mut session = output.session(&time);
                    let mut states = states.borrow_mut();
                    for (_, (key, val)) in pend {
                        let bin = (hash(&key) >> bin_shift) as usize;
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
