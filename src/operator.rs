//! General purpose state transition operator.
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::Content;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::Probe;
use timely::Data;
use timely::dataflow::operators::{Capability, CapabilityRef, Operator};
use timely::dataflow::operators::generic::OutputHandle;

use ::{Control, Key};
use stateful::{Stateful, State, StateHandle};
use notificator::FrontierNotificator;

pub trait StatefulOperator<G: Scope, D1: ExchangeData+Hash+Eq> {
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2> where G::Timestamp: Hash + Eq
    ;

    fn stateful_unary_input<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, consume: C, fold: F) -> Stream<G, D2> where G::Timestamp: Hash + Eq
    ;

    fn stateful_binary<
        D2: ExchangeData+Hash+Eq,                    // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3> where G::Timestamp: Hash + Eq
    ;

    fn stateful_binary_input<
        D2: ExchangeData+Hash+Eq,                    // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>) + 'static,
        C2: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D2)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D2)>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, input1: C1, input2: C2, fold1: F1, fold2: F2) -> Stream<G, D3> where G::Timestamp: Hash + Eq
    ;

}

impl<G, D1> StatefulOperator<G, D1> for Stream<G, D1>
    where
        G: Scope, // The containing scope
        D1: ExchangeData+Hash+Eq, // Input data
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2> where G::Timestamp: Hash + Eq
    {
        self.stateful_unary_input(control, key, name,
                                   |not, time, data| not.notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect()),
                                   fold)
    }

    fn stateful_unary_input<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, consume: C, fold: F) -> Stream<G, D2> where G::Timestamp: Hash + Eq
    {
        let mut stateful = self.stateful(move |v| key(&v), control);
        let states = stateful.state.clone();

        stateful.stream.unary_frontier(Pipeline, name, |cap, _info| {
            states.borrow_mut().notificator().init_cap(&cap);
            move |input, output| {
                let mut states = states.borrow_mut();
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    consume(&mut states.notificator(), time, data);
                };

                // go through each time with data
                while let Some((time, data)) = states.notificator().next(&[input.frontier()]) {
                    fold(time, data, &mut states, output);
                }
            }
        }).probe_with(&mut stateful.probe)
    }

    fn stateful_binary<
        D2: ExchangeData+Hash+Eq,                    // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3> where G::Timestamp: Hash + Eq {

        self.stateful_binary_input(control, other, key1, key2, name,
            |not, time, data| not.notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect()),
           |not, time, data| not.notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect()),
            fold1, fold2)
    }

    fn stateful_binary_input<
        D2: ExchangeData+Hash+Eq,                    // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<G::Timestamp, S1, D1>,
            &mut State<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>) + 'static,
        C2: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D2)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D2)>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, consume1: C1, consume2: C2, fold1: F1, fold2: F2) -> Stream<G, D3> where G::Timestamp: Hash + Eq
    {
        let mut stateful1 = self.stateful(move |d1| key1(&d1), &control);
        let mut stateful2 = other.stateful(move |d2| key2(&d2), &control);
        let states1 = stateful1.state.clone();
        let states2 = stateful2.state.clone();

        stateful1.stream.binary_frontier(&stateful2.stream, Pipeline, Pipeline, name, |cap, _info| {
            states1.borrow_mut().notificator().init_cap(&cap);
            states2.borrow_mut().notificator().init_cap(&cap);
            move |input1, input2, output| {
                let mut states1 = states1.borrow_mut();
                let mut states2 = states2.borrow_mut();
                // stash each input and request a notification when ready
                while let Some((time, data)) = input1.next() {
                    consume1(&mut states1.notificator(), time, data);
                };
                while let Some((time, data)) = input2.next() {
                    consume2(&mut states2.notificator(), time, data);
                };

                // go through each time with data
                while let Some((time, data)) = states1.notificator().next(&[input1.frontier(), input2.frontier()]) {
                    fold1(time, data, &mut states1, &mut states2, output);
                }
                while let Some((time, data)) = states2.notificator().next(&[input1.frontier(), input2.frontier()]) {
                    fold2(time, data, &mut states1, &mut states2, output);
                }
            }
        }).probe_with(&mut stateful1.probe).probe_with(&mut stateful2.probe)
    }
}
