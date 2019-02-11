//! General purpose migratable operators.

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::{ConnectLoop, Filter, Map};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandle;
use timely::order::TotalOrder;

use ::{Bin, Control, Key, State};
use stateful::{Stateful, apply_state_updates, Notificator};
use notificator::{Notify};

/// Building blocks for single- and dual-input stateful operators.
pub trait StatefulOperator<G, D1>
    where
        G: Scope,
        G::Timestamp: TotalOrder,
        D1: ExchangeData + Eq,
{
    /// Stateful operator with a single input.
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D1)>,
            &mut Bin<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2>
    ;

    /// Stateful operator with a single input and input transformation.
    fn stateful_unary_input<
        D2: Data,                                    // output type
        N: ExchangeData+Eq,
        B: Fn(&D1)->u64+'static,                     // Key extraction function
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static, // State type
        W: ExchangeData,                            // State format on the wire
        F: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N)>,
            &mut Bin<G::Timestamp, S, N>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: FnMut(&mut State<G::Timestamp, S, N>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, consume: C, fold: F) -> Stream<G, D2>
    ;

    /// Stateful operator with two inputs.
    fn stateful_binary<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,                    // Key extraction function, input 1
        B2: Fn(&D2)->u64+'static,                    // Key extraction function, input 2
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static, // State type, input 1
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static, // State type, input 2
        W1: ExchangeData,                            // State format on the wire, input 1
        W2: ExchangeData,                            // State format on the wire, input 2
        F1: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D1)>,
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 1
        F2: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D2)>,
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 2
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    ;

    /// Stateful operator with two inputs and input transformation.
    fn stateful_binary_input<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        N1: ExchangeData,
        N2: ExchangeData,
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N1)>,
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N2)>,
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: FnMut(&mut State<G::Timestamp, S1, N1>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: FnMut(&mut State<G::Timestamp, S2, N2>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D2)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, input1: C1, input2: C2, fold1: F1, fold2: F2) -> Stream<G, D3>
    ;

    /// Move state to a worker as specified in the control input. Do not maintain state.
    fn distribute<B1>(&self, control: &Stream<G, Control>, key: B1, name: &str) -> Stream<G, (usize, Key, D1)>
    where
        B1: Fn(&D1)->u64+'static,
    ;
}

impl<G, D1> StatefulOperator<G, D1> for Stream<G, D1>
    where
        G: Scope, // The containing scope
        G::Timestamp: TotalOrder,
        D1: ExchangeData+Eq, // Input data
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D1)>,
            &mut Bin<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, mut fold: F) -> Stream<G, D2>
    {
        let stateful = self.stateful(key, control);
        let states = stateful.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        let mut state_update_buffer = vec![];

        let mut notificator = Notificator::new();

        let mut not_drain = Vec::new();
        let mut bin_drain = Vec::new();

        // TODO: Should probably be written in terms of `stateful_unary_input`
        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    data.swap(&mut state_update_buffer);
                    apply_state_updates(&mut states, &time.retain(), state_update_buffer.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    let mut data_buffer = vec![];
                    data.swap(&mut data_buffer);
                    let cap = time.retain();
                    notificator.notify_at_data(&cap, cap.time().clone(), data_buffer);
                }

                if let Some(cap) = notificator.drain(&[&frontiers[0], &frontiers[1]], &mut not_drain) {
                    for (time, mut keyed_data) in not_drain.drain(..) {
                        for (_, key_id, d) in keyed_data.drain(..) {
                            states.get(key_id).notificator.notify_at_data(&cap, time.clone(), d);
                        }
                    }
                }

                // go through each time with data
                for bin in states.bins.iter_mut().filter(|b| b.is_some()) {
                    let bin = bin.as_mut().unwrap();
                    if let Some(cap) = bin.notificator().drain(&[&frontiers[0], &frontiers[1]], &mut bin_drain) {
                        fold(&cap, &mut bin_drain, bin, &mut output_handle);
                    }
                }
            }
        });
        let progress_stream = stream.filter(|_| false).map(|_| ());
        progress_stream.connect_loop(stateful.feedback);
        stream
    }

    fn stateful_unary_input<
        D2: Data,                                    // output type
        N: ExchangeData+Eq,
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N)>,
            &mut Bin<G::Timestamp, S, N>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: FnMut(&mut State<G::Timestamp, S, N>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, mut consume: C, mut fold: F) -> Stream<G, D2>
    {
        let stateful = self.stateful(key, control);
        let states = stateful.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        let mut state_update_buffer = vec![];
        let mut notificator = Notificator::new();

        let mut not_drain = Vec::new();
        let mut bin_drain = Vec::new();

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    data.swap(&mut state_update_buffer);
                    apply_state_updates(&mut states, &time.retain(), state_update_buffer.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((cap, data)) = input.next() {
//                    if !frontiers[0].less_than(time.time()) && !frontiers[1].less_equal(time.time()) {
//                        consume(&mut states, time.retain(), data, &mut output_handle);
//                    } else {
                        let mut data_buffer = vec![];
                        data.swap(&mut data_buffer);
                        let time = cap.time().clone();
                        notificator.notify_at_data(&cap.retain(), time, data_buffer);
//                    }
                }

                if let Some(cap) = notificator.drain(&[&frontiers[0], &frontiers[1]], &mut not_drain) {
                    for (time, mut data) in not_drain.drain(..) {
                        consume(&mut states, &cap, time, RefOrMut::Mut(&mut data), &mut output_handle);
                    }
                }

                // go through each time with data
                for bin in states.bins.iter_mut().filter(|b| b.is_some()) {
                    let bin = bin.as_mut().unwrap();
                    if let Some(cap) = bin.notificator().drain(&[&frontiers[0], &frontiers[1]], &mut bin_drain) {
                        fold(&cap, &mut bin_drain, bin, &mut output_handle);
                    }
                }
            }
        });
        let progress_stream = stream.filter(|_| false).map(|_| ());
        progress_stream.connect_loop(stateful.feedback);
        stream
    }

    fn stateful_binary<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D1)>,
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, D2)>,
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    {

        let mut data1_buffer = vec![];
        let mut data2_buffer = vec![];

        self.stateful_binary_input(control, other, key1, key2, name,
            move |state, cap, time, data, _output| {
                data.swap(&mut data1_buffer);
                for (_worker, key_id, d) in data1_buffer.drain(..) {
                    state.get(key_id).notificator().notify_at_data(&cap, time.clone(), d);
                }
            },
            move |state, cap, time, data, _output| {
               data.swap(&mut data2_buffer);
               for (_worker, key_id, d) in data2_buffer.drain(..) {
                   state.get(key_id).notificator().notify_at_data(&cap, time.clone(), d);
               }
           }, fold1, fold2)
    }

    fn stateful_binary_input<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        N1: ExchangeData,
        N2: ExchangeData,
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N1)>,
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &mut Vec<(G::Timestamp, N2)>,
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: FnMut(&mut State<G::Timestamp, S1, N1>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: FnMut(&mut State<G::Timestamp, S2, N2>,
            &Capability<G::Timestamp>,
            G::Timestamp,
            RefOrMut<Vec<(usize, Key, D2)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, mut consume1: C1, mut consume2: C2, mut fold1: F1, mut fold2: F2) -> Stream<G, D3>
    {
        let stateful1 = self.stateful(key1, &control);
        let stateful2 = other.stateful(key2, &control);
        let states1 = stateful1.state.clone();
        let states2 = stateful2.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input1 = builder.new_input(&stateful1.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input1_state = builder.new_input(&stateful1.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let mut input2 = builder.new_input(&stateful2.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input2_state = builder.new_input(&stateful2.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let (mut output, stream) = builder.new_output();

        let mut not1_drain = Vec::new();
        let mut not2_drain = Vec::new();
        let mut bin1_drain = Vec::new();
        let mut bin2_drain = Vec::new();

        builder.build(move |_capability| {
            let mut state1_update_buffer = vec![];
            let mut state2_update_buffer = vec![];

            let mut notificator1 = Notificator::new();
            let mut notificator2 = Notificator::new();

            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states1 = states1.borrow_mut();
                let mut states2 = states2.borrow_mut();

                while let Some((time, data)) = input1_state.next() {
                    data.swap(&mut state1_update_buffer);
                    apply_state_updates(&mut states1, &time.retain(), state1_update_buffer.drain(..))
                }
                while let Some((time, data)) = input2_state.next() {
                    data.swap(&mut state2_update_buffer);
                    apply_state_updates(&mut states2, &time.retain(), state2_update_buffer.drain(..))
                }

                // stash each input and request a notification when ready
                while let Some((cap, data)) = input1.next() {
                    let mut data1_buffer = vec![];
                    data.swap(&mut data1_buffer);
                    let time = cap.time().clone();
                    notificator1.notify_at_data(&cap.retain(), time, data1_buffer);
                }

                while let Some((cap, data)) = input2.next() {
                    let mut data2_buffer = vec![];
                    data.swap(&mut data2_buffer);
                    let time = cap.time().clone();
                    notificator2.notify_at_data(&cap.retain(), time, data2_buffer);
                }

                if let Some(cap) = notificator1.drain(&[&frontiers[0], &frontiers[1]], &mut not1_drain) {
                    for (time, mut data) in not1_drain.drain(..) {
                        consume1(&mut states1, &cap, time, RefOrMut::Mut(&mut data), &mut output_handle);
                    }
                }

                if let Some(cap) = notificator2.drain(&[&frontiers[2], &frontiers[3]], &mut not2_drain) {
                    for (time, mut data) in not2_drain.drain(..) {
                        consume2(&mut states2, &cap, time, RefOrMut::Mut(&mut data), &mut output_handle);
                    }
                }

                // go through each time with data
                for (bin1, bin2) in states1.bins.iter_mut().zip(states2.bins.iter_mut()).filter(|(b1, b2)| b1.is_some() && b2.is_some()) {
                    let (bin1, bin2) = (bin1.as_mut().unwrap(), bin2.as_mut().unwrap());
                    if let Some(cap) = bin1.notificator().drain(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]], &mut bin1_drain) {
                        fold1(&cap, &mut bin1_drain, bin1, bin2, &mut output_handle);
                    }
                    if let Some(cap) = bin2.notificator().drain(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]], &mut bin2_drain) {
                        fold2(&cap, &mut bin2_drain, bin1, bin2, &mut output_handle);
                    }
                }
            }
        });
        let progress_stream = stream.filter(|_| false).map(|_| ());
        progress_stream.connect_loop(stateful1.feedback);
        progress_stream.connect_loop(stateful2.feedback);
        stream
    }

    fn distribute<B1>(&self, control: &Stream<G, Control>, key: B1, name: &str) -> Stream<G, (usize, Key, D1)>
        where
            B1: Fn(&D1)->u64+'static,
    {
        let mut data_vec = vec![];
        self.stateful_unary_input::<_, (), _, Vec<()>, _, _, _>(control, key, name, move |_state, cap, _time, data, output| {
            data.swap(&mut data_vec);
            output.session(&cap).give_vec(&mut data_vec);
        }, |_cap, _data, _bin, _output| {})
    }

}
