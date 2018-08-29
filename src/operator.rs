//! General purpose state transition operator.

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::communication::allocator::RefOrMut;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::{Capability, CapabilityRef};
use timely::dataflow::operators::generic::OutputHandle;
use timely::order::TotalOrder;

use ::{Bin, Control, Key, State};
use stateful::{Stateful, apply_state_updates, Notificator};
use notificator::{Notify};

pub trait StatefulOperator<G, D1>
    where
        G: Scope,
        G::Timestamp: TotalOrder,
        D1: ExchangeData + Eq,
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(&Capability<G::Timestamp>,
            &[D1],
            &mut Bin<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2>
    ;

    fn stateful_unary_input<
        D2: Data,                                    // output type
        N: ExchangeData+Eq,
        B: Fn(&D1)->u64+'static,                     // Key extraction function
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static, // State type
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            &[N],
            &mut Bin<G::Timestamp, S, N>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: FnMut(&mut State<G::Timestamp, S, N>,
            Capability<G::Timestamp>,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, consume: C, fold: F) -> Stream<G, D2>
    ;

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
            &[D1],
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 1
        F2: FnMut(&Capability<G::Timestamp>,
            &[D2],
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 2
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    ;

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
            &[N1],
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &[N2],
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: FnMut(&mut State<G::Timestamp, S1, N1>,
            Capability<G::Timestamp>, RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: FnMut(&mut State<G::Timestamp, S2, N2>,
            Capability<G::Timestamp>, RefOrMut<Vec<(usize, Key, D2)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, input1: C1, input2: C2, fold1: F1, fold2: F2) -> Stream<G, D3>
    ;

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
        F: Fn(&Capability<G::Timestamp>,
            &[D1],
            &mut Bin<G::Timestamp, S, D1>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2>
    {
        let mut stateful = self.stateful(key, control);
        let states = stateful.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        let mut state_update_buffer = vec![];
        let mut data_buffer = vec![];

        let mut notificator = Notificator::new();

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    data.swap(&mut state_update_buffer);
                    apply_state_updates(&mut states, &time, state_update_buffer.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    data.swap(&mut data_buffer);
                    if frontiers.iter().all(|f| !f.less_equal(time.time())) {
                        let cap = time.retain();
                        for (_worker, key_id, d) in data_buffer.drain(..) {
                            let slice = [d; 1];
                            fold(&cap, &slice[..], states.get(key_id), &mut output_handle);
                        }
                    } else {
                        notificator.notify_at_data(time.retain(), data_buffer.drain(..));
                    }
                }

                for (time, data) in notificator.next(&[&frontiers[0], &frontiers[1]]) {
                    for (_worker, key_id, d) in data {
                        let mut slice = [d; 1];
                        fold(&time, &mut slice[..], states.get(key_id), &mut output_handle);
                    }
                }

                // go through each time with data
                for bin in states.bins.iter_mut().filter(|b| b.is_some()) {
                    let bin = bin.as_mut().unwrap();
                    while let Some((time, data)) = bin.notificator().next(&[&frontiers[0], &frontiers[1]]) {
                        fold(&time, data.as_slice(), bin, &mut output_handle);
                    }
                }
            }
        });
        stream.probe_with(&mut stateful.probe)
    }

    fn stateful_unary_input<
        D2: Data,                                    // output type
        N: ExchangeData+Eq,
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            &[N],
            &mut Bin<G::Timestamp, S, N>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: FnMut(&mut State<G::Timestamp, S, N>,
            Capability<G::Timestamp>,
            RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, mut consume: C, fold: F) -> Stream<G, D2>
    {
        let mut stateful = self.stateful(key, control);
        let states = stateful.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        let mut state_update_buffer = vec![];
        let mut notificator = Notificator::new();

        let mut data_buffer = vec![];

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    data.swap(&mut state_update_buffer);
                    apply_state_updates(&mut states, &time, state_update_buffer.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    if frontiers.iter().all(|f| !f.less_equal(time.time())) {
                        consume(&mut states, time.retain(), data, &mut output_handle);
                    } else {
                        data.swap(&mut data_buffer);
                        notificator.notify_at_data(time.retain(), data_buffer.drain(..));
                    }
                }

                for (time, mut data) in notificator.next(&[&frontiers[0], &frontiers[1]]) {
                    consume(&mut states, time, RefOrMut::Mut(&mut data), &mut output_handle);
                }

                // go through each time with data
                for bin in states.bins.iter_mut().filter(|b| b.is_some()) {
                    let bin = bin.as_mut().unwrap();
                    while let Some((time, data)) = bin.notificator().next(&[&frontiers[0], &frontiers[1]]) {
                        fold(time, data.as_slice(), bin, &mut output_handle);
                    }
                }
            }
        });
        stream.probe_with(&mut stateful.probe)
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
            &[D1],
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &[D2],
            &mut Bin<G::Timestamp, S1, D1>,
            &mut Bin<G::Timestamp, S2, D2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    {

        let mut data1_buffer = vec![];
        let mut data2_buffer = vec![];

        self.stateful_binary_input(control, other, key1, key2, name,
            move |state, cap, data, _output| {
                data.swap(&mut data1_buffer);
                for (_worker, key_id, d) in data1_buffer.drain(..) {
                    state.get(key_id).notificator().notify_at_data(cap.clone(), Some(d));
                }
            },
            move |state, cap, data, _output| {
               data.swap(&mut data2_buffer);
               for (_worker, key_id, d) in data2_buffer.drain(..) {
                   state.get(key_id).notificator().notify_at_data(cap.clone(), Some(d));
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
            &[N1],
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: FnMut(&Capability<G::Timestamp>,
            &[N2],
            &mut Bin<G::Timestamp, S1, N1>,
            &mut Bin<G::Timestamp, S2, N2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: FnMut(&mut State<G::Timestamp, S1, N1>,
            Capability<G::Timestamp>, RefOrMut<Vec<(usize, Key, D1)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: FnMut(&mut State<G::Timestamp, S2, N2>,
            Capability<G::Timestamp>, RefOrMut<Vec<(usize, Key, D2)>>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, mut consume1: C1, mut consume2: C2, mut fold1: F1, mut fold2: F2) -> Stream<G, D3>
    {
        let mut stateful1 = self.stateful(key1, &control);
        let mut stateful2 = other.stateful(key2, &control);
        let states1 = stateful1.state.clone();
        let states2 = stateful2.state.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input1 = builder.new_input(&stateful1.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input1_state = builder.new_input(&stateful1.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let mut input2 = builder.new_input(&stateful2.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input2_state = builder.new_input(&stateful2.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let (mut output, stream) = builder.new_output();

        builder.build(move |_capability| {
            let mut state1_update_buffer = vec![];
            let mut state2_update_buffer = vec![];

            let mut data1_buffer = vec![];
            let mut data2_buffer = vec![];

            let mut notificator1 = Notificator::new();
            let mut notificator2 = Notificator::new();

            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states1 = states1.borrow_mut();
                let mut states2 = states2.borrow_mut();

                while let Some((time, data)) = input1_state.next() {
                    data.swap(&mut state1_update_buffer);
                    apply_state_updates(&mut states1, &time, state1_update_buffer.drain(..))
                }
                while let Some((time, data)) = input2_state.next() {
                    data.swap(&mut state2_update_buffer);
                    apply_state_updates(&mut states2, &time, state2_update_buffer.drain(..))
                }

                // stash each input and request a notification when ready
                while let Some((time, data)) = input1.next() {
                    if frontiers.iter().all(|f| !f.less_equal(time.time())) {
                        consume1(&mut states1, time.retain(), data, &mut output_handle);
                    } else {
                        data.swap(&mut data1_buffer);
                        notificator1.notify_at_data(time.retain(), data1_buffer.drain(..));
                    }
                }

                while let Some((time, data)) = input2.next() {
                    if frontiers.iter().all(|f| !f.less_equal(time.time())) {
                        consume2(&mut states2, time.retain(), data, &mut output_handle);
                    } else {
                        data.swap(&mut data2_buffer);
                        notificator2.notify_at_data(time.retain(), data2_buffer.drain(..));
                    }
                }

                for (time, mut data) in notificator1.next(&[&frontiers[0], &frontiers[1]]) {
                    consume1(&mut states1, time, RefOrMut::Mut(&mut data), &mut output_handle);
                }

                for (time, mut data) in notificator2.next(&[&frontiers[0], &frontiers[1]]) {
                    consume2(&mut states2, time, RefOrMut::Mut(&mut data), &mut output_handle);
                }

                // go through each time with data
                for (bin1, bin2) in states1.bins.iter_mut().zip(states2.bins.iter_mut()).filter(|(b1, b2)| b1.is_some() && b2.is_some()) {
                    let (bin1, bin2) = (bin1.as_mut().unwrap(), bin2.as_mut().unwrap());
                    while let Some((time, data)) = bin1.notificator().next(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]]) {
                        fold1(&time, data.as_slice(), bin1, bin2, &mut output_handle);
                    }
                    while let Some((time, data)) = bin2.notificator().next(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]]) {
                        fold2(&time, data.as_slice(), bin1, bin2, &mut output_handle);
                    }
                }
            }
        });
        stream.probe_with(&mut stateful1.probe).probe_with(&mut stateful2.probe)
    }

    fn distribute<B1>(&self, control: &Stream<G, Control>, key: B1, name: &str) -> Stream<G, (usize, Key, D1)>
        where
            B1: Fn(&D1)->u64+'static,
    {
        let mut data_vec = vec![];
        self.stateful_unary_input::<_, (), _, Vec<()>, _, _, _>(control, key, name, move |_state, time, data, output| {
            data.swap(&mut data_vec);
            output.session(&time).give_vec(&mut data_vec);
        }, |_time, _data, _bin, _output| {})
    }

}
