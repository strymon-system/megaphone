//! General purpose state transition operator.

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::Content;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::{Capability, CapabilityRef};
use timely::dataflow::operators::generic::OutputHandle;

use ::{Control, Key, State};
use stateful::{Stateful, apply_state_updates};
use notificator::{Notify, FrontierNotificator};

pub trait StatefulOperator<G, D1>
    where
        G: Scope,
        D1: ExchangeData + Eq,
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S>,
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
            Vec<(Key, N)>,
            &mut State<S>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, N)>,
            CapabilityRef<G::Timestamp>,
            &mut Content<(usize, Key, D1)>,
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
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 1
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic, input 2
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    ;

    fn stateful_binary_input<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D2)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D2)>,
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
        D1: ExchangeData+Eq, // Input data
{
    fn stateful_unary<
        D2: Data,                                    // output type
        B: Fn(&D1)->u64+'static,
        S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
        W: ExchangeData,                            // State format on the wire
        F: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> Stream<G, D2>
    {
        let mut stateful = self.stateful(move |v| key(&v), control);
        let states = stateful.state.clone();
        let notificator = stateful.notificator.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                let mut notificator = notificator.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    apply_state_updates(&mut states, &mut notificator, &time, data.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    if frontiers.iter().all(|f| !f.less_equal(time.time())) {
                        fold(time.retain(), data.drain(..).map(|(_worker, key_id, d)| (key_id, d)).collect(), &mut states, &mut output_handle);
                    } else {
                        notificator.notify_at_data(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)));
                    }
                }

                // go through each time with data
                while let Some((time, data)) = notificator.next(&[&frontiers[0], &frontiers[1]]) {
                    fold(time, data, &mut states, &mut output_handle);
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
            Vec<(Key, N)>,
            &mut State<S>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
        C: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, N)>,
            CapabilityRef<G::Timestamp>,
            &mut Content<(usize, Key, D1)>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,
    >(&self, control: &Stream<G, Control>, key: B, name: &str, consume: C, fold: F) -> Stream<G, D2>
    {
        let mut stateful = self.stateful(move |v| key(&v), control);
        let states = stateful.state.clone();
        let notificator = stateful.notificator.clone();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

        let (mut output, stream) = builder.new_output();

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states = states.borrow_mut();
                let mut notificator = notificator.borrow_mut();
                while let Some((time, data)) = input_state.next() {
                    apply_state_updates(&mut states, &mut notificator, &time, data.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    consume(&mut notificator, time, data, &mut output_handle);
                };

                // go through each time with data
                while let Some((time, data)) = notificator.next(&[&frontiers[0], &frontiers[1]]) {
                    fold(time, data, &mut states, &mut output_handle);
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
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, fold1: F1, fold2: F2) -> Stream<G, D3>
    {

        self.stateful_binary_input(control, other, key1, key2, name,
            |not, time, data, _output| not.notify_at_data(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d))),
           |not, time, data, _output| not.notify_at_data(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d))),
            fold1, fold2)
    }

    fn stateful_binary_input<
        D2: ExchangeData+Eq,                         // input type
        D3: Data,                                    // output type
        B1: Fn(&D1)->u64+'static,
        B2: Fn(&D2)->u64+'static,
        S1: Clone+IntoIterator<Item=W1>+Extend<W1>+Default+'static,
        S2: Clone+IntoIterator<Item=W2>+Extend<W2>+Default+'static,
        W1: ExchangeData,                            // State format on the wire
        W2: ExchangeData,                            // State format on the wire
        F1: Fn(Capability<G::Timestamp>,
            Vec<(Key, D1)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        F2: Fn(Capability<G::Timestamp>,
            Vec<(Key, D2)>,
            &mut State<S1>,
            &mut State<S2>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,    // state update logic
        C1: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D1)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D1)>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
        C2: Fn(&mut FrontierNotificator<G::Timestamp, (::Key, D2)>,
            CapabilityRef<G::Timestamp>, &mut Content<(usize, Key, D2)>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>) + 'static,
    >(&self, control: &Stream<G, Control>, other: &Stream<G, D2>, key1: B1, key2: B2, name: &str, consume1: C1, consume2: C2, fold1: F1, fold2: F2) -> Stream<G, D3>
    {
        let mut stateful1 = self.stateful(move |d1| key1(&d1), &control);
        let mut stateful2 = other.stateful(move |d2| key2(&d2), &control);
        let states1 = stateful1.state.clone();
        let states2 = stateful2.state.clone();
        let notificator1 = ::std::rc::Rc::clone(&stateful1.notificator);
        let notificator2 = ::std::rc::Rc::clone(&stateful2.notificator);

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input1 = builder.new_input(&stateful1.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input1_state = builder.new_input(&stateful1.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let mut input2 = builder.new_input(&stateful2.stream, Exchange::new(move |&(target, _key, _)| target as u64));
        let mut input2_state = builder.new_input(&stateful2.state_stream, Exchange::new(move |&(target, _)| target as u64));
        let (mut output, stream) = builder.new_output();

        builder.build(move |_capability| {
            move |frontiers| {
                let mut output_handle = output.activate();

                let mut states1 = states1.borrow_mut();
                let mut states2 = states2.borrow_mut();
                let mut notificator1 = notificator1.borrow_mut();
                let mut notificator2 = notificator2.borrow_mut();
                while let Some((time, data)) = input1_state.next() {
                    apply_state_updates(&mut states1, &mut notificator1, &time, data.drain(..))
                }
                while let Some((time, data)) = input2_state.next() {
                    apply_state_updates(&mut states2, &mut notificator2, &time, data.drain(..))
                }
                // stash each input and request a notification when ready
                while let Some((time, data)) = input1.next() {
                    consume1(&mut notificator1, time, data, &mut output_handle);
                };
                while let Some((time, data)) = input2.next() {
                    consume2(&mut notificator2, time, data, &mut output_handle);
                };

                // go through each time with data
                while let Some((time, data)) = notificator1.next(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]]) {
                    fold1(time, data, &mut states1, &mut states2, &mut output_handle);
                }
                while let Some((time, data)) = notificator2.next(&[&frontiers[0], &frontiers[1], &frontiers[2], &frontiers[3]]) {
                    fold2(time, data, &mut states1, &mut states2, &mut output_handle);
                }

            }
        });
        stream.probe_with(&mut stateful1.probe).probe_with(&mut stateful2.probe)
    }

    fn distribute<B1>(&self, control: &Stream<G, Control>, key: B1, name: &str) -> Stream<G, (usize, Key, D1)>
        where
            B1: Fn(&D1)->u64+'static,
    {
        self.stateful_unary_input::<_, (), _, Vec<()>, _, _, _>(control, key, name, |_not, time, data, output| {
            output.session(&time).give_content(data);
        }, |_time, _data, _states, _output| {})
    }

}
