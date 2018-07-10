extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate streaming_harness;
extern crate dynamic_scaling_mechanism;
extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

use std::hash::Hash;
use std::hash::Hasher;
use std::collections::{HashMap, VecDeque};

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Filter, Probe, Capture, capture::Replay, FrontierNotificator};

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Capability;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::Input;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::stateful::{Stateful, StateHandle};
use dynamic_scaling_mechanism::{ControlInst, Control};

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}


fn verify<S: Scope, T: ExchangeData+Ord+::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    use timely::dataflow::channels::pact::Exchange;
    use std::collections::HashMap;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    correct.binary_notify(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data.drain(..));
                not.notify_at(time.retain());
            });
            not.for_each(|time, _, _| {
                let mut v1 = in1_pending.remove(time.time()).unwrap_or_default();
                let mut v2 = in2_pending.remove(time.time()).unwrap_or_default();
                v1.sort();
                v2.sort();
                assert_eq!(v1.len(), v2.len());
                let i1 = v1.iter();
                let i2 = v2.iter();
                for (a, b) in i1.zip(i2) {
//                    println!("a: {:?}, b: {:?}", a, b);
                    assert_eq!(a, b, " at {:?}", time.time());
                }
            })
        }
    )
}

#[derive(Debug, PartialEq, Eq)]
enum ExperimentMapMode {
    None,
    Sudden,
//    OneByOne,
//    Fluid,
    File(String),
}

fn main() {

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(std::env::args(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let timer = ::std::time::Instant::now();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Q0: Do nothing in particular.
        if std::env::args().any(|x| x == "q0") {
            worker.dataflow(|scope| {
                input.to_stream(scope)
                     .probe_with(&mut probe);
            });
        }

        // Q0-flex: Do nothing in particular.
        if std::env::args().any(|x| x == "q0-flex") {
            worker.dataflow(|scope| {
                let control = scope.input_from(&mut control_input).broadcast();
                let mut state_stream = input.to_stream(scope)
                                        .stateful::<_, HashMap<(), ()>, _, ()>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control);
                state_stream.stream
                        .probe_with(&mut state_stream.probe)
                        .probe_with(&mut probe);
            });
        }

        // Q1: Convert bids to euros.
        if std::env::args().any(|x| x == "q1") {
            worker.dataflow(|scope| {
                input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     .map_in_place(|b| b.price = (b.price * 89)/100)
                     .probe_with(&mut probe);
            });
        }

        // Q1-flex: Convert bids to euros.
        if std::env::args().any(|x| x == "q1-flex") {
            worker.dataflow(|scope| {
                let control = scope.input_from(&mut control_input).broadcast();
                let mut state_stream = input.to_stream(scope)
                                        .stateful::<_, HashMap<(), ()>, _, ()>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control);
                                        
                                                                             
                state_stream.stream
                     .flat_map(|(_,_,e)| nexmark::event::Bid::from(e))
                     .map_in_place(|b| b.price = (b.price * 89)/100)
                     .probe_with(&mut state_stream.probe)
                     .probe_with(&mut probe);
            });
        }

        // Q2: Filter some auctions.
        if std::env::args().any(|x| x == "q2") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     .filter(move |b| b.auction % auction_skip == 0)
                     .map(|b| (b.auction, b.price))
                     .probe_with(&mut probe);
            });
        }

        // Q2-flex: Filter some auctions.
        if std::env::args().any(|x| x == "q2-flex") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                let control = scope.input_from(&mut control_input).broadcast();
                let mut state_stream = input.to_stream(scope)
                                        .stateful::<_, HashMap<(), ()>, _, ()>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control);
                state_stream.stream
                     .flat_map(|(_,_,e)| nexmark::event::Bid::from(e))
                     .filter(move |b| b.auction % auction_skip == 0)
                     .map(|b| (b.auction, b.price))
                     .probe_with(&mut state_stream.probe)
                     .probe_with(&mut probe);
            });
        }

        // Q3: Join some auctions.
        if std::env::args().any(|x| x == "q3") {
            worker.dataflow(|scope| {

                let events = input.to_stream(scope);

                let auctions =
                events.flat_map(|e| nexmark::event::Auction::from(e))
                      .filter(|a| a.category == 10);

                let people =
                events.flat_map(|e| nexmark::event::Person::from(e))
                      .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

                auctions
                    .binary(
                        &people,
                        Exchange::new(|a: &nexmark::event::Auction| a.seller as u64),
                        Exchange::new(|p: &nexmark::event::Person| p.id as u64),
                        "Q3 Join",
                        |_capability, _info| {

                            let mut state1 = HashMap::new();
                            let mut state2 = HashMap::<usize, nexmark::event::Person>::new();

                            move |input1, input2, output| {

                                // Process each input auction.
                                input1.for_each(|time, data| {
                                    let mut session = output.session(&time);
                                    for auction in data.drain(..) {
                                        if let Some(person) = state2.get(&auction.seller) {
                                                session.give((
                                                    person.name.clone(),
                                                    person.city.clone(),
                                                    person.state.clone(),
                                                    auction.id));
                                        }
                                        state1.entry(auction.seller).or_insert(Vec::new()).push(auction);
                                    }
                                });

                                // Process each input person.
                                input2.for_each(|time, data| {
                                    let mut session = output.session(&time);
                                    for person in data.drain(..) {
                                        if let Some(auctions) = state1.get(&person.id) {
                                            for auction in auctions.iter() {
                                                session.give((
                                                    person.name.clone(),
                                                    person.city.clone(),
                                                    person.state.clone(),
                                                    auction.id));
                                            }
                                        }
                                        state2.insert(person.id, person);
                                    }
                                });
                            }
                        }
                    )
                    .probe_with(&mut probe);
            });
        }

        // Q3-flex: Join some auctions.
        if std::env::args().any(|x| x == "q3-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                let events = input.to_stream(scope);    

                let mut auctions =
                events.flat_map(|e| nexmark::event::Auction::from(e))
                      .filter(|a| a.category == 10)
                      .stateful::<_, HashMap<u64, nexmark::event::Auction>, _, _>(|a| calculate_hash(&a.seller), &control);

                let mut people =
                events.flat_map(|e| nexmark::event::Person::from(e))
                      .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA")
                      .stateful::<_, HashMap<u64, nexmark::event::Person>, _, _>(|p| calculate_hash(&p.id), &control);

                // The shared state for each input
                let auction_state = auctions.state.clone();
                let people_state = people.state.clone();

                auctions.stream.
                    binary_frontier(
                        &people.stream,
                        Pipeline,
                        Pipeline,
                        "Q3 Join Flex",
                        |_capability, _info| {

                            move |input1, input2, output| {

                                let mut auction_state = auction_state.borrow_mut();
                                let mut people_state = people_state.borrow_mut();

                                // Stash each input auction
                                input1.for_each(|time, data| {
                                    auction_state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });

                                // Stash each input person
                                input2.for_each(|time, data| {
                                    people_state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });

                                // Process input auctions
                                while let Some((time, data)) = auction_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                    let mut session = output.session(&time);
                                    for (bin_id, auction) in data {
                                        if let Some(mut person) = people_state.get_state(bin_id).get(&(auction.seller as u64)) {
                                            session.give((person.name.clone(), 
                                                        person.city.clone(),
                                                        person.state.clone(),
                                                        auction.id));
                                        }
                                        // Update auction state
                                        auction_state.get_state(bin_id).insert(auction.seller as u64, auction);
                                    };

                                }
                                // Process input people
                                while let Some((time, data)) = people_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                    let mut session = output.session(&time);
                                    for (bin_id, person) in data {
                                        if let Some(mut auction) = auction_state.get_state(bin_id).get(&(person.id as u64)) {
                                            session.give((person.name.clone(), 
                                                        person.city.clone(),
                                                        person.state.clone(),
                                                        auction.id));
                                        }
                                        // Update people state
                                        people_state.get_state(bin_id).insert(person.id as u64, person);
                                    };
                                }
                            }
                        }
                    )
                    .probe_with(&mut auctions.probe)
                    .probe_with(&mut people.probe)
                    .probe_with(&mut probe);
            
            });
        }

        // Intermission: Close some auctions.
        let closed_auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        if std::env::args().any(|x| x == "q4" || x == "q6") {
            worker.dataflow(|scope| {
                let events = input.to_stream(scope);

                let bids = events.flat_map(|e| nexmark::event::Bid::from(e));
                let auctions = events.flat_map(|e| nexmark::event::Auction::from(e));

                bids.binary_frontier(
                        &auctions,
                        Exchange::new(|b: &nexmark::event::Bid| b.auction as u64),
                        Exchange::new(|a: &nexmark::event::Auction| a.id as u64),
                        "Q4 Auction close",
                        |_capability, _info| {

                            let mut state = std::collections::HashMap::new();
                            let mut opens = std::collections::BinaryHeap::new();

                            let mut capability: Option<Capability<Product<RootTimestamp, usize>>> = None;

                            move |input1, input2, output| {

                                // Record each bid.
                                // NB: We don't summarize as the max, because we don't know which are valid.
                                input1.for_each(|_time, data| {
                                    for bid in data.drain(..) {
                                        state.entry(bid.auction).or_insert(Vec::new()).push(bid);
                                    }
                                });

                                // Record each auction.
                                input2.for_each(|time, data| {
                                    for auction in data.drain(..) {
                                        if capability.as_ref().map(|c| c.time().inner <= auction.expires) != Some(true) {
                                            let mut new_time = time.time().clone();
                                            new_time.inner = auction.expires;
                                            capability = Some(time.delayed(&new_time));
                                        }
                                        use std::cmp::Reverse;
                                        opens.push((Reverse(auction.expires), auction));
                                    }
                                });

                                // Use frontiers to determine which auctions to close.
                                if let Some(ref capability) = capability {

                                    let complete1 = input1.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                    let complete2 = input2.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                    let complete = std::cmp::min(complete1, complete2);

                                    let mut session = output.session(capability);
                                    while opens.peek().map(|x| (x.0).0 < complete) == Some(true) {

                                        let (_time, auction) = opens.pop().unwrap();
                                        if let Some(mut state) = state.remove(&auction.id) {
                                            state.retain(|b|
                                                auction.date_time <= b.date_time &&
                                                b.date_time < auction.expires &&
                                                b.price >= auction.reserve);
                                            state.sort_by(|b1,b2| b1.price.cmp(&b2.price));
                                            if let Some(winner) = state.pop() {
                                                session.give((auction, winner));
                                            }
                                        }
                                    }
                                }

                                // Downgrade capability.
                                if let Some(head) = opens.peek() {
                                    capability.as_mut().map(|c| c.downgrade(&RootTimestamp::new((head.0).0)));
                                }
                                else {
                                    capability = None;
                                }
                            }
                        }
                    )
                    .capture_into(closed_auctions.clone());
            });
        }

        // Intermission: Close some auctions (using stateful).
        let closed_auctions_flex = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        if std::env::args().any(|x| x == "q4-flex" || x == "q6-flex") {
            worker.dataflow(|scope| {
                let events = input.to_stream(scope);
                let control = scope.input_from(&mut control_input).broadcast();

                let mut bids = events.flat_map(|e| nexmark::event::Bid::from(e))
                                .stateful::<_,HashMap<usize, Vec<nexmark::event::Bid>>,_, _>(|bid| calculate_hash(&bid.auction), &control);

                let mut auctions = events.flat_map(|e| nexmark::event::Auction::from(e))
                                .stateful::<_,HashMap<(), ()>,_, _>(|a| calculate_hash(&a.id), &control);

                // The shared state for each input
                let bid_state = bids.state.clone();
                let auction_state = auctions.state.clone();

                bids.stream
                        .binary_frontier(
                        &auctions.stream,
                        Pipeline,
                        Pipeline,
                        "Q4 Auction close",
                        |_capability, _info| {

                            move |input1, input2, output| {

                                let mut bid_state = bid_state.borrow_mut();
                                let mut auction_state = auction_state.borrow_mut();

                                // Record each bid.
                                // NB: We don't summarize as the max, because we don't know which are valid.
                                input1.for_each(|time, data| {
                                    let mut not_time = time.time().clone();
                                    for (_, key_id, bid) in data.drain(..) {
                                        not_time.inner = bid.date_time;
                                        bid_state.notificator().notify_at(time.delayed(&not_time), vec![(key_id, bid)])
                                    }
                                });

                                // Record each auction.
                                input2.for_each(|time, data| {
                                    let mut new_time = time.time().clone();
                                    for (_target, key_id, auction) in data.drain(..) {
                                        new_time.inner = auction.expires;
                                        // Request notification for the auction's expiration time, which is used to look into the auctions_state
                                        auction_state.notificator().notify_at(time.delayed(&new_time), vec![(key_id, auction)]);
                                    }
                                });

                                // Process input bids
                                while let Some((_time, data)) = bid_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                    for (key_id, bid) in data {
                                        // Update bin state
                                        bid_state.get_state(key_id).entry(bid.auction).or_insert_with(Vec::new).push(bid);
                                    };
                                }
                                // Process input auctions
                                while let Some((time, auction_data)) = auction_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                    let mut session = output.session(&time);
                                    for (key_id, auction) in auction_data {
                                        if let Some(mut bids) = bid_state.get_state(key_id).remove(&auction.id) {
                                            bids.retain(|b|
                                                auction.date_time <= b.date_time &&
                                                    b.date_time < auction.expires &&
                                                    b.price >= auction.reserve);
                                            bids.sort_by(|b1,b2| b1.price.cmp(&b2.price));
                                            if let Some(winner) = bids.pop() {
                                                session.give((auction.clone(), winner));
                                            }
                                        }
                                    }
                                }
                            }
                        })
                    .probe_with(&mut bids.probe)
                    .probe_with(&mut auctions.probe)
                    .capture_into(closed_auctions_flex.clone());
            });
        }

        if std::env::args().any(|x| x == "q4") {
            worker.dataflow(|scope| {

                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;

                Some(closed_auctions.clone())
                    .replay_into(scope)
                    .map(|(a,b)| (a.category, b.price))
                    .unary(Exchange::new(|x: &(usize, usize)| x.0 as u64), "Q4 Average",
                        |_cap, _info| {

                            // Stores category -> (total, count)
                            let mut state = std::collections::HashMap::new();

                            move |input, output| {

                                input.for_each(|time, data| {
                                    let mut session = output.session(&time);
                                    for (category, price) in data.drain(..) {
                                        let entry = state.entry(category).or_insert((0, 0));
                                        entry.0 += price;
                                        entry.1 += 1;
                                        session.give((category, entry.0 / entry.1));
                                    }
                                })

                            }

                        })
                    .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q4-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                let mut closed_auctions_by_category = 
                    Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(a,b)| (a.category, b.price))
                    .stateful::<_, HashMap<_, _>, _, _>(|(a, _price)| calculate_hash(a), &control);

                let state = closed_auctions_by_category.state.clone();

                closed_auctions_by_category.stream
                    .unary_frontier(Pipeline, "Q4 Average",
                        |_cap, _info| {

                            move |input, output| {

                                let mut state = state.borrow_mut();

                                while let Some((time, data)) = state.notificator().next(&[input.frontier()]) {
                                    let mut session = output.session(&time);
                                    for (bin_id, (category, price)) in data {
                                        let entry = state.get_state(bin_id).entry(category).or_insert((0usize, 0));
                                        entry.0 += price;
                                        entry.1 += 1;
                                        session.give((category, entry.0 / entry.1));
                                    }
                                }
                                
                                input.for_each(|time, data| {
                                    state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });
                            }
                        })
                    .probe_with(&mut closed_auctions_by_category.probe)
                    .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q5") {
            worker.dataflow(|scope| {

                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;

                input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     .map(move |b| (b.auction, ((b.date_time / window_slide_ns) + 1) * window_slide_ns))
                     // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
                     .unary_frontier(Exchange::new(|b: &(usize, usize)| b.0 as u64), "Q5 Accumulate",
                        |_capability, _info| {

                            let mut additions = HashMap::new();
                            let mut deletions = HashMap::new();
                            let mut accumulations = HashMap::new();

                            move |input, output| {

                                input.for_each(|time, data| {

                                    let slide = ((time.time().inner / window_slide_ns) + 1) * window_slide_ns;
                                    let downgrade = time.delayed(&RootTimestamp::new(slide));

                                    // Collect all bids in a different slide.
                                    for &(auction, a_time) in data.iter() {
                                        if a_time != slide {
                                            additions
                                                .entry(time.delayed(&RootTimestamp::new(a_time)))
                                                .or_insert(Vec::new())
                                                .push(auction);
                                        }
                                    }
                                    data.retain(|&(_, a_time)| a_time == slide);

                                    // Collect all bids in the same slide.
                                    additions
                                        .entry(downgrade)
                                        .or_insert(Vec::new())
                                        .extend(data.drain(..).map(|(b,_)| b));
                                });

                                // Extract and order times we can now process.
                                let mut times = {
                                    let add_times = additions.keys().filter(|t| !input.frontier.less_equal(t.time())).cloned();
                                    let del_times = deletions.keys().filter(|t: &&Capability<Product<RootTimestamp, usize>>| !input.frontier.less_equal(t.time())).cloned();
                                    add_times.chain(del_times).collect::<Vec<_>>()
                                };
                                times.sort_by(|x,y| x.time().cmp(&y.time()));
                                times.dedup();

                                for time in times.drain(..) {
                                    if let Some(additions) = additions.remove(&time) {
                                        for &auction in additions.iter() {
                                            *accumulations.entry(auction).or_insert(0) += 1;
                                        }
                                        let new_time = time.time().inner + (window_slice_count * window_slide_ns);
                                        deletions.insert(time.delayed(&RootTimestamp::new(new_time)), additions);
                                    }
                                    if let Some(deletions) = deletions.remove(&time) {
                                        for auction in deletions.into_iter() {
                                            *accumulations.entry(auction).or_insert(0) -= 1;
                                        }
                                    }
                                    if let Some((_count, auction)) = accumulations.iter().map(|(&a,&c)| (c,a)).max() {
                                        output.session(&time).give(auction);
                                    }
                                }
                            }
                        })
                     .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q5-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;

                let mut bids = input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     // Discretize bid's datetime based on slides
                     .map(move |b| (b.auction, ((b.date_time / window_slide_ns) + 1) * window_slide_ns))
                     // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
                     // Partitions by auction id
                     .stateful::<_,HashMap<_, _>, _, _>(|(a, _b)| calculate_hash(a), &control);

                let bid_state = bids.state.clone();

                #[derive(Abomonation, Eq, PartialEq, Clone)]
                enum InsDel<T> { Ins(T), Del(T) };

                bids.stream
                     .unary_frontier(Pipeline, "Q5 Accumulate",
                        |_capability, _info| {

                            move |input, output| {

                                let mut bid_state = bid_state.borrow_mut();

                                input.for_each(|time, data| {
                                    for &(_, bin_id, (auction, a_time)) in data.iter() {
                                        // Stash pending additions
                                        bid_state.notificator().notify_at(time.delayed(&RootTimestamp::new(a_time)), vec![(bin_id, InsDel::Ins(auction))]);

                                        // Stash pending deletions
                                        let new_time = a_time + (window_slice_count * window_slide_ns);
                                        bid_state.notificator().notify_at(time.delayed(&RootTimestamp::new(new_time)),vec![(bin_id, InsDel::Del(auction))]);
                                    }
                                });

                                while let Some((time, data)) = bid_state.notificator().next(&[input.frontier()]) {
                                    // Process additions (if any)
                                    for (key_id, action) in data {
                                        match action {
                                            InsDel::Ins(auction) => {
                                                let slot = bid_state.get_state(key_id).entry(auction).or_insert(0);
                                                *slot += 1;
                                            },
                                            InsDel::Del(auction) => {
                                                let slot = bid_state.get_state(key_id).entry(auction).or_insert(0);
                                                *slot -= 1;
                                            }

                                        }
                                    }
                                    // Output results (if any)
                                    let mut session = output.session(&time);
                                    bid_state.scan(move |a| { 
                                        if let Some((auction, _count)) = a.iter().max_by_key(|(_auction_id, count)| *count) {
                                             session.give(*auction);
                                        }
                                    });
                                }
                            }
                        })
                     .probe_with(&mut bids.probe)
                     .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q6") {
            worker.dataflow(|scope| {

                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;

                Some(closed_auctions.clone())
                    .replay_into(scope)
                    .map(|(_a, b)| (b.bidder, b.price))
                    .unary(Exchange::new(|x: &(usize, usize)| x.0 as u64), "Q6 Average",
                        |_cap, _info| {

                            // Store bidder -> [prices; 10]
                            let mut state = std::collections::HashMap::new();

                            move |input, output| {

                                input.for_each(|time, data| {
                                    let mut session = output.session(&time);
                                    for (bidder, price) in data.drain(..) {
                                        let entry = state.entry(bidder).or_insert(VecDeque::new());
                                        if entry.len() >= 10 { entry.pop_back(); }
                                        entry.push_front(price);
                                        let mut sum: usize = entry.iter().sum();
                                        session.give((bidder, sum / entry.len()));
                                    }
                                });
                            }
                        })
                    .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q6-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                let mut winners =  Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(_a, b)| (b.bidder, b.price))
                    .stateful::<_, HashMap<_, _>, _, _>(|(b,_p)| calculate_hash(b), &control);

                let state = winners.state.clone();

                winners.stream
                    .unary_frontier(Pipeline, "Q6 Average",
                        |_cap, _info| {

                            move |input, output| {

                                let mut state = state.borrow_mut();

                                input.for_each(|time, data| {
                                    state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });

                                while let Some((time, data)) = state.notificator().next(&[input.frontier()]) {
                                    let mut session = output.session(&time);
                                    for (bin_id, (bidder, price)) in data {
                                        let entry = state.get_state(bin_id).entry(bidder).or_insert(::nexmark::AbomVecDeque(VecDeque::new()));
                                        if entry.len() >= 10 { entry.pop_back(); }
                                        entry.push_front(price);
                                        let mut sum: usize = entry.iter().sum();
                                        session.give((bidder, sum / entry.len()));
                                    }
                                }
                            }
                        })
                    .probe_with(&mut winners.probe)
                    .probe_with(&mut probe);
            });
        }


        if std::env::args().any(|x| x == "q7") {
            worker.dataflow(|scope| {

                use timely::dataflow::channels::pact::{Pipeline, Exchange};
                use timely::dataflow::operators::Operator;

                // Window ticks every 10 seconds.
                let window_size_ns = 1_000_000_000;

                input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     .map(move |b| (((b.date_time / window_size_ns) + 1) * window_size_ns, b.price))
                     .unary_frontier(Pipeline, "Q7 Pre-reduce", |_cap, _info| {

                        use timely::dataflow::operators::Capability;
                        use timely::progress::nested::product::Product;
                        use timely::progress::timestamp::RootTimestamp;

                        // Tracks the worker-local maximal bid for each capability.
                        let mut maxima = Vec::<(Capability<Product<RootTimestamp, usize>>, usize)>::new();

                        move |input, output| {

                            input.for_each(|time, data| {

                                for (window, price) in data.drain(..) {
                                    if let Some(position) = maxima.iter().position(|x| (x.0).time().inner == window) {
                                        if maxima[position].1 < price {
                                            maxima[position].1 = price;
                                        }
                                    }
                                    else {
                                        maxima.push((time.delayed(&RootTimestamp::new(window)), price));
                                    }
                                }

                            });

                            for &(ref capability, price) in maxima.iter() {
                                if !input.frontier.less_than(capability.time()) {
                                    output.session(&capability).give((capability.time().inner, price));
                                }
                            }

                            maxima.retain(|(capability, _)| input.frontier.less_than(capability));

                        }
                     })
                     .unary_frontier(Exchange::new(move |x: &(usize, usize)| (x.0 / window_size_ns) as u64), "Q7 All-reduce", |_cap, _info| {

                    use timely::dataflow::operators::Capability;
                    use timely::progress::nested::product::Product;
                    use timely::progress::timestamp::RootTimestamp;

                    // Tracks the global maximal bid for each capability.
                    let mut maxima = Vec::<(Capability<Product<RootTimestamp, usize>>, usize)>::new();

                    move |input, output| {

                        input.for_each(|time, data| {

                            for (window, price) in data.drain(..) {
                                if let Some(position) = maxima.iter().position(|x| (x.0).time().inner == window) {
                                    if maxima[position].1 < price {
                                        maxima[position].1 = price;
                                    }
                                }
                                else {
                                    maxima.push((time.delayed(&RootTimestamp::new(window)), price));
                                }
                            }

                        });

                        for &(ref capability, price) in maxima.iter() {
                            if !input.frontier.less_than(capability.time()) {
                                output.session(&capability).give(price);
                            }
                        }

                        maxima.retain(|(capability, _)| input.frontier.less_than(capability));

                    }
                 })
                 .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q7-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                // Window ticks every 10 seconds.
                let window_size_ns = 1_000_000_000;

                let mut bids = input.to_stream(scope)
                     .flat_map(|e| nexmark::event::Bid::from(e))
                     .map(move |b| (b.auction, ((b.date_time / window_size_ns) + 1) * window_size_ns, b.price))
                     // Partition by auction id to avoid serializing the computation
                     .stateful::<_, HashMap<_, _>, _, _>(|(a, _window, _price)| calculate_hash(a), &control);

                let bid_state = bids.state.clone();

                bids.stream
                     .unary_frontier(Pipeline, "Q7 Pre-reduce", |_cap, _info| {

                        move |input, output| {

                            let mut bid_state = bid_state.borrow_mut();

                            input.for_each(|time, data| {
                                for (_, key_id, (_auction, window, price)) in data.drain(..) {
                                    bid_state.notificator().notify_at(time.delayed(&RootTimestamp::new(window)),vec![(key_id, (window, price))]);
                                } 
                            });

                            while let Some((time, maxima)) = bid_state.notificator().next(&[input.frontier()]) {
                                let mut windows = HashMap::new();
                                for (key_id, (window, price)) in maxima {
                                    let open_windows = bid_state.get_state(key_id).entry(window).or_insert(0);
                                    if *open_windows < price {
                                        *open_windows = price;
                                        windows.insert(window, price);
                                    }
                                }
                                let mut session = output.session(&time);
                                session.give_iterator(windows.drain());
                            }
                        }
                     })
                     // Aggregate the partial counts. This doesn't need to be stateful since we request notification upon a window firing time and then we drop the state immediately after processing
                     .unary_frontier(Exchange::new(move |x: &(usize, usize)| (x.0 / window_size_ns) as u64), "Q7 All-reduce", |_cap, _info| 
                     {
                        let mut pending_maxima: HashMap<_,Vec<_>> = Default::default();
                        let mut notificator = FrontierNotificator::new();
                        move |input, output| {
                            input.for_each(|time, data| {
                                for (window,price) in data.drain(..) {
                                    let slot = pending_maxima.entry(window).or_insert_with(Vec::new);
                                    slot.push(price);
                                    notificator.notify_at(time.delayed(&RootTimestamp::new(window)));
                                }
                            });
                            while let Some(time) = notificator.next(&[input.frontier()]) {
                                if let Some(mut maxima) = pending_maxima.remove(&time.time().inner) {
                                    if let Some(max_price) = maxima.drain(..).max(){
                                        output.session(&time).give(max_price);
                                    }
                                }
                            }
                        }

                     })
                     .probe_with(&mut bids.probe)
                     .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q8") {
            worker.dataflow(|scope| {

                let events = input.to_stream(scope);

                let auctions =
                events.flat_map(|e| nexmark::event::Auction::from(e))
                      .map(|a| (a.seller, a.date_time));

                let people =
                events.flat_map(|e| nexmark::event::Person::from(e))
                      .map(|p| (p.id, p.date_time));

                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;


                people
                    .binary_frontier(
                        &auctions,
                        Exchange::new(|p: &(usize, usize)| p.0 as u64),
                        Exchange::new(|a: &(usize, usize)| a.0 as u64),
                        "Q8 join",
                        |_capability, _info| {

                            let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                            let mut new_people = std::collections::HashMap::new();
                            let mut auctions = Vec::new();

                            move |input1, input2, output| {

                                // Notice new people.
                                input1.for_each(|_time, data| {
                                    for (person, time) in data.drain(..) {
                                        new_people.insert(person, time);
                                    }
                                });

                                // Notice new auctions.
                                input2.for_each(|time, data| {
                                    auctions.push((time.retain(), data.take()));
                                });

                                // Determine least timestamp we might still see.
                                let complete1 = input1.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                let complete2 = input2.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                let complete = std::cmp::min(complete1, complete2);

                                for (capability, auctions) in auctions.iter_mut() {
                                    if capability.time().inner < complete {
                                        let mut session = output.session(&capability);
                                        for &(person, time) in auctions.iter() {
                                            if time < complete {
                                                if let Some(p_time) = new_people.get(&person) {
                                                    if (time - p_time) < window_size_ns {
                                                        session.give(person);
                                                    }
                                                }
                                            }
                                        }
                                        auctions.retain(|&(_, time)| time >= complete);
                                    }
                                }
                                auctions.retain(|&(_, ref list)| !list.is_empty());
                                // println!("auctions.len: {:?}", auctions.len());
                                // for thing in auctions.iter() {
                                //     println!("\t{:?} (len: {:?}) vs {:?}", thing.0, thing.1.len(), complete);
                                // }
                            }
                        })
                    .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q8-flex") {
            worker.dataflow(|scope| {

                let control = scope.input_from(&mut control_input).broadcast();

                let events = input.to_stream(scope);

                let mut auctions =
                events.flat_map(|e| nexmark::event::Auction::from(e))
                      .map(|a| (a.seller, a.date_time))
                      .stateful::<_, HashMap<(), ()>, _, _>(|(s,_d)| calculate_hash(s), &control);

                let mut people =
                events.flat_map(|e| nexmark::event::Person::from(e))
                      .map(|p| (p.id, p.date_time))
                      .stateful::<_, HashMap<_, _>, _, _>(|(p,_d)| calculate_hash(p), &control);
                
                let auctions_state = auctions.state.clone();
                let people_state = people.state.clone();

                people.stream
                    .binary_frontier(
                        &auctions.stream,
                        Pipeline,
                        Pipeline,
                        "Q8 join",
                        |_capability, _info| {

                            let window_size_ns = 12 * 60 * 60 * 1_000_000_000;

                            move |input1, input2, output| {

                                let mut auctions_state = auctions_state.borrow_mut();
                                let mut people_state = people_state.borrow_mut();

                                // Notice new people.
                                input1.for_each(|time, data| {
                                    people_state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });

                                // Notice new auctions.
                                input2.for_each(|time, data| {
                                    auctions_state.notificator().notify_at(time.retain(), data.drain(..).map(|(_, key_id, d)| (key_id, d)).collect());
                                });

                                while let Some((_time, data)) = people_state.notificator().next(&[input1.frontier(),input2.frontier()]) {
                                    // Update people state
                                    for (bin_id, (person, date)) in data {
                                        people_state.get_state(bin_id).entry(person as u64).or_insert(date);
                                    }
                                }

                                while let Some((time, data)) = auctions_state.notificator().next(&[input1.frontier(),input2.frontier()]) {
                                    for (bin_id, (seller, date)) in data {
                                        if let Some(p_time) = people_state.get_state(bin_id).get(&(seller as u64)) {
                                            if (date - p_time) < window_size_ns {
                                                output.session(&time).give(seller);
                                            }          
                                        } 
                                    }
                                }
                            }
                    })
                    .probe_with(&mut people.probe)
                    .probe_with(&mut auctions.probe)
                    .probe_with(&mut probe);
            });
        }

        let rate = std::env::args().nth(1).expect("rate absent").parse().expect("couldn't parse rate");
        let mut config1 = nexmark::config::Config::new();
        config1.insert("events-per-second", rate);
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        let duration_ns: u64 = std::env::args().nth(2).expect("duration absent").parse::<u64>().expect("couldn't parse duration") * 1_000_000_000;

        let map_mode = match std::env::args().nth(3).expect("migration file absent").as_str() {
            "none" => ExperimentMapMode::None,
            "sudden" => ExperimentMapMode::Sudden,
//            "one-by-one" => ExperimentMapMode::OneByOne,
//            "fluid" => ExperimentMapMode::Fluid,
            file_name => ExperimentMapMode::File(file_name.to_string()),
        };

        let mut instructions: Vec<(u64, Vec<ControlInst>)> = match map_mode {
            ExperimentMapMode::None => {
                let mut map = vec![0; 1 << ::dynamic_scaling_mechanism::BIN_SHIFT];
                for i in 0..map.len() {
                    map[i] = i % peers;
                };
                vec![(0, vec![ControlInst::Map(map)])]
            }
            ExperimentMapMode::Sudden => {
                let mut map = vec![0; 1 << ::dynamic_scaling_mechanism::BIN_SHIFT];
                // TODO(moritzo) HAAAACCCCKKK
                if peers != 2 {
                    for (i, v) in map.iter_mut().enumerate() {
                        *v = ((i / 2) * 2 + (i % 2) * peers / 2) % peers;
                    }
                }
                let initial_map = map.clone();
                for i in 0..map.len() {
                    map[i] = i % peers;

//                    if i % batches_per_migration == batches_per_migration - 1 {
//                        eprintln!("debug: setting up reconfiguration: {:?}", map);
//                        control_plan.push((rounds * 1_000_000_000, Control::new(control_counter, 1, ControlInst::Map(map.clone()))));
//                        control_counter += 1;
//                    }
                };
                vec![(0, vec![ControlInst::Map(initial_map)]), (duration_ns/2, vec![ControlInst::Map(map)])]
            },
            ExperimentMapMode::File(migrations_file) => {
                let f = ::std::fs::File::open(migrations_file).unwrap();
                let file = ::std::io::BufReader::new(&f);
                use ::std::io::BufRead;
                let mut instructions = Vec::new();
                let mut ts = 0;
                for line in file.lines() {
                    let line = line.unwrap();
                    let mut parts = line.split_whitespace();
                    let instr = match parts.next().expect("Missing map/diff indicator") {
                        "M" => (ts, vec![ControlInst::Map(parts.map(|x| x.parse().unwrap()).collect())]),
                        "D" => {
                            let parts: Vec<usize> = parts.map(|x| x.parse().unwrap()).collect();
                            let inst = parts.chunks(2).map(|x|ControlInst::Move(::dynamic_scaling_mechanism::Bin(x[0]), x[1])).collect();
                            (ts, inst)
                        },
                        _ => panic!("Incorrect input found in map file"),
                    };
                    instructions.push(instr);
                    ts = duration_ns / 2;
                }
                instructions
            },
//            _ => panic!("unsupported map mode"),
        };

        for instruction in &instructions {
            eprintln!("instructions\t{:?}", instruction);
        }

        // Establish a start of the computation.
        let elapsed_ns = timer.elapsed().to_nanos();
        config.base_time_ns = elapsed_ns as usize;

        use rand::{StdRng, SeedableRng};
        let mut rng = StdRng::from_seed([0;32]);

        let input_times = {
            let config = config.clone();
            move || nexmark::config::NexMarkInputTimes::new(config.clone(), duration_ns)
        };

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 2_000_000_000, duration_ns - 2_000_000_000, duration_ns,
                1_000_000_000);

        let mut events_so_far = 0;

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let mut input = Some(input);

        let control_sequence = 0;
        let mut control_input = Some(control_input);
        if index != 0 {
            control_input.take().unwrap().close();
        }

        loop {
            let elapsed_ns = timer.elapsed().to_nanos();

            if index == 0 {
                if instructions.get(0).map(|&(ts, _)| ts < elapsed_ns).unwrap_or(false) {
                    let instructions = instructions.remove(0).1;
                    let count = instructions.len();
                    for instruction in instructions {
                        control_input.as_mut().unwrap().send(Control::new(control_sequence, count, instruction));
                    }
                }
            }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&RootTimestamp::new(t as usize))
                });

            if input.is_none() {
                break;
            }

            let target_ns = (elapsed_ns + 1) / 1_000_000 * 1_000_000;
            if let Some(it) = input_times_gen.iter_until(target_ns) {
                let mut input = input.as_mut().unwrap();
                for _t in it {
                    input.send(
                        nexmark::event::Event::create(
                            events_so_far,
                            &mut rng,
                            &mut config));
                    events_so_far += 1;
                }
                input.advance_to(target_ns as usize);
                if index == 0 {
                    control_input.as_mut().unwrap().advance_to(target_ns as usize);
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if let Some(input) = input.as_ref() {
                while probe.less_than(input.time()) { worker.step(); }
            } else {
                while worker.step() { }
            }
        }

        output_metric_collector.into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    let ::streaming_harness::timeline::Timeline { timeline, latency_metrics, .. } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();
    eprintln!("== summary ==\n{}", latency_metrics.summary_string());
    eprintln!("== timeline ==\n{}",
              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
                    format!("-- {} ({} samples) --\n{}", time, samples, metrics.summary_string())).collect::<Vec<_>>().join("\n"));

    for (value, prob, count) in latency_metrics.ccdf() {
        println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    }
    println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
}
