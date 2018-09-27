extern crate clap;
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

use clap::{Arg, App};

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Filter, Probe, Capture, capture::Replay, FrontierNotificator};

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Capability;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::Bin;
use dynamic_scaling_mechanism::{ControlInst, Control};
use dynamic_scaling_mechanism::operator::StatefulOperator;

use nexmark::event::{Event, Auction, Bid, Person};
use nexmark::tools::ExperimentMapMode;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

#[allow(dead_code)]
fn verify<S: Scope, T: ExchangeData+Ord+::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    use timely::dataflow::channels::pact::Exchange;
    use std::collections::HashMap;
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
        move |in1, in2, _out, not| {
            in1.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                not.notify_at(time.retain());
            });
            in2.for_each(|time, data| {
                data.swap(&mut data_buffer);
                in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
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

fn main() {

    let matches = App::new("word_count")
        .arg(Arg::with_name("rate").long("rate").takes_value(true).required(true))
        .arg(Arg::with_name("duration").long("duration").takes_value(true).required(true))
        .arg(Arg::with_name("migration").long("migration").takes_value(true).required(true))
        .arg(Arg::with_name("time_dilation").long("time_dilation").takes_value(true).required(false))
        .arg(Arg::with_name("queries").long("queries").takes_value(true).required(true).multiple(true).value_delimiter(" "))
        .arg(Arg::with_name("timely").multiple(true))
        .get_matches();
    let timely_args = matches.values_of("timely").map_or(Vec::new(), |vs| vs.map(String::from).collect());


    let rate: u64 = matches.value_of("rate").expect("rate absent").parse::<u64>().expect("couldn't parse rate");

    let duration_ns: u64 = matches.value_of("duration").expect("duration absent").parse::<u64>().expect("couldn't parse duration") * 1_000_000_000;

    let map_mode: ExperimentMapMode = matches.value_of("migration").expect("migration file absent").parse().unwrap();

    let time_dilation = matches.value_of("time_dilation").map_or(1, |arg| arg.parse().unwrap_or(1));

    let queries: Vec<_> = matches.values_of("queries").unwrap().map(String::from).collect();

    // Read and report RSS every 100ms
    let statm_reporter_running = nexmark::tools::statm_reporter();

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(timely_args.into_iter(), move |worker| {

        let to_nexmark_time = move |x: usize| {
            debug_assert!(x.checked_mul(time_dilation).is_some(), "multiplication failed: {} * {}", x, time_dilation);
            ::nexmark::event::Date::new(x * time_dilation)
        };
        let from_nexmark_time = move |x: ::nexmark::event::Date| *x / time_dilation;

        let peers = worker.peers();
        let index = worker.index();

        let timer = ::std::time::Instant::now();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let bids = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        let auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        let people = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());

        let control = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());

        worker.dataflow(|scope| {
            use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
            let mut demux = OperatorBuilder::new("NEXMark demux".to_string(), scope.clone());

            let mut input = demux.new_input(&input.to_stream(scope), Pipeline);

            let (mut b_out, bids_stream) = demux.new_output();
            let (mut a_out, auctions_stream) = demux.new_output();
            let (mut p_out, people_stream) = demux.new_output();

            let mut demux_buffer = Vec::new();

            demux.build(move |_capability| {

                move |_frontiers| {

                    let mut b_out = b_out.activate();
                    let mut a_out = a_out.activate();
                    let mut p_out = p_out.activate();

                    input.for_each(|time, data| {
                        data.swap(&mut demux_buffer);
                        let mut b_session = b_out.session(&time);
                        let mut a_session = a_out.session(&time);
                        let mut p_session = p_out.session(&time);

                        for datum in demux_buffer.drain(..) {
                            match datum {
                                nexmark::event::Event::Bid(b) => { b_session.give(b) },
                                nexmark::event::Event::Auction(a) => { a_session.give(a) },
                                nexmark::event::Event::Person(p) =>  { p_session.give(p) },
                            }
                        }
                    });

                }

            });

            bids_stream.capture_into(bids.clone());
            auctions_stream.capture_into(auctions.clone());
            people_stream.capture_into(people.clone());
            control_input.to_stream(scope).broadcast().capture_into(control.clone());
        });


        // Q0: Do nothing in particular.
        if queries.iter().any(|x| *x == "q0") {
            worker.dataflow(|scope| {
                input.to_stream(scope)
                     .probe_with(&mut probe);
            });
        }

        // Q0-flex: Do nothing in particular.
        if queries.iter().any(|x| *x == "q0-flex") {
            worker.dataflow(|scope| {
                let control = Some(control.clone()).replay_into(scope);
                input.to_stream(scope)
                    .distribute(&control, |e| calculate_hash(&e.id()), "q0-flex")
                    .probe_with(&mut probe);
            });
        }

        // Q1: Convert bids to euros.
        if queries.iter().any(|x| *x == "q1") {
            worker.dataflow(|scope| {
                Some(bids.clone()).replay_into(scope)
                     .map_in_place(|b| b.price = (b.price * 89)/100)
                     .probe_with(&mut probe);
            });
        }

        // Q1-flex: Convert bids to euros.
        if queries.iter().any(|x| *x == "q1-flex") {
            worker.dataflow(|scope| {
                let control = Some(control.clone()).replay_into(scope);
                let state_stream = Some(bids.clone())
                    .replay_into(scope)
                    .distribute(&control, |bid| calculate_hash(&bid.auction), "q0-flex")
                    .map_in_place(|(_, _, b)| b.price = (b.price * 89)/100);
                state_stream.probe_with(&mut probe);
            });
        }

        // Q2: Filter some auctions.
        if queries.iter().any(|x| *x == "q2") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                Some(bids.clone()).replay_into(scope)
                     .filter(move |b| b.auction % auction_skip == 0)
                     .map(|b| (b.auction, b.price))
                     .probe_with(&mut probe);
            });
        }

        // Q2-flex: Filter some auctions.
        if queries.iter().any(|x| *x == "q2-flex") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                let control = Some(control.clone()).replay_into(scope);
                let state_stream = Some(bids.clone()).replay_into(scope)
                    .distribute(&control, |bid| calculate_hash(&bid.auction), "q2-flex");
                state_stream
                     .filter(move |(_, _, b)| b.auction % auction_skip == 0)
                     .map(|(_, _, b)| (b.auction, b.price))
                     .probe_with(&mut probe);
            });
        }

        // Q3: Join some auctions.
        if queries.iter().any(|x| *x == "q3") {
            worker.dataflow(|scope| {

                let auctions = Some(auctions.clone()).replay_into(scope)
                    .filter(|a| a.category == 10);

                let people = Some(people.clone()).replay_into(scope)
                    .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

                let mut auctions_buffer = vec![];
                let mut people_buffer = vec![];

                auctions
                    .binary(
                        &people,
                        Exchange::new(|a: &Auction| a.seller as u64),
                        Exchange::new(|p: &Person| p.id as u64),
                        "Q3 Join",
                        |_capability, _info| {

                            let mut state1 = HashMap::new();
                            let mut state2 = HashMap::<usize, Person>::new();

                            move |input1, input2, output| {

                                // Process each input auction.
                                input1.for_each(|time, data| {
                                    data.swap(&mut auctions_buffer);
                                    let mut session = output.session(&time);
                                    for auction in auctions_buffer.drain(..) {
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
                                    data.swap(&mut people_buffer);
                                    let mut session = output.session(&time);
                                    for person in people_buffer.drain(..) {
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
        if queries.iter().any(|x| *x == "q3-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                let auctions = Some(auctions.clone()).replay_into(scope)
                    .filter(|a| a.category == 10);

                let people = Some(people.clone()).replay_into(scope)
                      .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

                auctions.stateful_binary(&control, &people, |a| calculate_hash(&a.seller), |p| calculate_hash(&p.id), "q3-flex join", |time, data, auction_bin, people_bin, output| {
                    let mut session = output.session(&time);
                    let people_state: &mut HashMap<_, Person> = people_bin.state();
                    for auction in data {
                        if let Some(mut person) = people_state.get(&auction.seller) {
                            session.give((person.name.clone(),
                                          person.city.clone(),
                                          person.state.clone(),
                                          auction.id));
                        }
                        // Update auction state
                        let bin: &mut HashMap<_, Auction> = auction_bin.state();
                        bin.insert(auction.seller, auction.clone());
                    };
                }, |time, data, auction_bin, people_bin, output| {
                    let mut session = output.session(&time);
                    let auction_state = auction_bin.state();
                    for person in data {
                        if let Some(mut auction) = auction_state.get(&person.id) {
                            session.give((person.name.clone(),
                                          person.city.clone(),
                                          person.state.clone(),
                                          auction.id));
                        }
                        // Update people state
                        people_bin.state().insert(person.id, person.clone());
                    };
                }).probe_with(&mut probe);

            });
        }

        // Intermission: Close some auctions.
        let closed_auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        if queries.iter().any(|x| *x == "q4" || *x == "q6") {
            worker.dataflow(|scope| {
                let bids = Some(bids.clone()).replay_into(scope);
                let auctions = Some(auctions.clone()).replay_into(scope);

                bids.binary_frontier(
                        &auctions,
                        Exchange::new(|b: &Bid| b.auction as u64),
                        Exchange::new(|a: &Auction| a.id as u64),
                        "Q4 Auction close",
                        |_capability, _info| {

                            let mut state: HashMap<_, (Option<_>, Vec<Bid>)> = std::collections::HashMap::new();
                            let mut opens = std::collections::BinaryHeap::new();

                            let mut capability: Option<Capability<Product<RootTimestamp, usize>>> = None;
                            use std::collections::hash_map::Entry;
                            use std::cmp::Reverse;

                            fn is_valid_bid(bid: &Bid, auction: &Auction) -> bool {
                                bid.price >= auction.reserve && auction.date_time <= bid.date_time && bid.date_time < auction.expires
                            }

                            move |input1, input2, output| {

                                // Record each bid.
                                // NB: We don't summarize as the max, because we don't know which are valid.
                                input1.for_each(|time, data| {
                                    for bid in data.iter().cloned() {
//                                        eprintln!("[{:?}] bid: {:?}", time.time().inner, bid);
                                        let mut entry = state.entry(bid.auction).or_insert((None, Vec::new()));
                                        if let Some(ref auction) = entry.0 {
                                            debug_assert!(entry.1.len() <= 1);
                                            if is_valid_bid(&bid, auction) {
                                                // bid must fall between auction creation and expiration
                                                if let Some(existing) = entry.1.get(0).cloned() {
                                                    if existing.price < bid.price {
                                                        entry.1[0] = bid;
                                                    }
                                                } else {
                                                    entry.1.push(bid);
                                                }
                                            }
                                        } else {
                                            opens.push((Reverse(bid.date_time), bid.auction));
                                            if capability.as_ref().map(|c| to_nexmark_time(c.time().inner) <= bid.date_time) != Some(true) {
                                                let mut new_time = time.time().clone();
                                                new_time.inner = from_nexmark_time(bid.date_time);
                                                capability = Some(time.delayed(&new_time));
                                            }
                                            entry.1.push(bid);
                                        }
                                    }
                                });

                                // Record each auction.
                                input2.for_each(|time, data| {
                                    for auction in data.iter().cloned() {
//                                        eprintln!("[{:?}] auction: {:?}", time.time().inner, auction);
                                        if capability.as_ref().map(|c| to_nexmark_time(c.time().inner) <= auction.expires) != Some(true) {
                                            let mut new_time = time.time().clone();
                                            new_time.inner = from_nexmark_time(auction.expires);
                                            capability = Some(time.delayed(&new_time));
                                        }
                                        opens.push((Reverse(auction.expires), auction.id));
                                        let mut entry = state.entry(auction.id).or_insert((None, Vec::new()));
                                        debug_assert!(entry.0.is_none());
                                        entry.1.retain(|bid| is_valid_bid(&bid, &auction));
                                        if let Some(bid) = entry.1.iter().max_by_key(|bid| bid.price).cloned() {
                                            entry.1.clear();
                                            entry.1.push(bid);
                                        }
                                        entry.0 = Some(auction);
                                    }
                                });

                                // Use frontiers to determine which auctions to close.
                                if let Some(ref capability) = capability {

                                    let complete1 = input1.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                    let complete2 = input2.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                    let complete = std::cmp::min(complete1, complete2);

                                    let mut session = output.session(capability);
                                    while opens.peek().map(|x| complete == usize::max_value() || (x.0).0 < to_nexmark_time(complete)) == Some(true) {
//                                        eprintln!("[{:?}] opens.len(): {} state.len(): {} {:?}", capability.time().inner, opens.len(), state.len(), state.iter().map(|x| (x.1).1.len()).sum::<usize>());

                                        let (Reverse(time), auction) = opens.pop().unwrap();
                                        let mut entry = state.entry(auction);
                                        if let Entry::Occupied(mut entry) = entry {
                                            let delete = {
                                                let auction_bids = entry.get_mut();
                                                if let Some(ref auction) = auction_bids.0 {
                                                    if time == auction.expires {
                                                        // Auction expired, clean up state
                                                        if let Some(winner) = auction_bids.1.pop() {
                                                            session.give((auction.clone(), winner));
                                                        }
                                                        true
                                                    } else {
                                                        false
                                                    }
                                                } else {
                                                    auction_bids.1.retain(|bid| bid.date_time > time);
                                                    auction_bids.1.is_empty()
                                                }
                                            };
                                            if delete {
                                                entry.remove_entry();
                                            }
                                        }
                                    }
                                }

                                // Downgrade capability.
                                if let Some(head) = opens.peek() {
                                    capability.as_mut().map(|c| c.downgrade(&RootTimestamp::new(from_nexmark_time((head.0).0))));
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
        if queries.iter().any(|x| *x == "q4-flex" || *x == "q6-flex") {
            worker.dataflow(|scope| {
                let control = Some(control.clone()).replay_into(scope);

                let bids = Some(bids.clone()).replay_into(scope);
                let auctions = Some(auctions.clone()).replay_into(scope);

                bids.stateful_binary_input(&control,
                        &auctions,
                        |bid| calculate_hash(&bid.auction),
                        |a| calculate_hash(&a.id),
                        "Q4 Auction close",
                        move |state, time, data, _output| {
                            let mut not_time = time.time().clone();
                            for (_, key_id, bid) in &*data {
                                not_time.inner = from_nexmark_time(bid.date_time);
                                state.get(*key_id).notificator().notify_at_data(time.delayed(&not_time), Some( bid.clone()));
                            }
                        },
                        move |state, time, data, _output| {
                            let mut new_time = time.time().clone();
                            for (_target, key_id, auction) in &*data {
                                new_time.inner = from_nexmark_time(auction.expires);
                                // Request notification for the auction's expiration time, which is used to look into the auctions_state
                                state.get(*key_id).notificator().notify_at_data(time.delayed(&new_time), Some(auction.clone()));
                            }
                        },
                        |_time, data, bid_bin, _auction_bin, _output| {
                            let state: &mut HashMap<_, _> = bid_bin.state();
                            for bid in data {
                                // Update bin state
                                state.entry(bid.auction).or_insert_with(Vec::new).push(bid.clone());
                            };
                        },
                        |time, auction_data, bid_bin, _auction_bin: &mut Bin<_, Vec<()>, _>, output| {
                            let mut session = output.session(&time);
                            let bid_state = bid_bin.state();
                            for auction in auction_data {
                                if let Some(mut bids) = bid_state.remove(&auction.id) {
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
                        },
                    )
                    .capture_into(closed_auctions_flex.clone());
            });
        }

        if queries.iter().any(|x| *x == "q4") {
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
                                    for (category, price) in data.iter().cloned() {
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

        if queries.iter().any(|x| *x == "q4-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(a,b)| (a.category, b.price))
                    .stateful_unary(&control, |x: &(usize, usize)| calculate_hash(&x.0), "Q4 Average",
                        |time, data, bin, output| {
                            let mut session = output.session(&time);
                            let state: &mut HashMap<_, _> = bin.state();
                            for (category, price) in data.iter() {
                                let entry = state.entry(*category).or_insert((0usize, 0usize));
                                entry.0 += *price;
                                entry.1 += 1;
                                session.give((*category, entry.0 / entry.1));
                            }
                        })
                    .probe_with(&mut probe);
            });
        }

        if queries.iter().any(|x| *x == "q5") {
            worker.dataflow(|scope| {

                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;

                Some(bids.clone()).replay_into(scope)
                     .map(move |b| (b.auction, ::nexmark::event::Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns)))
                     // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
                     .unary_frontier(Exchange::new(|b: &(usize, _)| b.0 as u64), "Q5 Accumulate",
                        |_capability, _info| {

                            let mut additions = HashMap::new();
                            let mut deletions = HashMap::new();
                            let mut accumulations = HashMap::new();

                            let mut bids_buffer = vec![];

                            move |input, output| {

                                input.for_each(|time, data| {
                                    data.swap(&mut bids_buffer);
                                    let slide = ::nexmark::event::Date::new(((*to_nexmark_time(time.time().inner) / window_slide_ns) + 1) * window_slide_ns);
                                    let downgrade = time.delayed(&RootTimestamp::new(from_nexmark_time(slide)));

                                    // Collect all bids in a different slide.
                                    for &(auction, a_time) in bids_buffer.iter() {
                                        if a_time != slide {
                                            additions
                                                .entry(time.delayed(&RootTimestamp::new(from_nexmark_time(a_time))))
                                                .or_insert(Vec::new())
                                                .push(auction);
                                        }
                                    }
                                    bids_buffer.retain(|&(_, a_time)| a_time == slide);

                                    // Collect all bids in the same slide.
                                    additions
                                        .entry(downgrade)
                                        .or_insert(Vec::new())
                                        .extend(bids_buffer.drain(..).map(|(b,_)| b));
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
                                            use std::collections::hash_map::Entry;
                                            match accumulations.entry(auction) {
                                                Entry::Occupied(mut entry) => {
                                                    *entry.get_mut() -= 1;
                                                    if *entry.get_mut() == 0 {
                                                        entry.remove();
                                                    }
                                                },
                                                _ => panic!("entry has to exist"),
                                            }
                                        }
                                    }
                                    // TODO: This only accumulates per *worker*, not globally!
                                    if let Some((_count, auction)) = accumulations.iter().map(|(&a,&c)| (c,a)).max() {
                                        output.session(&time).give(auction);
                                    }
                                }
                            }
                        })
                     .probe_with(&mut probe);
            });
        }

        if queries.iter().any(|x| *x == "q5-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                let window_slice_count = 60;
                let window_slide_ns = 1_000_000_000;

                let bids = Some(bids.clone()).replay_into(scope)
                     // Discretize bid's datetime based on slides
                     .map(move |b| (b.auction, ::nexmark::event::Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns)));
                     // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.

                #[derive(Abomonation, Eq, PartialEq, Clone)]
                enum InsDel<T> { Ins(T), Del(T) };
                let mut buffer = Vec::new();
                let mut buffer2 = Vec::new();
                let mut in_buffer = Vec::new();

                // Partitions by auction id
                bids.stateful_unary_input(&control, |(auction, _time)| calculate_hash(auction), "q5-flex", move |state, time, data, _output| {
                    let mut current_time_key = None;
                    data.swap(&mut in_buffer);
                    in_buffer.sort_by_key(|&(_, key_id, (_auction, a_time))| (key_id, a_time));
                    for (_, key_id, (auction, a_time)) in in_buffer.drain(..) {
                        let bin_id = state.key_to_bin(key_id);
                        if Some((a_time, bin_id)) == current_time_key {
                            buffer.push(InsDel::Ins(auction));
                        } else if current_time_key.is_none() {
                            buffer.push(InsDel::Ins(auction));
                            current_time_key = Some((a_time, bin_id));
                        } else if let Some((current_time, current_bin)) = current_time_key.take() {
                            let not = state.get_bin(current_bin).notificator();
                            not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(current_time))), buffer.drain(..));
                            current_time_key = Some((a_time, bin_id));
                            buffer.push(InsDel::Ins(auction));
                        }
                    }
                    if let Some((current_time, bin)) = current_time_key {
                        let not = state.get_bin(bin).notificator();
                        not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(current_time))), buffer.drain(..));
                    }
                }, move |time, data, bid_bin, output| {
                    // Process additions (if any)
                    for action in data {
                        match action {
                            InsDel::Ins(auction) => {
                                // Stash pending deletions
                                let bid_state: &mut HashMap<_, _> = bid_bin.state();
                                let slot = bid_state.entry(*auction).or_insert(0);
                                *slot += 1;
                                buffer2.push(InsDel::Del(*auction));
                            },
                            InsDel::Del(auction) => {
                                let slot = bid_bin.state().entry(*auction).or_insert(0);
                                *slot -= 1;
                            }
                        }
                        let new_time = ::nexmark::event::Date::new(*to_nexmark_time(time.time().inner) + (window_slice_count * window_slide_ns));
                        bid_bin.notificator().notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(new_time))), buffer2.drain(..));
                    }
                    // Output results (if any)
                    let mut session = output.session(&time);
                    // TODO: This only accumulates per *bin*, not globally!
                    if let Some((auction, _count)) = bid_bin.state().iter().max_by_key(|(_auction_id, count)| *count) {
                        session.give(*auction);
                    }
                }).probe_with(&mut probe);
            });
        }

        if queries.iter().any(|x| *x == "q6") {
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
                                    for (bidder, price) in data.iter().cloned() {
                                        let entry = state.entry(bidder).or_insert(VecDeque::new());
                                        if entry.len() >= 10 { entry.pop_back(); }
                                        entry.push_front(price);
                                        let sum: usize = entry.iter().sum();
                                        session.give((bidder, sum / entry.len()));
                                    }
                                });
                            }
                        })
                    .probe_with(&mut probe);
            });
        }

        if queries.iter().any(|x| *x == "q6-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                let winners =  Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(_a, b)| (b.bidder, b.price));

                winners.stateful_unary(&control, |(b, _p)| calculate_hash(b), "q6-flex", |time, data, bin, output| {
                    let mut session = output.session(&time);
                    let state: &mut HashMap<_, _> = bin.state();
                    for (bidder, price) in data {
                        let entry = state.entry(*bidder).or_insert(Vec::new());
                        while entry.len() >= 10 { entry.remove(0); }
                        entry.push(*price);
                        let sum: usize = entry.iter().sum();
                        session.give((*bidder, sum / entry.len()));
                    }
                }).probe_with(&mut probe);
            });
        }


        if queries.iter().any(|x| *x == "q7") {
            worker.dataflow(|scope| {

                use timely::dataflow::channels::pact::{Pipeline, Exchange};
                use timely::dataflow::operators::Operator;

                // Window ticks every 10 seconds.
                let window_size_ns = 10_000_000_000;

                Some(bids.clone()).replay_into(scope)
                     .map(move |b| (::nexmark::event::Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns), b.price))
                     .unary_frontier(Pipeline, "Q7 Pre-reduce", |_cap, _info| {

                        use timely::dataflow::operators::Capability;
                        use timely::progress::nested::product::Product;
                        use timely::progress::timestamp::RootTimestamp;

                        // Tracks the worker-local maximal bid for each capability.
                        let mut maxima = Vec::<(Capability<Product<RootTimestamp, usize>>, usize)>::new();

                        move |input, output| {

                            input.for_each(|time, data| {

                                for (window, price) in data.iter().cloned() {
                                    if let Some(position) = maxima.iter().position(|x| (x.0).time().inner == from_nexmark_time(window)) {
                                        if maxima[position].1 < price {
                                            maxima[position].1 = price;
                                        }
                                    }
                                    else {
                                        maxima.push((time.delayed(&RootTimestamp::new(from_nexmark_time(window))), price));
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

                            for (window, price) in data.iter().cloned() {
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

        if queries.iter().any(|x| *x == "q7-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                // Window ticks every 10 seconds.
                let window_size_ns = 10_000_000_000;

                let bids = Some(bids.clone()).replay_into(scope)
                     .map(move |b| (b.auction, ::nexmark::event::Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns), b.price));


                // Partition by auction id to avoid serializing the computation
                bids.stateful_unary_input(&control, |(a, _window, _price)| calculate_hash(a), "q7-flex pre-reduce", move |state, time, data, _output| {
                    for (_, key_id, (_auction, window, price)) in data.iter() {
                        let not = state.get(*key_id).notificator();
                        not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(*window))), Some((*window, *price)));
                    }
                }, |time, maxima, bid_bin, output| {
                    let mut windows = HashMap::new();
                    let bid_state: &mut HashMap<_, _> = bid_bin.state();
                    for (window, price) in maxima.iter() {
                        let open_windows = bid_state.entry(*window).or_insert(0);
                        if *open_windows < *price {
                            *open_windows = *price;
                            windows.insert(window.clone(), *price);
                        }
                    }
                    let mut session = output.session(&time);
                    session.give_iterator(windows.drain());
                })
                     // Aggregate the partial counts. This doesn't need to be stateful since we request notification upon a window firing time and then we drop the state immediately after processing
                     .unary_frontier(Exchange::new(move |x: &(::nexmark::event::Date, usize)| (*x.0 / window_size_ns) as u64), "Q7 All-reduce", |_cap, _info|
                     {
                        let mut pending_maxima: HashMap<_,Vec<_>> = Default::default();
                        let mut notificator = FrontierNotificator::new();
                        move |input, output| {
                            input.for_each(|time, data| {
                                for (window,price) in data.iter().cloned() {
                                    let slot = pending_maxima.entry(window).or_insert_with(Vec::new);
                                    slot.push(price);
                                    notificator.notify_at(time.delayed(&RootTimestamp::new(from_nexmark_time(window))));
                                }
                            });
                            while let Some(time) = notificator.next(&[input.frontier()]) {
                                if let Some(mut maxima) = pending_maxima.remove(&to_nexmark_time(time.time().inner)) {
                                    if let Some(max_price) = maxima.drain(..).max(){
                                        output.session(&time).give(max_price);
                                    }
                                }
                            }
                        }

                     })
                     .probe_with(&mut probe);
            });
        }

        if queries.iter().any(|x| *x == "q8") {
            worker.dataflow(|scope| {

                let auctions = Some(auctions.clone()).replay_into(scope)
                      .map(|a| (a.seller, a.date_time));

                let people = Some(people.clone()).replay_into(scope)
                      .map(|p| (p.id, p.date_time));

                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;


                people
                    .binary_frontier(
                        &auctions,
                        Exchange::new(|p: &(usize, _)| p.0 as u64),
                        Exchange::new(|a: &(usize, _)| a.0 as u64),
                        "Q8 join",
                        |_capability, _info| {

                            let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                            let mut new_people = std::collections::HashMap::new();
                            let mut auctions = Vec::new();

                            move |input1, input2, output| {

                                // Notice new people.
                                input1.for_each(|_time, data| {
                                    for (person, time) in data.iter().cloned() {
                                        new_people.insert(person, time);
                                    }
                                });

                                // Notice new auctions.
                                input2.for_each(|time, data| {
                                    let mut data_vec = vec![];
                                    data.swap(&mut data_vec);
                                    auctions.push((time.retain(), data_vec));
                                });

                                // Determine least timestamp we might still see.
                                let complete1 = input1.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                let complete2 = input2.frontier.frontier().get(0).map(|t| t.inner).unwrap_or(usize::max_value());
                                let complete = std::cmp::min(complete1, complete2);

                                for (capability, auctions) in auctions.iter_mut() {
                                    if capability.time().inner < complete {
                                        {
                                            let mut session = output.session(&capability);
                                            for &(person, time) in auctions.iter() {
                                                if time < to_nexmark_time(complete) {
                                                    if let Some(p_time) = new_people.get(&person) {
                                                        if *time < **p_time + window_size_ns {
                                                            session.give(person);
                                                        }
                                                    }
                                                }
                                            }
                                            auctions.retain(|&(_, time)| time >= to_nexmark_time(complete));
                                        }
                                        if let Some(minimum) = auctions.iter().map(|x| x.1).min() {
                                            capability.downgrade(&RootTimestamp::new(from_nexmark_time(minimum)));
                                        }
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

        if queries.iter().any(|x| *x == "q8-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                let auctions = Some(auctions.clone()).replay_into(scope)
                      .map(|a| (a.seller, a.date_time));

                let people = Some(people.clone()).replay_into(scope)
                      .map(|p| (p.id, p.date_time));


                let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                people.stateful_binary(&control, &auctions, |(p, _d)| calculate_hash(p), |(s, _d)| calculate_hash(s), "q8-flex", |_time, data, people_bin: &mut Bin<_, HashMap<_, _>, _>, _auctions_state: &mut Bin<_, Vec<()>, _>, _output| {
                    // Update people state
                    for (person, date) in data {
                        people_bin.state().entry(*person as u64).or_insert(*date);
                    }
                }, move |time, data, people_bin, _auctions_bin, output| {
                    for (seller, date) in data {
                        if let Some(p_time) = people_bin.state().get(&(*seller as u64)) {
                            if **date < **p_time + window_size_ns {
                                output.session(&time).give(*seller);
                            }
                        }
                    }
                }).probe_with(&mut probe);
            });
        }


        ::std::mem::drop(bids);
        ::std::mem::drop(auctions);
        ::std::mem::drop(people);
        ::std::mem::drop(control);

        ::std::mem::drop(closed_auctions);
        ::std::mem::drop(closed_auctions_flex);

        let mut config1 = nexmark::config::Config::new();
        // 0.06*60*60*12 = 0.06*60*60*12
        // auction_proportion*sec_in_12h
        config1.insert("in-flight-auctions", format!("{}", rate * 2592));
        config1.insert("events-per-second", format!("{}", rate));
        config1.insert("first-event-number", format!("{}", index));
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        let mut instructions: Vec<(u64, u64, Vec<ControlInst>)> = map_mode.instructions(peers, duration_ns).unwrap();

        if index == 0 {
            println!("time_dilation\t{}", time_dilation);
            println!("bin_shift\t{}", ::dynamic_scaling_mechanism::BIN_SHIFT);

            for instruction in instructions.iter().take(10) {
                // Format instructions first to be able to truncate the string representation
                eprintln!("instructions\t{:.120}", format!("{:?}", instruction));
            }
        }

        // Establish a start of the computation.
        let elapsed_ns = timer.elapsed().to_nanos();
        config.base_time_ns = elapsed_ns as usize;

        use rand::SeedableRng;
        use rand::rngs::SmallRng;
        assert!(worker.peers() < 256);
        let mut rng = SmallRng::from_seed([worker.peers() as u8;16]);

        let input_times = {
            let config = config.clone();
            move || nexmark::config::NexMarkInputTimes::new(config.clone(), duration_ns, time_dilation, peers)
        };

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 2_000_000_000, duration_ns - 2_000_000_000, duration_ns,
                250_000_000);

        let mut events_so_far = 0;

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let mut input = Some(input);

        let mut control_sequence = 0;
        let mut control_input = Some(control_input);
        if index != 0 {
            control_input.take().unwrap().close();
        } else {
            if let Some((visible, _, _)) = instructions.get(0) {
                control_input.as_mut().unwrap().advance_to(*visible as usize);
            }
        }

        let mut last_migrated = None;

        let mut last_ns = 0;

        loop {
            let elapsed_ns = timer.elapsed().to_nanos();
            let wait_ns = last_ns;
            let target_ns = (elapsed_ns + 1) / 1_000_000 * 1_000_000;
            last_ns = target_ns;

            if index == 0 {

                if last_migrated.map_or(true, |time| control_input.as_ref().map_or(false, |t| t.time().inner != time))
                    && instructions.get(0).map(|&(visible, _ts, _)| visible < target_ns).unwrap_or(false)
                {
                    let (_visible, ts, ctrl_instructions) = instructions.remove(0);
                    let count = ctrl_instructions.len();

                    let control_input = control_input.as_mut().unwrap();
                    control_input.advance_to(ts as usize);

                    for instruction in ctrl_instructions {
                        control_input.send(Control::new(control_sequence, count, instruction));
                    }
                    control_sequence += 1;
                    last_migrated = Some(control_input.time().inner);
                    if let Some((visible, _, _)) = instructions.get(0) {
                        if control_input.time().inner < *visible as usize {
                            control_input.advance_to(*visible as usize);
                        }
                    }
                }

                if instructions.is_empty() {
                    control_input.take();
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

            if let Some(it) = input_times_gen.iter_until(target_ns) {
                let mut input = input.as_mut().unwrap();
                for _t in it {
                    input.send(
                        Event::create(
                            events_so_far,
                            &mut rng,
                            &mut config));
                    events_so_far += worker.peers();
                }
                input.advance_to(target_ns as usize);
                if let Some(control_input) = control_input.as_mut() {
                    if control_input.time().inner < target_ns as usize {
                        control_input.advance_to(target_ns as usize);
                    }
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                while probe.less_than(&RootTimestamp::new(wait_ns as usize)) { worker.step(); }
            } else {
                while worker.step() { }
            }
        }

        output_metric_collector.into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    statm_reporter_running.store(false, ::std::sync::atomic::Ordering::SeqCst);

    let ::streaming_harness::timeline::Timeline { timeline, latency_metrics, .. } = ::streaming_harness::output::combine_all(timelines);

    let latency_metrics = latency_metrics.into_inner();
    println!("DEBUG_summary\t{}", latency_metrics.summary_string().replace("\n", "\nDEBUG_summary\t"));
    println!("{}",
              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
                    format!("DEBUG_timeline\t-- {} ({} samples) --\nDEBUG_timeline\t{}", time, samples, metrics.summary_string().replace("\n", "\nDEBUG_timeline\t"))).collect::<Vec<_>>().join("\n"));

    for (value, prob, count) in latency_metrics.ccdf() {
        println!("latency_ccdf\t{}\t{}\t{}", value, prob, count);
    }
    println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
}
