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

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Capability;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::ExchangeData;

use dynamic_scaling_mechanism::State;
use dynamic_scaling_mechanism::{ControlInst, Control};
use dynamic_scaling_mechanism::operator::StatefulOperator;

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

    // Read and report RSS every 100ms
    let statm_reporter_running = ::std::sync::Arc::new(::std::sync::atomic::AtomicBool::new(true));
    {
        let statm_reporter_running = statm_reporter_running.clone();
        ::std::thread::spawn(move || {
            use std::io::Read;
            let timer = ::std::time::Instant::now();
            let mut iteration = 0;
            while statm_reporter_running.load(::std::sync::atomic::Ordering::SeqCst) {
                let mut stat_s = String::new();
                let mut statm_f = ::std::fs::File::open("/proc/self/statm").expect("can't open /proc/self/statm");
                statm_f.read_to_string(&mut stat_s).expect("can't read /proc/self/statm");
                let pages: u64 = stat_s.split_whitespace().nth(1).expect("wooo").parse().expect("not a number");
                let rss = pages * 1024;

                let elapsed_ns = timer.elapsed().to_nanos();
                println!("statm_RSS\t{}\t{}", elapsed_ns, rss);
                #[allow(deprecated)]
                ::std::thread::sleep_ms(100 - (elapsed_ns / 1_000_000 - iteration * 100) as u32);
                iteration += 1;
            }
        });
    }

    // define a new computational scope, in which to run BFS
    let timelines: Vec<_> = timely::execute_from_args(std::env::args(), move |worker| {

        let time_dilation = std::env::args().nth(4).map_or(1, |arg| arg.parse().unwrap_or(1));

        let to_nexmark_time = move |x| ::nexmark::event::Date::new(x * time_dilation);
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
            let input = input.to_stream(scope);
            input.flat_map(|e| nexmark::event::Bid::from(e)).capture_into(bids.clone());
            input.flat_map(|e| nexmark::event::Auction::from(e)).capture_into(auctions.clone());
            input.flat_map(|e| nexmark::event::Person::from(e)).capture_into(people.clone());
            control_input.to_stream(scope).broadcast().capture_into(control.clone());
        });


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
                let control = Some(control.clone()).replay_into(scope);
                input.to_stream(scope)
                    .distribute(&control, |e| calculate_hash(&e.id()), "q0-flex")
                    .probe_with(&mut probe);
            });
        }

        // Q1: Convert bids to euros.
        if std::env::args().any(|x| x == "q1") {
            worker.dataflow(|scope| {
                Some(bids.clone()).replay_into(scope)
                     .map_in_place(|b| b.price = (b.price * 89)/100)
                     .probe_with(&mut probe);
            });
        }

        // Q1-flex: Convert bids to euros.
        if std::env::args().any(|x| x == "q1-flex") {
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
        if std::env::args().any(|x| x == "q2") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                Some(bids.clone()).replay_into(scope)
                     .filter(move |b| b.auction % auction_skip == 0)
                     .map(|b| (b.auction, b.price))
                     .probe_with(&mut probe);
            });
        }

        // Q2-flex: Filter some auctions.
        if std::env::args().any(|x| x == "q2-flex") {
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
        if std::env::args().any(|x| x == "q3") {
            worker.dataflow(|scope| {

                let auctions = Some(auctions.clone()).replay_into(scope)
                    .filter(|a| a.category == 10);

                let people = Some(people.clone()).replay_into(scope)
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

                let control = Some(control.clone()).replay_into(scope);

                let auctions = Some(auctions.clone()).replay_into(scope)
                    .filter(|a| a.category == 10);

                let people = Some(people.clone()).replay_into(scope)
                      .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

                auctions.stateful_binary(&control, &people, |a| calculate_hash(&a.seller), |p| calculate_hash(&p.id), "q3-flex join", |time, data, auction_state, people_state, output| {
                    let mut session = output.session(&time);
                    for (bin_id, auction) in data {
                        let bin: &mut HashMap<_, nexmark::event::Person> = people_state.get_state(bin_id);
                        if let Some(mut person) = bin.get(&auction.seller) {
                            session.give((person.name.clone(),
                                          person.city.clone(),
                                          person.state.clone(),
                                          auction.id));
                        }
                        // Update auction state
                        let bin: &mut HashMap<_, nexmark::event::Auction> = auction_state.get_state(bin_id);
                        bin.insert(auction.seller, auction);
                    };
                }, |time, data, auction_state, people_state, output| {
                    let mut session = output.session(&time);
                    for (bin_id, person) in data {
                        if let Some(mut auction) = auction_state.get_state(bin_id).get(&person.id) {
                            session.give((person.name.clone(),
                                          person.city.clone(),
                                          person.state.clone(),
                                          auction.id));
                        }
                        // Update people state
                        people_state.get_state(bin_id).insert(person.id, person);
                    };
                }).probe_with(&mut probe);

            });
        }

        // Intermission: Close some auctions.
        let closed_auctions = std::rc::Rc::new(timely::dataflow::operators::capture::event::link::EventLink::new());
        if std::env::args().any(|x| x == "q4" || x == "q6") {
            worker.dataflow(|scope| {
                let bids = Some(bids.clone()).replay_into(scope);
                let auctions = Some(auctions.clone()).replay_into(scope);

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
                                        if capability.as_ref().map(|c| to_nexmark_time(c.time().inner) <= auction.expires) != Some(true) {
                                            let mut new_time = time.time().clone();
                                            new_time.inner = from_nexmark_time(auction.expires);
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
                                    while opens.peek().map(|x| (x.0).0 < to_nexmark_time(complete)) == Some(true) {

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
        if std::env::args().any(|x| x == "q4-flex" || x == "q6-flex") {
            worker.dataflow(|scope| {
                let control = Some(control.clone()).replay_into(scope);

                let bids = Some(bids.clone()).replay_into(scope);
                let auctions = Some(auctions.clone()).replay_into(scope);

                bids.stateful_binary_input(&control,
                        &auctions,
                        |bid| calculate_hash(&bid.auction),
                        |a| calculate_hash(&a.id),
                        "Q4 Auction close",
                        move |not, time, data, _output| {
                            let mut not_time = time.time().clone();
                            for (_, key_id, bid) in data.drain(..) {
                                not_time.inner = from_nexmark_time(bid.date_time);
                                not.notify_at_data(time.delayed(&not_time), Some((key_id, bid)))
                            }
                        },
                        move |not, time, data, _output| {
                            let mut new_time = time.time().clone();
                            for (_target, key_id, auction) in data.drain(..) {
                                new_time.inner = from_nexmark_time(auction.expires);
                                // Request notification for the auction's expiration time, which is used to look into the auctions_state
                                not.notify_at_data(time.delayed(&new_time), Some((key_id, auction)));
                            }
                        },
                        |_time, data, bid_state, _auction_state, _output| {
                            for (key_id, bid) in data {
                                // Update bin state
                                let bin: &mut HashMap<_, _> = bid_state.get_state(key_id);
                                bin.entry(bid.auction).or_insert_with(Vec::new).push(bid);
                            };
                        },
                        |time, auction_data, bid_state, _auction_state: &mut State<Vec<()>>, output| {
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
                        },
                    )
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

                let control = Some(control.clone()).replay_into(scope);

                Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(a,b)| (a.category, b.price))
                    .stateful_unary(&control, |x| calculate_hash(&x.0), "Q4 Average",
                        |time, data, state, output| {
                            let mut session = output.session(&time);
                            for (key_id, (category, price)) in data {
                                let bin: &mut HashMap<_, _> = state.get_state(key_id);
                                let entry = bin.entry(category).or_insert((0usize, 0));
                                entry.0 += price;
                                entry.1 += 1;
                                session.give((category, entry.0 / entry.1));
                            }
                        })
                    .probe_with(&mut probe);
            });
        }

        if std::env::args().any(|x| x == "q5") {
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

                            move |input, output| {

                                input.for_each(|time, data| {

                                    let slide = ::nexmark::event::Date::new(((*to_nexmark_time(time.time().inner) / window_slide_ns) + 1) * window_slide_ns);
                                    let downgrade = time.delayed(&RootTimestamp::new(from_nexmark_time(slide)));

                                    // Collect all bids in a different slide.
                                    for &(auction, a_time) in data.iter() {
                                        if a_time != slide {
                                            additions
                                                .entry(time.delayed(&RootTimestamp::new(from_nexmark_time(a_time))))
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

        if std::env::args().any(|x| x == "q5-flex") {
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

                // Partitions by auction id
                bids.stateful_unary_input(&control, |(auction, _time)| calculate_hash(auction), "q5-flex", move |not, time, data, _output| {
                    let _: &mut dynamic_scaling_mechanism::notificator::FrontierNotificator<_, (dynamic_scaling_mechanism::Key, InsDel<_>)> = not;
                    for &(_, key_id, (auction, a_time)) in data.iter() {
                        // Stash pending additions
                        not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(a_time))), Some((key_id, InsDel::Ins(auction))));

                        // Stash pending deletions
                        let new_time = ::nexmark::event::Date::new(*a_time + (window_slice_count * window_slide_ns));
                        not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(new_time))), Some((key_id, InsDel::Del(auction))));
                    }
                }, |time, data, bid_state, output| {
                    // Process additions (if any)
                    for (key_id, action) in data {
                        let mut bin: &mut HashMap<_, _> = bid_state.get_state(key_id);
                        match action {
                            InsDel::Ins(auction) => {
                                let slot = bin.entry(auction).or_insert(0);
                                *slot += 1;
                            },
                            InsDel::Del(auction) => {
                                let slot = bin.entry(auction).or_insert(0);
                                *slot -= 1;
                            }
                        }
                    }
                    // Output results (if any)
                    let mut session = output.session(&time);
                    // TODO: This only accumulates per *bin*, not globally!
                    bid_state.scan(move |a| {
                        if let Some((auction, _count)) = a.iter().max_by_key(|(_auction_id, count)| *count) {
                            session.give(*auction);
                        }
                    });
                }).probe_with(&mut probe);
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
                                        let sum: usize = entry.iter().sum();
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

                let control = Some(control.clone()).replay_into(scope);

                let winners =  Some(closed_auctions_flex.clone())
                    .replay_into(scope)
                    .map(|(_a, b)| (b.bidder, b.price));

                winners.stateful_unary(&control, |(b,_p)| calculate_hash(b), "q6-flex", |time, data, state, output| {
                    let mut session = output.session(&time);
                    for (bin_id, (bidder, price)) in data {
                        let bin: &mut HashMap<_, _> = state.get_state(bin_id);
                        let entry = bin.entry(bidder).or_insert(Vec::new());
                        while entry.len() >= 10 { entry.remove(0); }
                        entry.push(price);
                        let sum: usize = entry.iter().sum();
                        session.give((bidder, sum / entry.len()));
                    }
                }).probe_with(&mut probe);
            });
        }


        if std::env::args().any(|x| x == "q7") {
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

                                for (window, price) in data.drain(..) {
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

                let control = Some(control.clone()).replay_into(scope);

                // Window ticks every 10 seconds.
                let window_size_ns = 10_000_000_000;

                let bids = Some(bids.clone()).replay_into(scope)
                     .map(move |b| (b.auction, ::nexmark::event::Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns), b.price));


                // Partition by auction id to avoid serializing the computation
                bids.stateful_unary_input(&control, |(a, _window, _price)| calculate_hash(a), "q7-flex pre-reduce", move |not, time, data, _output| {
                    for (_, key_id, (_auction, window, price)) in data.drain(..) {
                        not.notify_at_data(time.delayed(&RootTimestamp::new(from_nexmark_time(window))), Some((key_id, (window, price))));
                    }
                }, |time, maxima, bid_state, output| {
                    let mut windows = HashMap::new();
                    for (key_id, (window, price)) in maxima {
                        let bin: &mut HashMap<_, _> = bid_state.get_state(key_id);
                        let open_windows = bin.entry(window).or_insert(0);
                        if *open_windows < price {
                            *open_windows = price;
                            windows.insert(window, price);
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
                                for (window,price) in data.drain(..) {
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

        if std::env::args().any(|x| x == "q8") {
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

        if std::env::args().any(|x| x == "q8-flex") {
            worker.dataflow(|scope| {

                let control = Some(control.clone()).replay_into(scope);

                let auctions = Some(auctions.clone()).replay_into(scope)
                      .map(|a| (a.seller, a.date_time));

                let people = Some(people.clone()).replay_into(scope)
                      .map(|p| (p.id, p.date_time));


                let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                people.stateful_binary(&control, &auctions, |(p, _d)| calculate_hash(p), |(s, _d)| calculate_hash(s), "q8-flex", |_time, data, people_state: &mut State<HashMap<_, _>>, _auctions_state: &mut State<Vec<()>>, _output| {
                    // Update people state
                    for (bin_id, (person, date)) in data {
                        people_state.get_state(bin_id).entry(person as u64).or_insert(date);
                    }
                }, move |time, data, people_state, _auctions_state, output| {
                    for (bin_id, (seller, date)) in data {
                        if let Some(p_time) = people_state.get_state(bin_id).get(&(seller as u64)) {
                            if *date < **p_time + window_size_ns {
                                output.session(&time).give(seller);
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
                            let inst = parts.chunks(2).map(|x|ControlInst::Move(::dynamic_scaling_mechanism::Bin::new(x[0]), x[1])).collect();
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
            move || nexmark::config::NexMarkInputTimes::new(config.clone(), duration_ns, time_dilation)
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
        }

        let mut last_migrated = None;

        let mut last_ns = 0;

        loop {
            let elapsed_ns = timer.elapsed().to_nanos();
            let wait_ns = last_ns;
            let target_ns = {
                let scale = (elapsed_ns - last_ns).next_power_of_two() / 2;
                elapsed_ns & !(scale - 1)
            };
            last_ns = target_ns;

            if index == 0
                && last_migrated.map_or(true, |time| control_input.as_ref().map_or(false, |t| t.time().inner != time))
                && instructions.get(0).map(|&(ts, _)| ts < target_ns).unwrap_or(false)
            {
                let instructions = instructions.remove(0).1;
                let count = instructions.len();
                for instruction in instructions {
                    control_input.as_mut().unwrap().send(Control::new(control_sequence, count, instruction));
                }
                control_sequence += 1;
                last_migrated = Some(control_input.as_ref().unwrap().time().inner);
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
