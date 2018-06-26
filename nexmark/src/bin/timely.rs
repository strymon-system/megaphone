extern crate fnv;
extern crate rand;
extern crate timely;
extern crate nexmark;
extern crate dynamic_scaling_mechanism;

use std::hash::Hash;
use std::hash::Hasher;
use std::collections::{HashMap, VecDeque, BinaryHeap};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Filter, Probe, Capture, capture::Replay};

use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Capability;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::Input;
use timely::dataflow::operators::Broadcast;

use dynamic_scaling_mechanism::stateful::{Stateful, StateHandle};
use dynamic_scaling_mechanism::{BIN_SHIFT, ControlInst, Control};

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // Declare re-used input, control and probe handles.
        let mut input = InputHandle::new();
        let mut control_input = InputHandle::new();
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
                                        .stateful::<_, HashMap<(), ()>, _>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control);
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
                                        .stateful::<_, HashMap<(), ()>, _>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control);
                                        
                                                                             
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
                                        .stateful::<_, HashMap<(), ()>, _>(|e: &nexmark::event::Event| calculate_hash(&e.id()), &control); 
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

                use std::collections::HashMap;
                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;

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

                let auctions =
                events.flat_map(|e| nexmark::event::Auction::from(e))
                      .filter(|a| a.category == 10)
                      .map(|a| (calculate_hash(&a.seller),a))
                      .stateful::<_, HashMap<u64, nexmark::event::Auction>, _>(|(k,_a)| *k, &control);

                let people =
                events.flat_map(|e| nexmark::event::Person::from(e))
                      .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA")
                      .map(|p| (calculate_hash(&p.id),p))
                      .stateful::<_, HashMap<u64,nexmark::event::Person>, _>(|(k,_p)| *k, &control);

                // Pointers to the shared state
                let auction_state = auctions.state.clone();
                let people_state = people.state.clone();

                use std::collections::HashMap;
                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;

                auctions.stream.
                    binary_frontier(
                        &people.stream,
                        Pipeline,
                        Pipeline,
                        "Q3 Join Flex",
                        |_capability, _info| {

                            move |input1, input2, output| {

                                // Stash data till frontiers from both inputs have been advanced
                                let mut pending_auction_state: HashMap<_, Vec<(_, _, _)>> = Default::default();
                                let mut pending_people_state: HashMap<_, Vec<(_, _, _)>> = Default::default();

                                // The shared state
                                let mut auction_state = auction_state.borrow_mut();
                                let mut people_state = people_state.borrow_mut();

                                // Process each input auction.
                                input1.for_each(|time, data| {
                                    pending_auction_state.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                                    auction_state.notificator().notify_at(time.retain());
                                });

                                // Process each input person.
                                input2.for_each(|time, data| {
                                    pending_people_state.entry(time.time().clone()).or_insert_with(Vec::new).extend(data.drain(..));
                                    people_state.notificator().notify_at(time.retain());
                                });

                            while let Some(time) = auction_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                let mut session = output.session(&time);

                                for (_target, key_id, (seller_id,auction)) in pending_auction_state.remove(&time.time()).into_iter().flat_map(|v| v.into_iter()) {
                                    if let Some(mut person) = people_state.get_state(key_id).remove(&seller_id) {
                                        session.give((person.name.clone(), 
                                                    person.city.clone(),
                                                    person.state.clone(),
                                                    auction.id));
                                    }
                                    auction_state.get_state(key_id).insert(seller_id, auction);
                                };

                            }

                            while let Some(time) = people_state.notificator().next(&[input1.frontier(), input2.frontier()]) {
                                let mut session = output.session(&time);

                                for (_target, key_id, (person_id,person)) in pending_people_state.remove(&time.time()).into_iter().flat_map(|v| v.into_iter()) {
                                    if let Some(mut auction) = auction_state.get_state(key_id).remove(&person_id) {
                                        session.give((person.name.clone(), 
                                                    person.city.clone(),
                                                    person.state.clone(),
                                                    auction.id));
                                    }
                                    people_state.get_state(key_id).insert(person_id, person);
                                };
                            }

                            }
                        }
                    )
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
                                    if let Some((count, auction)) = accumulations.iter().map(|(&a,&c)| (c,a)).max() {
                                        output.session(&time).give(auction);
                                    }
                                }
                            }
                        })
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
                                        let entry = state.entry(bidder).or_insert(std::collections::VecDeque::new());
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
                                    let new_time = (window + 1) * window_size_ns;
                                    if let Some(position) = maxima.iter().position(|x| (x.0).time().inner == new_time) {
                                        if maxima[position].1 < price {
                                            maxima[position].1 = price;
                                        }
                                    }
                                    else {
                                        maxima.push((time.delayed(&RootTimestamp::new(new_time)), price));
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

        let rate = std::env::args().nth(1).expect("rate absent").parse().expect("couldn't parse rate");
        let mut config1 = nexmark::config::Config::new();
        config1.insert("events-per-second", rate);
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        let duration_ns: usize = std::env::args().nth(2).expect("duration absent").parse::<usize>().expect("couldn't parse duration") * 1_000_000_000;

        // Establish a start of the computation.
        let elapsed = timer.elapsed();
        let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;
        config.base_time_ns = elapsed_ns;
        let mut requested_ns = elapsed_ns;

        use rand::{StdRng, SeedableRng};
        let mut rng = StdRng::from_seed([0;32]);

        let mut counts = vec![[0u64; 16]; 64];

        let mut control_counter = 0;
        let mut map = vec![0; 1 << BIN_SHIFT];
        // TODO(moritzo) HAAAACCCCKKK
        let peers  = worker.peers();
        if peers != 2 {
            for (i, v) in map.iter_mut().enumerate() {
                *v = ((i / 2) * 2 + (i % 2) * peers / 2) % peers;
            }
        }

        let mut event_id = 0;
        // let duration_ns = 10_000_000_000;
        while requested_ns < duration_ns {

            if worker.index() == 0 {
                control_input.send(Control::new(control_counter,  1, ControlInst::Map(map.clone())));
                control_counter += 1;
            }

            let elapsed = timer.elapsed();
            let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;

            // Determine completed ns.
            let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier.get(0).map(|x| x.inner).unwrap_or(elapsed_ns));

            // Record completed measurements.
            while requested_ns < acknowledged_ns && requested_ns < duration_ns {
                if requested_ns > duration_ns / 2 {
                    let count_index = (elapsed_ns - requested_ns).next_power_of_two().trailing_zeros() as usize;
                    let low_bits = ((elapsed_ns - requested_ns) >> (count_index - 5)) & 0xF;
                    counts[count_index][low_bits as usize] += 1;
                }
                requested_ns += 1_000_000;
            }

            // Insert random events as long as their times precede `elapsed_ns`.
            let mut next_event = nexmark::event::Event::create(event_id, &mut rng, &mut config);
            while next_event.time() <= elapsed_ns && next_event.time() <= duration_ns {
                input.send(next_event);
                event_id += 1;
                next_event = nexmark::event::Event::create(event_id, &mut rng, &mut config);
            }

            // println!("{:?}\tAdvanced", elapsed);
            input.advance_to(elapsed_ns);
            control_input.advance_to(elapsed_ns);

            // while probe.less_than(input.time()) { worker.step(); }
            worker.step();
        }

        // Once complete, report ccdf measurements.
        if worker.index() == 0 {

            let mut results = Vec::new();
            let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
            let mut sum = 0;
            for index in (10 .. counts.len()).rev() {
                for sub in (0 .. 16).rev() {
                    if sum > 0 && sum < total {
                        let latency = (1 << (index-1)) + (sub << (index-5));
                        let fraction = (sum as f64) / (total as f64);
                        results.push((latency, fraction));
                    }
                    sum += counts[index][sub];
                }
            }
            for (latency, fraction) in results.drain(..).rev() {
                println!("{}\t{}", latency, fraction);
            }
        }

    }).expect("timely execution failed");
}