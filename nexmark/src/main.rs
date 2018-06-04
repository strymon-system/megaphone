extern crate rand;
extern crate timely;
extern crate nexmark;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Filter, Probe};

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // Declare re-used input and probe handles.
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Q0: Do nothing in particular.
        if std::env::args().any(|x| x == "q0") {
            worker.dataflow(|scope| {
                input.to_stream(scope)
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

        // Q4: Close some auctions.
        if std::env::args().any(|x| x == "q4") {
            worker.dataflow(|scope| {
                let events = input.to_stream(scope);

                let bids = events.flat_map(|e| nexmark::event::Bid::from(e));
                let auctions = events.flat_map(|e| nexmark::event::Auction::from(e));

                use timely::dataflow::channels::pact::Exchange;
                use timely::dataflow::operators::Operator;

                bids.binary_frontier(
                        &auctions,
                        Exchange::new(|b: &nexmark::event::Bid| b.auction as u64),
                        Exchange::new(|a: &nexmark::event::Auction| a.id as u64),
                        "Q4 Auction close",
                        |_capability, _info| {

                            let mut state = std::collections::HashMap::new();
                            let mut opens = std::collections::BinaryHeap::new();

                            use timely::dataflow::operators::Capability;
                            use timely::progress::nested::product::Product;
                            use timely::progress::timestamp::RootTimestamp;

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
                    );
            });
        }


        let rate = std::env::args().nth(1).expect("rate absent").parse().expect("couldn't parse rate");
        let mut config1 = nexmark::config::Config::new();
        config1.insert("events-per-second", rate);
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        // Establish a start of the computation.
        let elapsed = timer.elapsed();
        let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;
        config.base_time_ns = elapsed_ns;
        let mut requested_ns = elapsed_ns;

        use rand::{StdRng, SeedableRng};
        let mut rng = StdRng::from_seed([0;32]);

        let mut counts = vec![[0u64; 16]; 64];

        let mut event_id = 0;
        let duration_ns = 10_000_000_000;
        while requested_ns < duration_ns {

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

            input.advance_to(elapsed_ns);
            while probe.less_than(input.time()) { worker.step(); }
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