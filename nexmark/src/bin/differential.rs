extern crate rand;
extern crate timely;
extern crate differential_dataflow;

extern crate nexmark;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Probe};

use timely::dataflow::channels::pact::Pipeline;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // Declare re-used input and probe handles.
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Capture bids, auctions, and people. Index where appropriate.
        let (mut bids, mut auctions, mut people) = worker.dataflow(|scope| {

            use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

            let mut demux = OperatorBuilder::new("NEXMark demux".to_string(), scope.clone());

            let mut input = demux.new_input(&input.to_stream(scope), Pipeline);

            let (mut b_out, bids) = demux.new_output();
            let (mut a_out, auctions) = demux.new_output();
            let (mut p_out, people) = demux.new_output();

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
                                nexmark::event::Event::Bid(b) => { let temp = b.date_time; b_session.give((b, *temp, 1 as isize)) },
                                nexmark::event::Event::Auction(a) => { let temp = a.date_time; a_session.give(((a.id,a), *temp, 1 as isize)) },
                                nexmark::event::Event::Person(p) =>  { let temp = p.date_time; p_session.give(((p.id,p), *temp, 1 as isize)) },
                            }
                        }
                    });

                }

            });

            let bids = bids.as_collection();
            let auctions = auctions.as_collection();
            let people = people.as_collection();

            (bids.arrange_by_self().trace, auctions.arrange_by_key().trace, people.arrange_by_key().trace)
        });

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
                bids.import(scope)
                    .as_collection(|b,_| b.clone())
                    .map_in_place(|b| b.price = (b.price * 89)/100)
                    .probe_with(&mut probe);
            });
        }

        // Q2: Filter some auctions.
        if std::env::args().any(|x| x == "q2") {
            worker.dataflow(|scope| {
                let auction_skip = 123;
                bids
                    .import(scope)
                    .flat_map_ref(move |b,_| if b.auction % auction_skip == 0 { Some((b.auction, b.price)) } else { None })
                    .probe_with(&mut probe);
            });
        }

        // Q3: Join some auctions.
        if std::env::args().any(|x| x == "q3") {
            worker.dataflow(|scope| {

                let auctions =
                auctions
                    .import(scope)
                    .flat_map_ref(|&id,a| if a.category == 10 { Some(id) } else { None });

                let people =
                people
                    .import(scope)
                    .flat_map_ref(|&id,p| if p.state == "OR" || p.state == "ID" || p.state == "CA" {
                        Some((id,(p.name.clone(), p.state.clone(), p.city.clone())))
                    } else { None });

                use differential_dataflow::operators::Join;

                // TODO: Could consider filtered arrangements here.
                people
                    .semijoin(&auctions)
                    .probe_with(&mut probe);
            });
        }

        // Intermission: Close some auctions.
        let mut closed_auctions = None;
        if std::env::args().any(|x| x == "q4" || x == "q6") {

            closed_auctions = Some(
                worker.dataflow(|scope| {

                    // An auction is open from its date_time forward, and closed from its expires forward.
                    // Bids for an auction are valid if their time is before the auction closes, and their
                    // price exceeds the reserve of the auction.
                    // As we only want to see closed auctions, we should join with the delayed auction.

                    let auctions = auctions.import(scope);
                    let bids = bids.import(scope).as_collection(|b,_| (b.auction,b.clone()));

                    use differential_dataflow::operators::{Join, JoinCore, Group};

                    let valid =
                    bids.join_core(&auctions, |key,bid,auc| {
                            if bid.date_time < auc.expires && bid.price >= auc.reserve {
                                Some((*key, bid.clone()))
                            }
                            else {
                                None
                            }
                        });

                    let leaders =
                    valid
                        .group(|_key,src,tgt| {
                            let mut max_idx = 0;
                            for idx in 1 .. src.len() {
                                if src[idx].0.price > src[max_idx].0.price {
                                    max_idx = idx;
                                }
                            }
                            tgt.push((src[max_idx].0.clone(),1))
                        });

                    // Time shift the auctions to their closing moments.
                    let shifted =
                    auctions
                        .as_collection(|&id,a| (id,a.clone()))
                        .inner
                        .map_in_place(|x| x.1 = *(x.0).1.expires)
                        .as_collection();

                    // Join leaders with time-shifted auctions to lock in winners.
                    leaders
                        .join_map(&shifted, |&key,bid,auc| (key,(auc.clone(),bid.clone())))
                        .arrange_by_key()
                        .trace
                })
            );
        }

        if std::env::args().any(|x| x == "q4") {
            worker.dataflow(|scope| {

                use differential_dataflow::operators::Consolidate;

                closed_auctions
                    .as_mut()
                    .expect("closed_auctions not properly set")
                    .import(scope)
                    .as_collection(|_,&(ref a, ref b)| (a.category, b.price))
                    .explode(|(category, price)| Some((category, differential_dataflow::difference::DiffPair::new(price as isize, 1))))
                    .consolidate()
                    .probe_with(&mut probe);
            });
        }

        // Q5 determines the auction with the largest number of bids, in a sliding one hour window.
        if std::env::args().any(|x| x == "q5") {
            worker.dataflow(|scope| {

                use differential_dataflow::operators::{Consolidate, Group};

                let window_slide_count = 60;
                let window_slide_ns = 1_000_000_000;

                let bids =
                bids.import(scope)
                    .as_collection(|b,_| b.auction);

                let additions =
                bids.inner
                    .map_in_place(move |x| x.1 = ((x.1 / window_slide_ns) + 1) * window_slide_ns)
                    .as_collection()
                    .consolidate();

                let deletions =
                additions
                    .inner
                    .map_in_place(move |x| x.1 += window_slide_ns * window_slide_count)
                    .as_collection();

                let totals =
                deletions
                    .negate()
                    .concat(&additions)
                    .consolidate();

                totals
                    .map(|auc| ((), auc))
                    .group(|_key,src,tgt| {
                        let mut max_idx = 0;
                        for idx in 1 .. src.len() { if src[idx].1 > src[max_idx].1 { max_idx = idx; } }
                        tgt.push((*src[max_idx].0, 1))
                    })
                    .probe_with(&mut probe);
            });
        }

        // Q6 determines for each seller the average closed auction price over their last ten auctions.
        if std::env::args().any(|x| x == "q6") {
            worker.dataflow(|scope| {

                use differential_dataflow::operators::Group;

                let limit = 10;

                closed_auctions
                    .as_mut()
                    .expect("closed_auctions not properly set")
                    .import(scope)
                    .as_collection(|_id,auc_bid| (auc_bid.0.seller, (auc_bid.0.expires, auc_bid.1.price)))
                    .group(move |_key,src,tgt| {
                        let start = if src.len() < limit { 0 } else { src.len() - limit };
                        let slice = &src[start ..];
                        let total: usize = slice.iter().map(|x| (x.0).1).sum();
                        tgt.push((total/slice.len(),1));
                    })
                    .probe_with(&mut probe);
            });
        }

        // Q7 determines for each minute the highest bid.
        if std::env::args().any(|x| x == "q7") {
            worker.dataflow(|scope| {

                use differential_dataflow::operators::Group;

                // Window ticks every 1 second.
                let window_size_ns = 1_000_000_000;

                let additions =
                bids.import(scope)
                    .as_collection(|b,_| (b.auction, b.price, b.bidder))
                    .inner
                    .map_in_place(move |x| x.1 = ((x.1 / window_size_ns) + 1) * window_size_ns)
                    .as_collection();

                let deletions =
                additions
                    .inner
                    .map_in_place(move |x| x.1 += window_size_ns)
                    .as_collection()
                    .negate();

                deletions
                    .concat(&additions)
                    .map(|bid| ((), bid))
                    .group(|_key,src,tgt| {
                        let mut max_idx = 0;
                        for idx in 1 .. src.len() { if (src[idx].0).1 > (src[max_idx].0).1 { max_idx = idx; } }
                        tgt.push((*src[max_idx].0, 1))
                    })
                    .probe_with(&mut probe);
            });
        }

        // Q8 reports any auctions created within 12 hours of a person's account creation.
        if std::env::args().any(|x| x == "q8") {
            worker.dataflow(|scope| {

                use differential_dataflow::operators::JoinCore;

                let window_size_ns = 12 * 60 * 1_000_000_000;

                let auctions = auctions.import(scope);

                let people =
                people
                    .import(scope)
                    .as_collection(|_,p| (p.id, p.name.clone()));

                people
                    .inner
                    .map_in_place(move |x| x.1 += window_size_ns)
                    .as_collection()
                    .negate()
                    .concat(&people)
                    .join_core(&auctions, |key,per,auc| Some((*key, per.clone(), auc.reserve)))
                    .probe_with(&mut probe);
            });
        }

        drop(bids);
        drop(auctions);
        drop(people);

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

        use rand::SeedableRng;
        use rand::rngs::SmallRng;
        assert!(worker.peers() < 256);
        let mut rng = SmallRng::from_seed([worker.peers() as u8;16]);

        let mut counts = vec![[0u64; 16]; 64];

        let mut event_id = 0;
        // let duration_ns = 120_000_000_000;
        while requested_ns < duration_ns {

            let elapsed = timer.elapsed();
            let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;

            // Determine completed ns.
            let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier.get(0).cloned().unwrap_or(elapsed_ns));

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
            while *next_event.time() <= elapsed_ns && *next_event.time() <= duration_ns {
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
