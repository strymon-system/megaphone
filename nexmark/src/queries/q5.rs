
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Map, Operator};

use ::event::Date;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q5<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, usize>
{
    let window_slice_count = 60;
    let window_slide_ns = 1_000_000_000;

    input.bids(scope)
        .map(move |b| (b.auction, Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns)))
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
                                    let slide = Date::new(((*nt.to_nexmark_time(*time.time()) / window_slide_ns) + 1) * window_slide_ns);
                                    let downgrade = time.delayed(&nt.from_nexmark_time(slide));

                                    // Collect all bids in a different slide.
                                    for &(auction, a_time) in bids_buffer.iter() {
                                        if a_time != slide {
                                            additions
                                                .entry(time.delayed(&nt.from_nexmark_time(a_time)))
                                                .or_insert(Vec::new())
                                                .push(auction);
                                        }
                                    }
                                    bids_buffer.retain(|&(_, a_time)| a_time == slide);

                                    // Collect all bids in the same slide.
                                    additions
                                        .entry(downgrade)
                                        .or_insert(Vec::new())
                                        .extend(bids_buffer.drain(..).map(|(b, _)| b));
                                });

                                // Extract and order times we can now process.
                                let mut times = {
                                    let add_times = additions.keys().filter(|t| !input.frontier.less_equal(t.time())).cloned();
                                    let del_times = deletions.keys().filter(|t: &&Capability<usize>| !input.frontier.less_equal(t.time())).cloned();
                                    add_times.chain(del_times).collect::<Vec<_>>()
                                };
                                times.sort_by(|x, y| x.time().cmp(&y.time()));
                                times.dedup();

                                for time in times.drain(..) {
                                    if let Some(additions) = additions.remove(&time) {
                                        for &auction in additions.iter() {
                                            *accumulations.entry(auction).or_insert(0) += 1;
                                        }
                                        let new_time = time.time() + (window_slice_count * window_slide_ns);
                                        deletions.insert(time.delayed(&new_time), additions);
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
                                    if let Some((_count, auction)) = accumulations.iter().map(|(&a, &c)| (c, a)).max() {
                                        output.session(&time).give(auction);
                                    }
                                }
                            }
                        })
}
