
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Map;

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::event::Date;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q5_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, usize>
{
    let control = input.control(scope);

    let window_slice_count = 60;
    let window_slide_ns = 1_000_000_000;

    let bids = input.bids(scope)
        // Discretize bid's datetime based on slides
        .map(move |b| (b.auction, Date::new(((*b.date_time / window_slide_ns) + 1) * window_slide_ns)));
    // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.

    #[derive(Abomonation, Eq, PartialEq, Clone)]
    enum InsDel<T> { Ins(T), Del(T) };
    let mut in_buffer = Vec::new();

    // Partitions by auction id
    bids.stateful_unary_input(&control, |(auction, _time)| calculate_hash(auction), "q5-flex", move |state, cap, _time, data, _output| {
        data.swap(&mut in_buffer);

        for (_, key_id, (auction, a_time)) in in_buffer.drain(..) {
            let not = state.get(key_id).notificator();
            not.notify_at_data(cap, nt.from_nexmark_time(a_time), InsDel::Ins(auction));
            not.notify_at_data(cap, nt.from_nexmark_time(Date::new(*a_time + window_slice_count * window_slide_ns)), InsDel::Del(auction));
        }
    }, move |cap, data, bid_bin, output| {
        // Process additions (if any)
//        println!("data.len(): {:?}", data.len());
        for (_time, action) in data.drain(..) {
            match action {
                InsDel::Ins(auction) => {
                    // Stash pending deletions
                    let bid_state: &mut HashMap<_, _> = bid_bin.state();
                    let slot = bid_state.entry(auction).or_insert(0);
                    *slot += 1;
                },
                InsDel::Del(auction) => {
                    let slot = bid_bin.state().entry(auction).or_insert(0);
                    *slot -= 1;
                }
            }
        }
        bid_bin.state().retain(|_k, v| *v != 0);
        // Output results (if any)
        // TODO: This only accumulates per *bin*, not globally!
        if let Some((auction, _count)) = bid_bin.state().iter().max_by_key(|(_auction_id, count)| *count) {
            let mut session = output.session(&cap);
            session.give(*auction);
        }
    })
}
