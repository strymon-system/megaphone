
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{FrontierNotificator, Map, Operator};

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::event::Date;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q7_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, usize>
{
    let control = input.control(scope);
    // Window ticks every 10 seconds.
    let window_size_ns = 10_000_000_000;

    let bids = input.bids(scope)
        .map(move |b| (b.auction, Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns), b.price));


    // Partition by auction id to avoid serializing the computation
    bids.stateful_unary_input(&control, |(a, _window, _price)| calculate_hash(a), "q7-flex pre-reduce", move |state, cap, _time, data, _output| {
        for (_, key_id, (_auction, window, price)) in data.iter() {
            let not = state.get(*key_id).notificator();
            not.notify_at_data(cap, nt.from_nexmark_time(*window), (*window, *price));
        }
    }, |cap, maxima, bid_bin, output| {
        let mut windows = HashMap::new();
        let bid_state: &mut HashMap<_, _> = bid_bin.state();
        for (_time, (window, price)) in maxima.iter() {
            let open_windows = bid_state.entry(*window).or_insert(0);
            if *open_windows < *price {
                *open_windows = *price;
                windows.insert(window.clone(), *price);
            }
        }
        let mut session = output.session(&cap);
        session.give_iterator(windows.drain());
    })
        // Aggregate the partial counts. This doesn't need to be stateful since we request notification upon a window firing time and then we drop the state immediately after processing
        .unary_frontier(Exchange::new(move |x: &(Date, usize)| (*x.0 / window_size_ns) as u64), "Q7 All-reduce", |_cap, _info|
            {
                let mut pending_maxima: HashMap<_, Vec<_>> = Default::default();
                let mut notificator = FrontierNotificator::new();
                move |input, output| {
                    input.for_each(|time, data| {
                        for (window, price) in data.iter().cloned() {
                            let slot = pending_maxima.entry(window).or_insert_with(Vec::new);
                            slot.push(price);
                            notificator.notify_at(time.delayed(&nt.from_nexmark_time(window)));
                        }
                    });
                    while let Some(time) = notificator.next(&[input.frontier()]) {
                        if let Some(mut maxima) = pending_maxima.remove(&nt.to_nexmark_time(*time.time())) {
                            if let Some(max_price) = maxima.drain(..).max() {
                                output.session(&time).give(max_price);
                            }
                        }
                    }
                }
            })

}
