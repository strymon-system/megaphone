
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Map;

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q6_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{
    let control = input.control(scope);
    let winners = input.closed_auctions_flex(scope)
        .map(|(_a, b)| (b.bidder, b.price));

    winners.stateful_unary(&control, |(b, _p)| calculate_hash(b), "q6-flex", |cap, data, bin, output| {
        let mut session = output.session(&cap);
        let state: &mut HashMap<_, _> = bin.state();
        for (_time, (bidder, price)) in data.drain(..) {
            let entry = state.entry(bidder).or_insert_with(Vec::new);
            while entry.len() >= 10 { entry.remove(0); }
            entry.push(price);
            let sum: usize = entry.iter().sum();
            session.give((bidder, sum / entry.len()));
        }
    })
}
