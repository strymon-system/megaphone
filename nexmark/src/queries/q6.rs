
use std::collections::VecDeque;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q6<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{

    input.closed_auctions(scope)
        .map(|(_a, b)| (b.bidder, b.price))
        .unary(Exchange::new(|x: &(usize, usize)| x.0 as u64), "Q6 Average",
               |_cap, _info| {

                   // Store bidder -> [prices; 10]
                   let mut state = std::collections::HashMap::new();

                   move |input, output| {
                       input.for_each(|time, data| {
                           let mut session = output.session(&time);
                           for (bidder, price) in data.iter().cloned() {
                               let entry = state.entry(bidder).or_insert_with(VecDeque::new);
                               if entry.len() >= 10 { entry.pop_back(); }
                               entry.push_front(price);
                               let sum: usize = entry.iter().sum();
                               session.give((bidder, sum / entry.len()));
                           }
                       });
                   }
               })
}
