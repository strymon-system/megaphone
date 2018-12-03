use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Filter, Map};

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q2<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{
    let auction_skip = 123;
    input.bids(scope)
        .filter(move |b| b.auction % auction_skip == 0)
        .map(|b| (b.auction, b.price))
}
