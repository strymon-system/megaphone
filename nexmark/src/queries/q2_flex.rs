use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Filter, Map};

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q2_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{
    let control = input.control(scope);

    let auction_skip = 123;
    let state_stream = input.bids(scope)
        .distribute(&control, |bid| calculate_hash(&bid.auction), "q2-flex");
    state_stream
        .filter(move |(_, _, b)| b.auction % auction_skip == 0)
        .map(|(_, _, b)| (b.auction, b.price))

}
