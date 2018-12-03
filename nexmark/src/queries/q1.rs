use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::{Map};

use ::event::Bid;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q1<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, Bid>
{
    input.bids(scope)
        .map_in_place(|b| b.price = (b.price * 89) / 100)
}
