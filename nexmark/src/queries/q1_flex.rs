use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Map;

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::event::{Bid};
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q1_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, dynamic_scaling_mechanism::Key, Bid)>
{
    let control = input.control(scope);

    input.bids(scope)
        .distribute(&control, |bid| calculate_hash(&bid.auction), "q0-flex")
        .map_in_place(|(_, _, b)| b.price = (b.price * 89) / 100)
}
