
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Map;

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q4_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (usize, usize)>
{
    let control = input.control(scope);

    input.closed_auctions_flex(scope)
        .map(|(a, b)| (a.category, b.price))
        .stateful_unary(&control, |x: &(usize, usize)| calculate_hash(&x.0), "Q4 Average",
                        |cap, data, bin, output| {
                            let mut session = output.session(&cap);
                            let state: &mut HashMap<_, _> = bin.state();
                            for (_time, (category, price)) in data.drain(..) {
                                let entry = state.entry(category).or_insert((0usize, 0usize));
                                entry.0 += price;
                                entry.1 += 1;
                                session.give((category, entry.0 / entry.1));
                            }
                        })
}
