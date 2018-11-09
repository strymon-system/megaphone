
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Map;

use dynamic_scaling_mechanism::Bin;
use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q8_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, usize>
{
    let control = input.control(scope);

    let auctions = input.auctions(scope)
        .map(|a| (a.seller, a.date_time));

    let people = input.auctions(scope)
        .map(|p| (p.id, p.date_time));

    let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
    people.stateful_binary(&control, &auctions, |(p, _d)| calculate_hash(p), |(s, _d)| calculate_hash(s), "q8-flex", |_cap, data, people_bin: &mut Bin<_, HashMap<_, _>, _>, _auctions_state: &mut Bin<_, Vec<()>, _>, _output| {
        // Update people state
        for (_time, (person, date)) in data.drain(..) {
            people_bin.state().entry(person as u64).or_insert(*date);
        }
    }, move |cap, data, people_bin, _auctions_bin, output| {
        for (_time, (seller, date)) in data.drain(..) {
            if let Some(p_time) = people_bin.state().get(&(seller as u64)) {
                if *date < *p_time + window_size_ns {
                    output.session(cap).give(seller);
                }
            }
        }
    })
}
