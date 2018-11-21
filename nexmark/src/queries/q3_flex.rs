
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Filter;

use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::event::{Auction, Person};
use ::calculate_hash;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q3_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (String, String, String, usize)>
{
    let control = input.control(scope);

    let auctions = input.auctions(scope)
        .filter(|a| a.category == 10);

    let people = input.people(scope)
        .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

    auctions.stateful_binary(&control, &people, |a| calculate_hash(&a.seller), |p| calculate_hash(&p.id), "q3-flex join", |cap, data, auction_bin, people_bin, output| {
        let mut session = output.session(&cap);
        let people_state: &mut HashMap<_, Person> = people_bin.state();
        for (_time, auction) in data.drain(..) {
            if let Some(mut person) = people_state.get(&auction.seller) {
                session.give((person.name.clone(),
                              person.city.clone(),
                              person.state.clone(),
                              auction.id));
            }
            // Update auction state
            let bin: &mut HashMap<_, Auction> = auction_bin.state();
            bin.insert(auction.seller, auction);
        };
    }, |cap, data, auction_bin, people_bin, output| {
        let mut session = output.session(&cap);
        let auction_state = auction_bin.state();
        for (_time, person) in data.drain(..) {
            if let Some(mut auction) = auction_state.get(&person.id) {
                session.give((person.name.clone(),
                              person.city.clone(),
                              person.state.clone(),
                              auction.id));
            }
            // Update people state
            people_bin.state().insert(person.id, person);
        };
    })
}
