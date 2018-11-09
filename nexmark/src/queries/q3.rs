
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Filter, Operator};

use ::event::{Auction, Person};

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q3<S: Scope<Timestamp=usize>>(input: &NexmarkInput, _nt: NexmarkTimer, scope: &mut S) -> Stream<S, (String, String, String, usize)>
{
    let auctions = input.auctions(scope)
        .filter(|a| a.category == 10);

    let people = input.people(scope)
        .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");

    let mut auctions_buffer = vec![];
    let mut people_buffer = vec![];

    auctions
        .binary(
            &people,
            Exchange::new(|a: &Auction| a.seller as u64),
            Exchange::new(|p: &Person| p.id as u64),
            "Q3 Join",
            |_capability, _info| {
                let mut state1 = HashMap::new();
                let mut state2 = HashMap::<usize, Person>::new();

                move |input1, input2, output| {

                    // Process each input auction.
                    input1.for_each(|time, data| {
                        data.swap(&mut auctions_buffer);
                        let mut session = output.session(&time);
                        for auction in auctions_buffer.drain(..) {
                            if let Some(person) = state2.get(&auction.seller) {
                                session.give((
                                    person.name.clone(),
                                    person.city.clone(),
                                    person.state.clone(),
                                    auction.id));
                            }
                            state1.entry(auction.seller).or_insert(Vec::new()).push(auction);
                        }
                    });

                    // Process each input person.
                    input2.for_each(|time, data| {
                        data.swap(&mut people_buffer);
                        let mut session = output.session(&time);
                        for person in people_buffer.drain(..) {
                            if let Some(auctions) = state1.get(&person.id) {
                                for auction in auctions.iter() {
                                    session.give((
                                        person.name.clone(),
                                        person.city.clone(),
                                        person.state.clone(),
                                        auction.id));
                                }
                            }
                            state2.insert(person.id, person);
                        }
                    });
                }
            }
        )
}
