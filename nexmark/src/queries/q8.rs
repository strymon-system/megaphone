
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q8<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, usize>
{
    let auctions = input.auctions(scope)
        .map(|a| (a.seller, a.date_time));

    let people = input.auctions(scope)
        .map(|p| (p.id, p.date_time));

    people
        .binary_frontier(
            &auctions,
            Exchange::new(|p: &(usize, _)| p.0 as u64),
            Exchange::new(|a: &(usize, _)| a.0 as u64),
            "Q8 join",
            |_capability, _info| {
                let window_size_ns = 12 * 60 * 60 * 1_000_000_000;
                let mut new_people = std::collections::HashMap::new();
                let mut auctions = Vec::new();

                move |input1, input2, output| {

                    // Notice new people.
                    input1.for_each(|_time, data| {
                        for (person, time) in data.iter().cloned() {
                            new_people.insert(person, time);
                        }
                    });

                    // Notice new auctions.
                    input2.for_each(|time, data| {
                        let mut data_vec = vec![];
                        data.swap(&mut data_vec);
                        auctions.push((time.retain(), data_vec));
                    });

                    // Determine least timestamp we might still see.
                    let complete1 = input1.frontier.frontier().get(0).cloned().unwrap_or(usize::max_value());
                    let complete2 = input2.frontier.frontier().get(0).cloned().unwrap_or(usize::max_value());
                    let complete = std::cmp::min(complete1, complete2);

                    for (capability, auctions) in auctions.iter_mut() {
                        if *capability.time() < complete {
                            {
                                let mut session = output.session(&capability);
                                for &(person, time) in auctions.iter() {
                                    if time < nt.to_nexmark_time(complete) {
                                        if let Some(p_time) = new_people.get(&person) {
                                            if *time < **p_time + window_size_ns {
                                                session.give(person);
                                            }
                                        }
                                    }
                                }
                                auctions.retain(|&(_, time)| time >= nt.to_nexmark_time(complete));
                            }
                            if let Some(minimum) = auctions.iter().map(|x| x.1).min() {
                                capability.downgrade(&nt.from_nexmark_time(minimum));
                            }
                        }
                    }
                    auctions.retain(|&(_, ref list)| !list.is_empty());
                    // println!("auctions.len: {:?}", auctions.len());
                    // for thing in auctions.iter() {
                    //     println!("\t{:?} (len: {:?}) vs {:?}", thing.0, thing.1.len(), complete);
                    // }
                }
            })
}
