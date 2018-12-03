
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Operator};

use ::event::{Auction, Bid};

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q4_q6_common<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, (Auction, Bid)>
{
    let bids = input.bids(scope);
    let auctions = input.auctions(scope);

    bids.binary_frontier(
        &auctions,
        Exchange::new(|b: &Bid| b.auction as u64),
        Exchange::new(|a: &Auction| a.id as u64),
        "Q4 Auction close",
        |_capability, _info| {
            let mut state: HashMap<_, (Option<_>, Vec<Bid>)> = std::collections::HashMap::new();
            let mut opens = std::collections::BinaryHeap::new();

            let mut capability: Option<Capability<usize>> = None;
            use std::collections::hash_map::Entry;
            use std::cmp::Reverse;

            fn is_valid_bid(bid: &Bid, auction: &Auction) -> bool {
                bid.price >= auction.reserve && auction.date_time <= bid.date_time && bid.date_time < auction.expires
            }

            move |input1, input2, output| {

                // Record each bid.
                // NB: We don't summarize as the max, because we don't know which are valid.
                input1.for_each(|time, data| {
                    for bid in data.iter().cloned() {
//                                        eprintln!("[{:?}] bid: {:?}", time.time().inner, bid);
                        let mut entry = state.entry(bid.auction).or_insert((None, Vec::new()));
                        if let Some(ref auction) = entry.0 {
                            debug_assert!(entry.1.len() <= 1);
                            if is_valid_bid(&bid, auction) {
                                // bid must fall between auction creation and expiration
                                if let Some(existing) = entry.1.get(0).cloned() {
                                    if existing.price < bid.price {
                                        entry.1[0] = bid;
                                    }
                                } else {
                                    entry.1.push(bid);
                                }
                            }
                        } else {
                            opens.push((Reverse(bid.date_time), bid.auction));
                            if capability.as_ref().map(|c| nt.to_nexmark_time(*c.time()) <= bid.date_time) != Some(true) {
                                capability = Some(time.delayed(&nt.from_nexmark_time(bid.date_time)));
                            }
                            entry.1.push(bid);
                        }
                    }
                });

                // Record each auction.
                input2.for_each(|time, data| {
                    for auction in data.iter().cloned() {
//                                        eprintln!("[{:?}] auction: {:?}", time.time().inner, auction);
                        if capability.as_ref().map(|c| nt.to_nexmark_time(*c.time()) <= auction.expires) != Some(true) {
                            capability = Some(time.delayed(&nt.from_nexmark_time(auction.expires)));
                        }
                        opens.push((Reverse(auction.expires), auction.id));
                        let mut entry = state.entry(auction.id).or_insert((None, Vec::new()));
                        debug_assert!(entry.0.is_none());
                        entry.1.retain(|bid| is_valid_bid(&bid, &auction));
                        if let Some(bid) = entry.1.iter().max_by_key(|bid| bid.price).cloned() {
                            entry.1.clear();
                            entry.1.push(bid);
                        }
                        entry.0 = Some(auction);
                    }
                });

                // Use frontiers to determine which auctions to close.
                if let Some(ref capability) = capability {
                    let complete1 = input1.frontier.frontier().get(0).cloned().unwrap_or(usize::max_value());
                    let complete2 = input2.frontier.frontier().get(0).cloned().unwrap_or(usize::max_value());
                    let complete = std::cmp::min(complete1, complete2);

                    let mut session = output.session(capability);
                    while opens.peek().map(|x| complete == usize::max_value() || (x.0).0 < nt.to_nexmark_time(complete)) == Some(true) {
//                                        eprintln!("[{:?}] opens.len(): {} state.len(): {} {:?}", capability.time().inner, opens.len(), state.len(), state.iter().map(|x| (x.1).1.len()).sum::<usize>());

                        let (Reverse(time), auction) = opens.pop().unwrap();
                        let mut entry = state.entry(auction);
                        if let Entry::Occupied(mut entry) = entry {
                            let delete = {
                                let auction_bids = entry.get_mut();
                                if let Some(ref auction) = auction_bids.0 {
                                    if time == auction.expires {
                                        // Auction expired, clean up state
                                        if let Some(winner) = auction_bids.1.pop() {
                                            session.give((auction.clone(), winner));
                                        }
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    auction_bids.1.retain(|bid| bid.date_time > time);
                                    auction_bids.1.is_empty()
                                }
                            };
                            if delete {
                                entry.remove_entry();
                            }
                        }
                    }
                }

                // Downgrade capability.
                if let Some(head) = opens.peek() {
                    capability.as_mut().map(|c| c.downgrade(&nt.from_nexmark_time((head.0).0)));
                } else {
                    capability = None;
                }
            }
        }
    )
}
