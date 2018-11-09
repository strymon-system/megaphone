
use ::std::collections::HashMap;
use ::timely::dataflow::{Scope, Stream};

use dynamic_scaling_mechanism::Bin;
use dynamic_scaling_mechanism::operator::StatefulOperator;
use ::event::{Auction, Bid};
use ::calculate_hash;

use queries::{NexmarkInput, NexmarkTimer};

pub fn q45_flex<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S) -> Stream<S, (Auction, Bid)>
{
    let control = input.control(scope);

    let bids = input.bids(scope);
    let auctions = input.auctions(scope);

    bids.stateful_binary_input(&control,
                               &auctions,
                               |bid: &Bid| calculate_hash(&bid.auction),
                               |a: &Auction| calculate_hash(&a.id),
                               "Q4 Auction close",
                               move |state, cap, _time, data, _output| {
                                   for (_, key_id, bid) in &*data {
                                       state.get(*key_id).notificator().notify_at_data(cap, nt.from_nexmark_time(bid.date_time), bid.clone());
                                   }
                               },
                               move |state, cap, _time, data, _output| {
                                   for (_target, key_id, auction) in &*data {
                                       // Request notification for the auction's expiration time, which is used to look into the auctions_state
                                       state.get(*key_id).notificator().notify_at_data(cap, nt.from_nexmark_time(auction.expires), auction.clone());
                                   }
                               },
                               |_cap, data, bid_bin, _auction_bin, _output| {
                                   let state: &mut HashMap<_, _> = bid_bin.state();
                                   for (_time, bid) in data {
                                       // Update bin state
                                       state.entry(bid.auction).or_insert_with(Vec::new).push(bid.clone());
                                   };
                               },
                               |cap, auction_data, bid_bin, _auction_bin: &mut Bin<_, Vec<()>, _>, output| {
                                   let mut session = output.session(&cap);
                                   let bid_state = bid_bin.state();
                                   for (_time, auction) in auction_data {
                                       if let Some(mut bids) = bid_state.remove(&auction.id) {
                                           bids.retain(|b|
                                               auction.date_time <= b.date_time &&
                                                   b.date_time < auction.expires &&
                                                   b.price >= auction.reserve);
                                           bids.sort_by(|b1, b2| b1.price.cmp(&b2.price));
                                           if let Some(winner) = bids.pop() {
                                               session.give((auction.clone(), winner));
                                           }
                                       }
                                   }
                               },
    )
}
