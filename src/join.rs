//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};

use operator::StatefulOperator;
use ::Bin;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    use ::std::hash::Hasher;
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

trait BinarySkeleton<S, K, V>
    where
        S: Scope, // The containing scope
        S::Timestamp: ::timely::order::TotalOrder,
        K: ExchangeData+Hash+Eq,
        V: ExchangeData, // Input data
{
    fn left_join<V2>(&mut self, other: &Stream<S, (K, V2)>, name: &str, control: &Stream<S, ::Control>) -> Stream<S, (K, V, V2)>
        where
            V2: ExchangeData+Eq,
;
}

impl<S, K, V> BinarySkeleton<S, K, V> for Stream<S, (K, V)>
where
    S: Scope, // The containing scope
    S::Timestamp: ::timely::order::TotalOrder,
    K: ExchangeData+Hash+Eq,
    V: ExchangeData+Eq, // Input data
{
    fn left_join<V2>(&mut self, other: &Stream<S, (K, V2)>, name: &str, control: &Stream<S, ::Control>) -> Stream<S, (K, V, V2)>
        where
            V2: ExchangeData+Eq,
    {
        self.stateful_binary(&control, other, |t| calculate_hash(&t.0), |t| calculate_hash(&t.0), name, |cap, data, bin1: &mut Bin<_, HashMap<K, V>, _>, bin2: &mut Bin<_, HashMap<K, Vec<V2>>, _>, output| {
            let mut session = output.session(&cap);
            let bin: &mut HashMap<_, _> = bin2.state();
            for (_time, (key, value)) in data {
                if let Some(mut d2) = bin.remove(&key) {
                    session.give_iterator(d2.drain(..).map(|d| (key.clone(), value.clone(), d)));
                }
                bin1.state().insert(key.clone(), value.clone());
            };
        }, |cap, data, bin1, bin2, output| {
            let mut session = output.session(&cap);
            let state1 = bin1.state();
            let state2 = bin2.state();
            for (_time, (key, value)) in data {
                if let Some(d1) = state1.get(&key) {
                    session.give((key.clone(), d1.clone(), value.clone()));
                } else {
                    state2.entry(key.clone()).or_insert_with(Vec::new).push(value.clone());
                }
            };
        })
    }
}
