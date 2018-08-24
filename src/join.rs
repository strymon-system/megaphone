//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};

use operator::StatefulOperator;
use ::State;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    use ::std::hash::Hasher;
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

trait BinarySkeleton<S, K, V>
    where
        S: Scope, // The containing scope
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
    K: ExchangeData+Hash+Eq,
    V: ExchangeData+Eq, // Input data
{
    fn left_join<V2>(&mut self, other: &Stream<S, (K, V2)>, name: &str, control: &Stream<S, ::Control>) -> Stream<S, (K, V, V2)>
        where
            V2: ExchangeData+Eq,
    {
        self.stateful_binary(&control, other, |t| calculate_hash(&t.0), |t| calculate_hash(&t.0), name, |time, data, state1: &mut State<HashMap<K, V>>, state2: &mut State<HashMap<K, Vec<V2>>>, output| {
            let mut session = output.session(&time);
            for (key_id, (key, value)) in data {
                let bin: &mut HashMap<_, _> = state2.get_state(key_id);
                if let Some(mut d2) = bin.remove(&key) {
                    session.give_iterator(d2.drain(..).map(|d| (key.clone(), value.clone(), d)));
                }
                state1.get_state(key_id).insert(key, value);
            };
        }, |time, data, state1, state2, output| {
            let mut session = output.session(&time);
            for (key_id, (key, value)) in data {
                if let Some(d1) = state1.get_state(key_id).get(&key) {
                    session.give((key.clone(), d1.clone(), value.clone()));
                } else {
                    state2.get_state(key_id).entry(key).or_insert_with(Vec::new).push(value);
                }
            };
        })
    }
}
