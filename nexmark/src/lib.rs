extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate timely;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate rand;
extern crate streaming_harness;
extern crate dynamic_scaling_mechanism;
extern crate fnv;

pub mod config;
pub mod event;
pub mod tools;

pub mod queries;


use std::hash::Hash;
use std::hash::Hasher;

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}
