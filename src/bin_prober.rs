//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::Data;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use ::{Key, BIN_SHIFT, key_to_bin};

pub trait BinProber<S: Scope> {
    /// Tracks bin occurrences per time.
    ///
    /// #Examples
    /// ```
    /// ```
    fn probe_bins(&mut self) -> Stream<S, (usize, usize)>
    where
        Self: Sized;
}

impl<S, V> BinProber<S> for Stream<S, (Key, V)>
    where
        V: Data,                            // State format on the wire
        S: Scope,
        S::Timestamp: Hash+Eq,
        Self: Sized,
{
    fn probe_bins(&mut self) -> Stream<S, (usize, usize)> {
        let mut builder = OperatorBuilder::new("Bin prober".into(), self.scope());

        let mut data_in = builder.new_input(&self, Pipeline);
        let (mut bin_probe_out, bin_probe): (_, Stream<S, (usize, usize)>) = builder.new_output();


        builder.build(move |_capability| {
            let mut data_notificator = FrontierNotificator::new();

            // Data input stash, time -> Vec<V>
            let mut count_map: HashMap<_, Vec<usize>> = Default::default();

            // Handle input data
            move |frontiers| {
                let mut bin_probe_out = bin_probe_out.activate();

                // Analyze control frontier
                data_notificator.for_each(&[&frontiers[0]], |time, _not| {
                    // Check if there are pending control instructions
                    if let Some(vec) = count_map.remove(time.time()) {
                        bin_probe_out.session(&time).give_iterator(vec.into_iter().enumerate());
                    }
                });

                // Read control input
                data_in.for_each(|time, data| {
                    let map = count_map.entry(time.time().clone()).or_insert_with(|| vec![0; 1 << BIN_SHIFT]);
                    {
                        for (key, _) in data.drain(..) {
                            map[key_to_bin(key)] += 1;
                        }
                    }
                    data_notificator.notify_at(time.retain());
                });
            }
        });
        bin_probe
    }
}
