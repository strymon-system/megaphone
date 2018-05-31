//! General purpose state transition operator.
use std::hash::Hash;

use fnv::FnvHashMap as HashMap;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use ::{BIN_SHIFT, key_to_bin};

use ::stateful::StateStream;

pub trait BinProber<S: Scope> {
    /// Tracks bin occurences per time.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::StateMachine;
    ///
    /// timely::example(|scope| {
    ///
    ///     // these results happen to be right, but aren't guaranteed.
    ///     // the system is at liberty to re-order within a timestamp.
    ///     let result = vec![(0,0), (0,2), (0,6), (0,12), (0,20),
    ///                       (1,1), (1,4), (1,9), (1,16), (1,25)];
    ///
    ///         (0..10).to_stream(scope)
    ///                .map(|x| (x % 2, x))
    ///                .state_machine(
    ///                    |_key, val, agg| { *agg += val; (false, Some((*_key, *agg))) },
    ///                    |key| *key as u64
    ///                )
    ///                .inspect(move |x| assert!(result.contains(x)));
    /// });
    /// ```
    fn probe_bins(&mut self) -> (Self, Stream<S, (usize, usize)>)
    where
        Self: Sized;
}

impl<S, D, W, KV> BinProber<S> for StateStream<S, KV, D, W>
    where
        W: ExchangeData,                            // State format on the wire
        D: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,    // per-key state (data)
        KV: ExchangeData,
        S: Scope,
        S::Timestamp: Hash+Eq,
        Self: Sized,
{
    fn probe_bins(&mut self) -> (Self, Stream<S, (usize, usize)>) {
        let mut builder = OperatorBuilder::new("Bin prober".into(), self.stream.scope());

        let mut data_in = builder.new_input(&self.stream, Pipeline);
        let (mut data_out, stream) = builder.new_output();
        let (mut bin_probe_out, bin_probe) = builder.new_output();


        builder.build(move |_capability| {
            let mut data_notificator = FrontierNotificator::new();

            // Data input stash, time -> Vec<V>
            let mut count_map: HashMap<S::Timestamp, Vec<usize>> = Default::default();

            // Handle input data
            move |frontiers| {
                let mut data_out = data_out.activate();
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
                        let contents: &Vec<_> = &*data;
                        for &(_target, key, _) in contents.iter() {
                            map[key_to_bin(key)] += 1;
                        }
                    }
                    data_out.session(&time).give_content(data);
                    data_notificator.notify_at(time.retain());
                });
            }
        });
        (StateStream::new(stream, self.state.clone(),self.probe.clone()), bin_probe)
    }
}
