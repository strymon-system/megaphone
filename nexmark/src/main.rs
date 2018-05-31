extern crate rand;
extern crate timely;
extern crate nexmark;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Filter, Probe};

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // Declare re-used input and probe handles.
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Q0: Do nothing in particular.
        worker.dataflow(|scope| {
            input.to_stream(scope)
                 .probe_with(&mut probe);
        });

        // Q1: Convert bids to euros.
        worker.dataflow(|scope| {
            input.to_stream(scope)
                 .flat_map(|e| nexmark::event::Bid::from(e))
                 .map_in_place(|b| b.price = (b.price * 89)/100)
                 .probe_with(&mut probe);
        });

        // Q2: Filter some auctions.
        worker.dataflow(|scope| {
            let auction_skip = 123;
            input.to_stream(scope)
                 .flat_map(|e| nexmark::event::Bid::from(e))
                 .filter(move |b| b.auction % auction_skip == 0)
                 .map(|b| (b.auction, b.price))
                 .probe_with(&mut probe);
        });

        let config1 = nexmark::config::Config::new();
        let mut config = nexmark::config::NEXMarkConfig::new(&config1);

        // Establish a start of the computation.
        let elapsed = timer.elapsed();
        let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;
        config.base_time_ns = elapsed_ns;
        let mut requested_ns = elapsed_ns;

        use rand::{StdRng, SeedableRng};
        let mut rng = StdRng::from_seed([0;32]);

        let mut counts = vec![[0u64; 16]; 64];

        let mut event_id = 0;
        let duration_ns = 10_000_000_000;
        while requested_ns < duration_ns {

            let elapsed = timer.elapsed();
            let elapsed_ns = (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;

            // Determine completed ns.
            let acknowledged_ns: usize = probe.with_frontier(|frontier| frontier[0].inner);

            // Record completed measurements.
            while requested_ns < acknowledged_ns {
                if requested_ns > duration_ns / 2 {
                    let count_index = (elapsed_ns - requested_ns).next_power_of_two().trailing_zeros() as usize;
                    let low_bits = ((elapsed_ns - requested_ns) >> (count_index - 5)) & 0xF;
                    counts[count_index][low_bits as usize] += 1;
                }
                requested_ns += 1_000_000;
            }

            // Insert random events as long as their times precede `elapsed_ns`.
            let mut next_event = nexmark::event::Event::create(event_id, &mut rng, &mut config);
            while next_event.time() <= elapsed_ns {
                input.send(next_event);
                event_id += 1;
                next_event = nexmark::event::Event::create(event_id, &mut rng, &mut config);
            }

            input.advance_to(elapsed_ns);
            while probe.less_than(input.time()) { worker.step(); }
        }

        // Once complete, report ccdf measurements.
        if worker.index() == 0 {

            let mut results = Vec::new();
            let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
            let mut sum = 0;
            for index in (10 .. counts.len()).rev() {
                for sub in (0 .. 16).rev() {
                    if sum > 0 && sum < total {
                        let latency = (1 << (index-1)) + (sub << (index-5));
                        let fraction = (sum as f64) / (total as f64);
                        results.push((latency, fraction));
                    }
                    sum += counts[index][sub];
                }
            }
            for (latency, fraction) in results.drain(..).rev() {
                println!("{}\t{}", latency, fraction);
            }
        }

    }).expect("timely execution failed");
}