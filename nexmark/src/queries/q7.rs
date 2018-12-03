
use ::timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::{Capability, Map, Operator};

use ::event::Date;

use {queries::NexmarkInput, queries::NexmarkTimer};

pub fn q7<S: Scope<Timestamp=usize>>(input: &NexmarkInput, nt: NexmarkTimer, scope: &mut S, window_size_ns: usize) -> Stream<S, usize>
{

    input.bids(scope)
        .map(move |b| (Date::new(((*b.date_time / window_size_ns) + 1) * window_size_ns), b.price))
        .unary_frontier(Pipeline, "Q7 Pre-reduce", |_cap, _info| {

            // Tracks the worker-local maximal bid for each capability.
            let mut maxima = Vec::<(Capability<usize>, usize)>::new();

            move |input, output| {
                input.for_each(|time, data| {
                    for (window, price) in data.iter().cloned() {
                        if let Some(position) = maxima.iter().position(|x| *(x.0).time() == nt.from_nexmark_time(window)) {
                            if maxima[position].1 < price {
                                maxima[position].1 = price;
                            }
                        } else {
                            maxima.push((time.delayed(&nt.from_nexmark_time(window)), price));
                        }
                    }
                });

                for &(ref capability, price) in maxima.iter() {
                    if !input.frontier.less_than(capability.time()) {
                        output.session(&capability).give((*capability.time(), price));
                    }
                }

                maxima.retain(|(capability, _)| input.frontier.less_than(capability));
            }
        })
        .unary_frontier(Exchange::new(move |x: &(usize, usize)| (x.0 / window_size_ns) as u64), "Q7 All-reduce", |_cap, _info| {

            // Tracks the global maximal bid for each capability.
            let mut maxima = Vec::<(Capability<usize>, usize)>::new();

            move |input, output| {
                input.for_each(|time, data| {
                    for (window, price) in data.iter().cloned() {
                        if let Some(position) = maxima.iter().position(|x| *(x.0).time() == window) {
                            if maxima[position].1 < price {
                                maxima[position].1 = price;
                            }
                        } else {
                            maxima.push((time.delayed(&window), price));
                        }
                    }
                });

                for &(ref capability, price) in maxima.iter() {
                    if !input.frontier.less_than(capability.time()) {
                        output.session(&capability).give(price);
                    }
                }

                maxima.retain(|(capability, _)| input.frontier.less_than(capability));
            }
        })
}
