use std::collections::BinaryHeap;

use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

pub trait Notify<T: Timestamp, D> {
    type DrainData: IntoIterator<Item=(T, D)>;

    fn drain(&mut self, frontiers: &[&MutableAntichain<T>]) -> Option<(Capability<T>, Self::DrainData)>;
}

/// Tracks requests for notification and delivers available notifications.
///
/// `TotalOrderFrontierNotificator` is meant to manage the delivery of requested notifications in the
/// presence of inputs that may have outstanding messages to deliver.
/// The notificator inspects the frontiers, as presented from the outside, for each input.
/// Requested notifications can be served only once there are no frontier elements less-or-equal
/// to them, and there are no other pending notification requests less than them. Each will be
/// less-or-equal to itself, so we want to dodge that corner case.
///
/// The `TotalOrderFrontierNotificator` is specialized for totally-ordered time domains. It will
/// only keep a single minimum-element capability around and does not retain pending capabilities
/// for each outstanding notification.
///
/// # Examples
/// ```
/// use std::collections::HashMap;
/// use timely::dataflow::operators::{Input, Inspect, TotalOrderFrontierNotificator};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::channels::pact::Pipeline;
///
/// timely::execute(timely::Configuration::Thread, |worker| {
///     let (mut in1, mut in2) = worker.dataflow(|scope| {
///         let (in1_handle, in1) = scope.new_input();
///         let (in2_handle, in2) = scope.new_input();
///         in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info| {
///             let mut notificator = TotalOrderFrontierNotificator::new();
///             move |input1, input2, output| {
///                 while let Some((time, data)) = input1.next() {
///                     notificator.notify_at_data(time.retain(), data.drain(..));
///                 }
///                 while let Some((time, data)) = input2.next() {
///                     notificator.notify_at_data(time.retain(), data.drain(..));
///                 }
///                 notificator.for_each_data(&[input1.frontier(), input2.frontier()], |time, mut vec, _| {
///                     output.session(&time).give_iterator(vec.drain(..));
///                 });
///             }
///         }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
///
///         (in1_handle, in2_handle)
///     });
///
///     for i in 1..10 {
///         in1.send(i - 1);
///         in1.advance_to(i);
///         in2.send(i - 1);
///         in2.advance_to(i);
///     }
///     in1.close();
///     in2.close();
/// }).unwrap();
/// ```
#[derive(Default)]
pub struct TotalOrderFrontierNotificator<T: Timestamp + TotalOrder, D = ()> {
    capability: Option<Capability<T>>,
    pending: BinaryHeap<OrderReversed<T, D>>,
}

impl<T: Timestamp + TotalOrder> TotalOrderFrontierNotificator<T, ()> {

    /// Allocates a new `TotalOrderFrontierNotificator` with initial capabilities.
    pub fn from<I: IntoIterator<Item=Capability<T>>>(iter: I) -> Self {
        let pending: Vec<_> = iter.into_iter().collect();
        let capability = pending.iter().min_by_key(|x| x.time()).cloned();
        Self {
            capability,
            pending: pending.into_iter().map(|x| OrderReversed{ element: x.time().clone(), data: ()}).collect(),
        }
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, TotalOrderFrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = TotalOrderFrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at(cap.delayed(&time));
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, _| {
    ///                        println!("done with time: {:?}", cap.time());
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>) {
        self.notify_at_data(&cap, cap.time().clone(), ());
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each<'a, F: FnMut(&Capability<T>, T, &mut Self)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
        if let Some((cap, iter)) = self.drain(frontiers) {
            for (time, _data) in iter {
                logic(&cap, time, self)
            }
        }
    }
}

impl<T: Timestamp + TotalOrder, D> TotalOrderFrontierNotificator<T, D> {
    /// Allocates a new `TotalOrderFrontierNotificator`.
    pub fn new() -> Self {
        Self {
            capability: None,
            pending: Default::default(),
//            available: ::std::collections::BinaryHeap::new(),
        }
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, TotalOrderFrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = TotalOrderFrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at_data(cap.delayed(&time), Some(time.inner));
    ///                    });
    ///                    notificator.for_each_data(&[input.frontier()], |cap, data, _| {
    ///                        println!("done with time: {:?} at: {:?}", cap.time(), data);
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at_data(&mut self, cap: &Capability<T>, time: T, data: D) {
        assert!(cap.time().less_equal(&time), "provided capability must be <= notification time, found {:?} and {:?}", cap.time(), time);
        self.pending.push(OrderReversed { element: time, data});
        if self.capability.as_ref().map_or(true, |c| c.time() > cap.time()) {
            self.capability = Some(cap.clone())
        }
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each_data<'a, F: FnMut(&Capability<T>, T, D, &mut Self)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
//        self.make_available(frontiers);
        if let Some((cap, iter)) = self.drain(frontiers) {
            for (time, data) in iter {
                logic(&cap, time, data, self);
            }
        }
    }

    /// Iterates over pending capabilities and their count. The count represents how often a
    /// capability has been requested.
    ///
    /// To make sure all pending capabilities are above the frontier, use `for_each` or exhaust
    /// `next` to consume all available capabilities.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at_data(cap.delayed(&time), Some(time.inner));
    ///                        assert_eq!(notificator.pending().filter(|t| t.0.time() == &time).count(), 1);
    ///                    });
    ///                    notificator.for_each_data(&[input.frontier()], |cap, data, _| {
    ///                        println!("done with time: {:?} at: {:?}", cap.time(), data);
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    pub fn pending(self) -> impl Iterator<Item=(T, D)> {
        self.pending.into_iter().map(|e| (e.element, e.data))
    }
}

impl<T: Timestamp + TotalOrder, D> Notify<T, D> for TotalOrderFrontierNotificator<T, D> {
    type DrainData = Vec<(T, D)>;

    #[inline]
    fn drain(&mut self, frontiers: &[&MutableAntichain<T>]) -> Option<(Capability<T>, Self::DrainData)> {
        let mut available = Vec::new();
        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.

        let mut result = None;
        if !self.pending.is_empty() {

            while self.pending.peek().map_or(false, |or| frontiers.iter().all(|f| !f.less_equal(&or.element))) {
                let min = self.pending.pop().unwrap();
                available.push((min.element, min.data));
            }
//            if !available.is_empty() {
                result = Some((self.capability.as_ref().unwrap().clone(), available));

                if let Some(cap) = self.capability.as_mut() {
                    if let Some(pending) = self.pending.peek() {
                        if cap.time().less_than(&pending.element) {
                            cap.downgrade(&pending.element);
                        }
                    }
                }
//            }
        } else {
            self.capability.take();
        }

        if frontiers.iter().all(|f| f.is_empty()) {
            self.capability.take();
        }
        result
    }
}

struct OrderReversed<T, D> {
    pub element: T,
    pub data: D,
}

impl<T: PartialOrd, D> PartialOrd for OrderReversed<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.element.partial_cmp(&self.element)
    }
}
impl<T: Ord, D> Ord for OrderReversed<T, D> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.element.cmp(&self.element)
    }
}
impl<T: PartialEq, D> PartialEq for OrderReversed<T, D> {
    fn eq(&self, other: &Self) -> bool {
        other.element.eq(&self.element)
    }
}
impl<T: Eq, D> Eq for OrderReversed<T, D> {}
