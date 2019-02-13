//! Specialized notificators for Megaphone.

use std::collections::BinaryHeap;

use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

/// Common trait to all notificator implementations.
///
/// Currently only prvides `drain`. TODO: Allow to schedule notifications.
pub trait Notify<T: Timestamp, D> {
    /// Drain all pending notifications that are not in advance of `frontiers`.
    ///
    /// If drain returns `Some(cap)` this indicates that notifications were enqueud to `buffer`.
    /// The buffer may be cleared by `drain`.
    fn drain(&mut self, frontiers: &[&MutableAntichain<T>], buffer: &mut Vec<(T, D)>) -> Option<Capability<T>>;
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
/// extern crate timely;
/// extern crate dynamic_scaling_mechanism;
/// use std::collections::HashMap;
/// use timely::dataflow::operators::{Input, Inspect};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::channels::pact::Pipeline;
/// use ::dynamic_scaling_mechanism::notificator::{Notify, TotalOrderFrontierNotificator};
///
/// timely::execute(timely::Configuration::Thread, |worker| {
///     let (mut in1, mut in2) = worker.dataflow::<usize, _, _>(|scope| {
///         let (in1_handle, in1) = scope.new_input();
///         let (in2_handle, in2) = scope.new_input();
///         in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info| {
///             let mut notificator = TotalOrderFrontierNotificator::new();
///             let mut notificator_buffer = Vec::new();
///             let mut input_buffer: Vec<usize> = Vec::new();
///             let mut input2_buffer: Vec<usize> = Vec::new();
///             move |input1, input2, output| {
///                 while let Some((time, data)) = input1.next() {
///                     let cap = time.retain();
///                     data.swap(&mut input_buffer);
///                     for d in input_buffer.drain(..) {
///                         notificator.notify_at_data(&cap, *cap.time(), d);
///                     }
///                 }
///                 while let Some((time, data)) = input2.next() {
///                     let cap = time.retain();
///                     data.swap(&mut input2_buffer);
///                     for d in input2_buffer.drain(..) {
///                         notificator.notify_at_data(&cap, *cap.time(), d);
///                     }
///                 }
///                 if let Some(cap) = notificator.drain(&[input1.frontier(), input2.frontier()], &mut notificator_buffer) {
///                     for (time, data) in notificator_buffer.drain(..) {
///                         output.session(&cap).give(data);
///                     }
///                 }
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
    /// extern crate timely;
    /// extern crate dynamic_scaling_mechanism;
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use dynamic_scaling_mechanism::notificator::TotalOrderFrontierNotificator;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = TotalOrderFrontierNotificator::new();
    ///                let mut buffer = Vec::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        data.swap(&mut buffer);
    ///                        output.session(&cap).give_iterator(buffer.drain(..));
    ///                        let mut time = cap.time().clone();
    ///                        time += 1;
    ///                        notificator.notify_at(&cap.delayed(&time));
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, time, _| {
    ///                        println!("done with time: {:?}", time);
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: &Capability<T>) {
        self.notify_at_data(cap, cap.time().clone(), ());
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each<'a, F: FnMut(&Capability<T>, T, &mut Self)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
        let mut vec = Vec::new();
        if let Some(cap) = self.drain(frontiers, &mut vec) {
            for (time, _data) in vec {
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
    /// extern crate timely;
    /// extern crate dynamic_scaling_mechanism;
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use dynamic_scaling_mechanism::notificator::TotalOrderFrontierNotificator;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = TotalOrderFrontierNotificator::new();
    ///                let mut buffer = Vec::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        data.swap(&mut buffer);
    ///                        output.session(&cap).give_iterator(buffer.drain(..));
    ///                        let mut time = cap.time().clone();
    ///                        time += 1;
    ///                        notificator.notify_at_data(&cap.retain(), time, ());
    ///                    });
    ///                    notificator.for_each_data(&[input.frontier()], |cap, time, data, _| {
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
        let mut vec = Vec::new();
        if let Some(cap) = self.drain(frontiers, &mut vec) {
            for (time, data) in vec {
                logic(&cap, time, data, self);
            }
        }
    }

    /// Descructures the notificator to obtain pending `(time, data)` pairs.
    pub fn pending(self) -> impl Iterator<Item=(T, D)> {
        self.pending.into_iter().map(|e| (e.element, e.data))
    }
}

impl<T: Timestamp + TotalOrder, D> Notify<T, D> for TotalOrderFrontierNotificator<T, D> {

    #[inline]
    fn drain(&mut self, frontiers: &[&MutableAntichain<T>], buffer: &mut Vec<(T, D)>) -> Option<Capability<T>> {
        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.

        let mut result = None;
        if !self.pending.is_empty() {
            buffer.clear();
            while self.pending.peek().map_or(false, |or| frontiers.iter().all(|f| !f.less_equal(&or.element))) {
                let min = self.pending.pop().unwrap();
                buffer.push((min.element, min.data));
            }
            if !buffer.is_empty() {
                result = Some(self.capability.as_ref().unwrap().clone());
            }

            if let Some(cap) = self.capability.as_mut() {
                if let Some(pending) = self.pending.peek() {
                    if cap.time().less_than(&pending.element) {
                        cap.downgrade(&pending.element);
                    }
                }
            }
        }
        if self.pending.is_empty() {
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
