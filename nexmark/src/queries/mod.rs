use ::std::rc::Rc;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;

use dynamic_scaling_mechanism::Control;
use event::{Bid, Auction, Person, Date};

mod q3;
mod q3_flex;
mod q45;
mod q45_flex;
mod q4;
mod q4_flex;
mod q5;
mod q5_flex;
mod q6;
mod q6_flex;
mod q7;
mod q7_flex;
mod q8;
mod q8_flex;

pub use self::q3::q3;
pub use self::q3_flex::q3_flex;
pub use self::q45::q45;
pub use self::q45_flex::q45_flex;
pub use self::q4::q4;
pub use self::q4_flex::q4_flex;
pub use self::q5::q5;
pub use self::q5_flex::q5_flex;
pub use self::q6::q6;
pub use self::q6_flex::q6_flex;
pub use self::q7::q7;
pub use self::q7_flex::q7_flex;
pub use self::q8::q8;
pub use self::q8_flex::q8_flex;

pub struct NexmarkInput<'a> {
    pub control: &'a Rc<EventLink<usize, Control>>,
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    pub fn control<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Control> {
        Some(self.control.clone()).replay_into(scope)
    }

    pub fn bids<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Bid> {
        Some(self.bids.clone()).replay_into(scope)
    }

    pub fn auctions<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Auction> {
        Some(self.auctions.clone()).replay_into(scope)
    }

    pub fn people<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Person> {
        Some(self.people.clone()).replay_into(scope)
    }

    pub fn closed_auctions<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions.clone()).replay_into(scope)
    }

    pub fn closed_auctions_flex<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions_flex.clone()).replay_into(scope)
    }
}


#[derive(Copy, Clone)]
pub struct NexmarkTimer {
    pub time_dilation: usize
}

impl NexmarkTimer {

    #[inline(always)]
    fn to_nexmark_time (&self, x: usize) -> Date{
        debug_assert!(x.checked_mul(self.time_dilation).is_some(), "multiplication failed: {} * {}", x, self.time_dilation);
        Date::new(x * self.time_dilation)
    }

    #[inline(always)]
    fn from_nexmark_time(&self, x: Date) -> usize{
        *x / self.time_dilation
    }

}
