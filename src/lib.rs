extern crate fnv;
extern crate timely;
extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

pub mod bin_prober;
pub mod distribution;
pub mod stateful;
pub mod state_machine;
pub mod join;
pub mod notificator;
pub mod operator;

use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

/// A control message consisting of a sequence number, a total count of messages to be expected
/// and an instruction.
#[derive(Abomonation, Clone, Debug)]
pub struct Control {
    sequence: u64,
    count: usize,

    inst: ControlInst,
}

/// A bin identifier. Wraps a `usize`.
#[derive(Abomonation, Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Bin(usize);

impl Bin {
    pub fn new(bin: usize) -> Self {
        Bin(bin)
    }
}

type KeyType = u64;
#[derive(Abomonation, Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Key(KeyType);

impl Key {
    pub fn bin(&self) -> usize {
        key_to_bin(self)
    }
}

pub fn key_to_bin(key: &Key) -> usize {
    (key.0 >> ::std::mem::size_of::<KeyType>() * 8 - BIN_SHIFT) as usize
}

impl ::std::ops::Deref for Bin {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ::std::ops::Deref for Key {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A control instruction
#[derive(Abomonation, Clone, Debug)]
pub enum ControlInst {
    /// Provide a new map
    Map(Vec<usize>),
    /// Provide a map update
    Move(Bin, /*worker*/ usize),
    /// No-op
    None,
}

impl Control {
    /// Construct a new `Control`
    pub fn new(sequence: u64, count: usize, inst: ControlInst) -> Self {
        Self { sequence, count, inst }
    }
}

/// A compiled set of control instructions
#[derive(Debug)]
pub struct ControlSet<T> {
    /// Its sequence number
    pub sequence: u64,
    /// The frontier at which to apply the instructions
    pub frontier: Antichain<T>,
    /// Collection of instructions
    pub map: Vec<usize>,
}

impl<T> ControlSet<T> {

    /// Obtain the current bin to destination mapping
    pub fn map(&self) -> &Vec<usize> {
        &self.map
    }

}

#[derive(Default)]
pub struct ControlSetBuilder<T> {
    sequence: Option<u64>,
    frontier: Vec<T>,
    instructions: Vec<ControlInst>,

    count: Option<usize>,
}

impl<T: PartialOrder> ControlSetBuilder<T> {

    pub fn apply(&mut self, control: Control) {
        if self.count.is_none() {
            self.count = Some(control.count);
        }
        if let Some(ref mut count) = self.count {
            assert!(*count > 0, "Received incorrect number of Controls");
            *count -= 1;
        }
        if let Some(sequence) = self.sequence {
            assert_eq!(sequence, control.sequence, "Received control with inconsistent sequence number");
        } else {
            self.sequence = Some(control.sequence);
        }
        match control.inst {
            ControlInst::None => {},
            inst => self.instructions.push(inst),
        };

    }

    pub fn frontier<I: IntoIterator<Item=T>>(&mut self, caps: I) {
        self.frontier.extend(caps);
    }

    pub fn build(self, previous: &ControlSet<T>) -> ControlSet<T> {
        assert_eq!(0, self.count.unwrap_or(0));
        let mut frontier = Antichain::new();
        for f in self.frontier {frontier.insert(f);}

        let mut map = previous.map().clone();

        for inst in self.instructions {
            match inst {
                ControlInst::Map(ref new_map) => {
                    assert_eq!(1 << BIN_SHIFT, new_map.len(), "provided map does not have correct len: {} != {}", 1 << BIN_SHIFT, new_map.len());
                    map.clear();
                    map.extend( new_map.iter());
                },
                ControlInst::Move(Bin(bin), target) => {
                    assert!(bin < (1 << BIN_SHIFT));
                    map[bin] = target
                },
                ControlInst::None => {},
            }
        }

        ControlSet {
            sequence: self.sequence.unwrap(),
            frontier,
            map,
        }
    }
}

#[cfg(feature = "bin-1")]
pub const BIN_SHIFT: usize = 1;
#[cfg(feature = "bin-2")]
pub const BIN_SHIFT: usize = 2;
#[cfg(feature = "bin-3")]
pub const BIN_SHIFT: usize = 3;
#[cfg(feature = "bin-4")]
pub const BIN_SHIFT: usize = 4;
#[cfg(feature = "bin-5")]
pub const BIN_SHIFT: usize = 5;
#[cfg(feature = "bin-6")]
pub const BIN_SHIFT: usize = 6;
#[cfg(feature = "bin-7")]
pub const BIN_SHIFT: usize = 7;
#[cfg(feature = "bin-8")]
pub const BIN_SHIFT: usize = 8;
#[cfg(feature = "bin-9")]
pub const BIN_SHIFT: usize = 9;
#[cfg(feature = "bin-10")]
pub const BIN_SHIFT: usize = 10;
#[cfg(feature = "bin-11")]
pub const BIN_SHIFT: usize = 11;
#[cfg(feature = "bin-12")]
pub const BIN_SHIFT: usize = 12;
#[cfg(feature = "bin-13")]
pub const BIN_SHIFT: usize = 13;
#[cfg(feature = "bin-14")]
pub const BIN_SHIFT: usize = 14;
#[cfg(feature = "bin-15")]
pub const BIN_SHIFT: usize = 15;
#[cfg(feature = "bin-16")]
pub const BIN_SHIFT: usize = 16;
#[cfg(feature = "bin-17")]
pub const BIN_SHIFT: usize = 17;
#[cfg(feature = "bin-18")]
pub const BIN_SHIFT: usize = 18;
#[cfg(feature = "bin-19")]
pub const BIN_SHIFT: usize = 19;
#[cfg(feature = "bin-20")]
pub const BIN_SHIFT: usize = 20;
