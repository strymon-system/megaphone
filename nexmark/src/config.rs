use std::io::{Result, Error, ErrorKind};
use std::collections::HashMap;
use std::str::FromStr;

/// This is a simple command line options parser.
#[derive(Clone)]
pub struct Config {
    args: HashMap<String, String>
}

impl Config {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Config{ args: HashMap::new() }
    }

    /// Parses the command line arguments into a new Config object.
    ///
    /// Its parsing strategy is as follows:
    ///   If an argument starts with --, the remaining string is used as the key
    ///   and the next argument as the associated value.
    ///   Otherwise the argument is used as the next positional value, counting
    ///   from zero.
    ///
    pub fn from<I: Iterator<Item=String>>(mut cmd_args: I) -> Result<Self> {
        let mut args = HashMap::new();
        let mut i = 0;
        while let Some(arg) = cmd_args.next() {
            if arg.starts_with("--") {
                match cmd_args.next() {
                    Some(value) => args.insert(format!("{}", &arg[2..]), value),
                    None => return Err(Error::new(ErrorKind::Other, "No corresponding value."))
                };
            } else {
                args.insert(format!("{}", i), arg);
                i = i+1;
            }
        }
        Ok(Config{ args: args })
    }

    /// Inserts the given value for the given key.
    ///
    /// If the key already exists, its value is overwritten.
    #[allow(dead_code)]
    pub fn insert(&mut self, key: &str, value: String) {
        self.args.insert(String::from(key), value);
    }

    /// Returns the value for the given key, if available.
    pub fn get(&self, key: &str) -> Option<String> {
        self.args.get(key).map(|x| x.clone())
    }

    /// Returns the value for the given key automatically parsed if possible.
    pub fn get_as<T: FromStr>(&self, key: &str) -> Option<T> {
        self.args.get(key).map_or(None, |x| x.parse::<T>().ok())
    }

    /// Returns the value for the given key or a default value if the key does not exist.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.args.get(key).map_or(String::from(default), |x| x.clone())
    }

    /// Returns the value for the given key automatically parsed, or a default value if the key does not exist.
    pub fn get_as_or<T: FromStr>(&self, key: &str, default: T) -> T {
        self.get_as(key).unwrap_or(default)
    }
}



use std::f64::consts::PI;

// type Id = usize;
// type Date = usize;

// const MIN_STRING_LENGTH: usize = 3;
const BASE_TIME: usize = 0; //1436918400_000;

fn split_string_arg(string: String) -> Vec<String> {
    string.split(",").map(String::from).collect::<Vec<String>>()
}

// trait NEXMarkRng {
//     fn gen_string(&mut self, usize) -> String;
//     fn gen_price(&mut self) -> usize;
// }

// impl NEXMarkRng for StdRng {
//     fn gen_string(&mut self, max: usize) -> String {
//         let len = self.gen_range(MIN_STRING_LENGTH, max);
//         String::from((0..len).map(|_|{
//             if self.gen_range(0, 13) == 0 { String::from(" ") }
//             else { from_u32('a' as u32+self.gen_range(0, 26)).unwrap().to_string() }
//         }).collect::<Vec<String>>().join("").trim())
//     }

//     fn gen_price(&mut self) -> usize {
//         (10.0_f32.powf(self.gen::<f32>() * 6.0) * 100.0).round() as usize
//     }
// }

#[derive(PartialEq)]
enum RateShape {
    Square,
    Sine,
}

#[derive(Clone)]
pub struct NEXMarkConfig {
    pub active_people: usize,
    pub in_flight_auctions: usize,
    pub out_of_order_group_size: usize,
    pub hot_seller_ratio: usize,
    pub hot_auction_ratio: usize,
    pub hot_bidder_ratio: usize,
    pub first_event_id: usize,
    pub first_event_number: usize,
    pub base_time_ns: usize,
    pub step_length: usize,
    pub events_per_epoch: usize,
    pub epoch_period: f32,
    pub inter_event_delays_ns: Vec<f64>,
    // Originally constants
    pub num_categories: usize,
    pub auction_id_lead: usize,
    pub hot_seller_ratio_2: usize,
    pub hot_auction_ratio_2: usize,
    pub hot_bidder_ratio_2: usize,
    pub person_proportion: usize,
    pub auction_proportion: usize,
    pub bid_proportion: usize,
    pub proportion_denominator: usize,
    pub first_auction_id: usize,
    pub first_person_id: usize,
    pub first_category_id: usize,
    pub person_id_lead: usize,
    pub sine_approx_steps: usize,
    pub us_states: Vec<String>,
    pub us_cities: Vec<String>,
    pub first_names: Vec<String>,
    pub last_names: Vec<String>,
}

impl NEXMarkConfig {
    pub fn new(config: &Config) -> Self{
        let active_people = config.get_as_or("active-people", 1000);
        let in_flight_auctions = config.get_as_or("in-flight-auctions", 100);
        let out_of_order_group_size = config.get_as_or("out-of-order-group-size", 1);
        let hot_seller_ratio = config.get_as_or("hot-seller-ratio", 4);
        let hot_auction_ratio = config.get_as_or("hot-auction-ratio", 2);
        let hot_bidder_ratio = config.get_as_or("hot-bidder-ratio", 4);
        let first_event_id = config.get_as_or("first-event-id", 0);
        let first_event_number = config.get_as_or("first-event-number", 0);
        let num_categories = config.get_as_or("num-categories", 5);
        let auction_id_lead = config.get_as_or("auction-id-lead", 10);
        let hot_seller_ratio_2 = config.get_as_or("hot-seller-ratio-2", 100);
        let hot_auction_ratio_2 = config.get_as_or("hot-auction-ratio-2", 100);
        let hot_bidder_ratio_2 = config.get_as_or("hot-bidder-ratio-2", 100);
        let person_proportion = config.get_as_or("person-proportion", 1);
        let auction_proportion = config.get_as_or("auction-proportion", 3);
        let bid_proportion = config.get_as_or("bid-proportion", 46);
        let proportion_denominator = person_proportion+auction_proportion+bid_proportion;
        let first_auction_id = config.get_as_or("first-auction-id", 1000);
        let first_person_id = config.get_as_or("first-person-id", 1000);
        let first_category_id = config.get_as_or("first-category-id", 10);
        let person_id_lead = config.get_as_or("person-id-lead", 10);
        let sine_approx_steps = config.get_as_or("sine-approx-steps", 10);
        let base_time_ns = config.get_as_or("base-time", BASE_TIME);
        let us_states = split_string_arg(config.get_or("us-states", "AZ,CA,ID,OR,WA,WY"));
        let us_cities = split_string_arg(config.get_or("us-cities", "phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne"));
        let first_names = split_string_arg(config.get_or("first-names", "peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter"));
        let last_names = split_string_arg(config.get_or("last-names", "shultz,abrams,spencer,white,bartels,walton,smith,jones,noris"));
        let rate_shape = if config.get_or("rate-shape", "sine") == "sine"{ RateShape::Sine }else{ RateShape::Square };
        let rate_period = config.get_as_or("rate-period", 600);
        let first_rate = config.get_as_or("first-event-rate", config.get_as_or("events-per-second", 1_000));
        let next_rate = config.get_as_or("next-event-rate", first_rate);
        let ns_per_unit = config.get_as_or("us-per-unit", 1_000_000_000); // Rate is in μs
        let generators = config.get_as_or("threads", 1) as f64;
        // Calculate inter event delays array.
        let mut inter_event_delays_ns = Vec::new();
        let rate_to_period = |r| (ns_per_unit) as f64 / r as f64;
        if first_rate == next_rate {
            inter_event_delays_ns.push(rate_to_period(first_rate) * generators);
        } else {
            match rate_shape {
                RateShape::Square => {
                    inter_event_delays_ns.push(rate_to_period(first_rate) * generators);
                    inter_event_delays_ns.push(rate_to_period(next_rate) * generators);
                },
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays_ns.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if rate_shape == RateShape::Square { 2 } else { sine_approx_steps };
        let step_length = (rate_period + n - 1) / n;
        let events_per_epoch = 0;
        let epoch_period = 0.0;
        if inter_event_delays_ns.len() > 1 {
            panic!("non-constant rate not supported");
            // for inter_event_delay in &inter_event_delays {
            //     let num_events_for_this_cycle = (step_length * 1_000_000) as f64 / inter_event_delay;
            //     events_per_epoch += num_events_for_this_cycle.round() as usize;
            //     epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
            // }
        }
        NEXMarkConfig {
            active_people: active_people,
            in_flight_auctions: in_flight_auctions,
            out_of_order_group_size: out_of_order_group_size,
            hot_seller_ratio: hot_seller_ratio,
            hot_auction_ratio: hot_auction_ratio,
            hot_bidder_ratio: hot_bidder_ratio,
            first_event_id: first_event_id,
            first_event_number: first_event_number,
            base_time_ns: base_time_ns,
            step_length: step_length,
            events_per_epoch: events_per_epoch,
            epoch_period: epoch_period,
            inter_event_delays_ns: inter_event_delays_ns,
            // Originally constants
            num_categories: num_categories,
            auction_id_lead: auction_id_lead,
            hot_seller_ratio_2: hot_seller_ratio_2,
            hot_auction_ratio_2: hot_auction_ratio_2,
            hot_bidder_ratio_2: hot_bidder_ratio_2,
            person_proportion: person_proportion,
            auction_proportion: auction_proportion,
            bid_proportion: bid_proportion,
            proportion_denominator: proportion_denominator,
            first_auction_id: first_auction_id,
            first_person_id: first_person_id,
            first_category_id: first_category_id,
            person_id_lead: person_id_lead,
            sine_approx_steps: sine_approx_steps,
            us_states: us_states,
            us_cities: us_cities,
            first_names: first_names,
            last_names: last_names,
        }
    }

    ///
    pub fn event_timestamp_ns(&self, event_number: usize) -> usize {
        // if self.inter_event_delays.len() == 1 {
            return self.base_time_ns + ((event_number as f64 * self.inter_event_delays_ns[0]) as usize);
        // }

        // let epoch = event_number / self.events_per_epoch;
        // let mut event_i = event_number % self.events_per_epoch;
        // let mut offset_in_epoch = 0.0;
        // for inter_event_delay in &self.inter_event_delays {
        //     let num_events_for_this_cycle = (self.step_length * 1_000_000) as f32 / inter_event_delay;
        //     if self.out_of_order_group_size < num_events_for_this_cycle.round() as usize {
        //         let offset_in_cycle = event_i as f32 * inter_event_delay;
        //         return self.base_time + (epoch as f32 * self.epoch_period + offset_in_epoch + offset_in_cycle / 1000.0).round() as usize;
        //     }
        //     event_i -= num_events_for_this_cycle.round() as usize;
        //     offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
        // }
        // return 0
    }

    pub fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }
}

pub struct NexMarkInputTimes {
    config: NEXMarkConfig,
    next: Option<u64>,
    events_so_far: usize,
    end: u64,
    time_dilation: usize,
}

impl NexMarkInputTimes {
    pub fn new(config: NEXMarkConfig, end: u64, time_dilation: usize) -> Self {
        let mut this = Self {
            config,
            next: None,
            events_so_far: 0,
            end,
            time_dilation,
        };
        this.make_next();
        this
    }

    fn make_next(&mut self) {
        let ts = self.config.event_timestamp_ns(
            self.config.next_adjusted_event(self.events_so_far)) as u64;
        let ts = ts / self.time_dilation as u64;
        if ts < self.end {
            self.events_so_far += 1;
            self.next = Some(ts);
        } else {
            self.next = None;
        };
    }
}

impl Iterator for NexMarkInputTimes {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let s = self.next;
        self.make_next();
        s
    }
}

impl ::streaming_harness::input::InputTimeResumableIterator<u64> for NexMarkInputTimes {
    fn peek(&mut self) -> Option<&u64> {
        self.next.as_ref()
    }

    fn end(&self) -> bool {
        self.next.is_none()
    }
}

