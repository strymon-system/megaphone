use streaming_harness::util::ToNanos;
use dynamic_scaling_mechanism::{ControlInst};

#[derive(Clone, Copy, Debug)]
pub enum ParseError {}

#[derive(Debug, PartialEq, Eq)]
pub enum ExperimentMapMode {
    None,
    Sudden,
    //    OneByOne,
    Fluid,
    File(String),
}

impl ::std::str::FromStr for ExperimentMapMode {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<ExperimentMapMode, Self::Err> {
        let map_mode = match s {
            "none" => ExperimentMapMode::None,
            "sudden" => ExperimentMapMode::Sudden,
//            "one-by-one" => ExperimentMapMode::OneByOne,
            "fluid" => ExperimentMapMode::Fluid,
            file_name => ExperimentMapMode::File(file_name.to_string()),
        };
        Ok(map_mode)
    }
}

impl ExperimentMapMode {
    pub fn instructions(&self, peers: usize, duration_ns: u64) -> Result<Vec<(u64, Vec<ControlInst>)>, String> {
        match self {
            ExperimentMapMode::None => {
                let mut map = vec![0; 1 << ::dynamic_scaling_mechanism::BIN_SHIFT];
                for (i, element) in map.iter_mut().enumerate() {
                    *element = i % peers;
                };
                Ok(vec![(0, vec![ControlInst::Map(map)])])
            }
            ExperimentMapMode::Sudden => {
                let mut map = vec![0; 1 << ::dynamic_scaling_mechanism::BIN_SHIFT];
                // TODO(moritzo) HAAAACCCCKKK
                if peers != 2 {
                    for (i, v) in map.iter_mut().enumerate() {
                        *v = ((i / 2) * 2 + (i % 2) * peers / 2) % peers;
                    }
                }
                let initial_map = map.clone();
                for i in 0..map.len() {
                    map[i] = i % peers;

//                    if i % batches_per_migration == batches_per_migration - 1 {
//                        eprintln!("debug: setting up reconfiguration: {:?}", map);
//                        control_plan.push((rounds * 1_000_000_000, Control::new(control_counter, 1, ControlInst::Map(map.clone()))));
//                        control_counter += 1;
//                    }
                };
                Ok(vec![(duration_ns/3, vec![ControlInst::Map(initial_map)]), (2*duration_ns/3, vec![ControlInst::Map(map)])])
            },
            ExperimentMapMode::Fluid => {
                let mut map = vec![0; 1 << ::dynamic_scaling_mechanism::BIN_SHIFT];
                // TODO(moritzo) HAAAACCCCKKK
                if peers != 2 {
                    for (i, v) in map.iter_mut().enumerate() {
                        *v = ((i / 2) * 2 + (i % 2) * peers / 2) % peers;
                    }
                }
                let initial_map = map.clone();
                let mut configurations = Vec::new();
                configurations.push((duration_ns / 3, vec![ControlInst::Map(initial_map)]));
                for i in 0..map.len() {
                    map[i] = i % peers;
                    configurations.push((2 * duration_ns / 3, vec![ControlInst::Move(::dynamic_scaling_mechanism::BinId::new(i), i % peers)]));
                };
                Ok(configurations)
            },
            ExperimentMapMode::File(migrations_file) => {
                let f = ::std::fs::File::open(migrations_file).map_err(|e| e.to_string())?;
                let file = ::std::io::BufReader::new(&f);
                use ::std::io::BufRead;
                let mut instructions = Vec::new();
                for line in file.lines() {
                    let line = line.map_err(|e| e.to_string())?;
                    let mut parts = line.split_whitespace();
                    let indicator = parts.next().expect("Missing map/diff indicator");
                    let ts: u64 = parts.next().expect("missing time stamp").parse().expect("Failed to parse time stamp");
                    let instr = match indicator {
                        "M" => (ts, vec![ControlInst::Map(parts.map(|x| x.parse().expect("Failed to parse parts")).collect())]),
                        "D" => {
                            let parts: Vec<usize> = parts.map(|x| x.parse().unwrap()).collect();
                            let inst = parts.chunks(2).map(|x| ControlInst::Move(::dynamic_scaling_mechanism::BinId::new(x[0]), x[1])).collect();
                            (ts, inst)
                        },
                        _ => return Err("Incorrect input found in map file".to_string()),
                    };
                    instructions.push(instr);
                }
                Ok(instructions)
            },
//            _ => panic!("unsupported map mode"),
        }

    }

}

pub fn statm_reporter() -> ::std::sync::Arc<::std::sync::atomic::AtomicBool> {

    // Read and report RSS every 100ms
    let statm_reporter_running = ::std::sync::Arc::new(::std::sync::atomic::AtomicBool::new(true));
    {
        let statm_reporter_running = statm_reporter_running.clone();
        ::std::thread::spawn(move || {
            use std::io::Read;
            let timer = ::std::time::Instant::now();
            let mut iteration = 0;
            while statm_reporter_running.load(::std::sync::atomic::Ordering::SeqCst) {
                let mut stat_s = String::new();
                let mut statm_f = ::std::fs::File::open("/proc/self/statm").expect("can't open /proc/self/statm");
                statm_f.read_to_string(&mut stat_s).expect("can't read /proc/self/statm");
                let pages: u64 = stat_s.split_whitespace().nth(1).expect("wooo").parse().expect("not a number");
                let rss = pages * 4096;

                let elapsed_ns = timer.elapsed().to_nanos();
                println!("statm_RSS\t{}\t{}", elapsed_ns, rss);
                #[allow(deprecated)]
                    ::std::thread::sleep_ms(500 - (elapsed_ns / 1_000_000 - iteration * 500) as u32);
                iteration += 1;
            }
        });
    }
    statm_reporter_running
}
