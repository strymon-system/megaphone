use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::io::{BufRead, BufReader};
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("words.rs");
    let mut f = File::create(&dest_path).unwrap();
    let input = File::open("words.txt").unwrap();
    let file = BufReader::new(&input);
    let mut words = vec!();
    for line in file.lines() {
        let mut word = line.unwrap();
        word.push_str(" ");
        words.push(word);

    }
    f.write_all(b"static WORDS: [&str; ").unwrap();
    f.write_all(format!("{}", words.len()).as_bytes()).unwrap();
    f.write_all(b"] = [\n").unwrap();
    for word in words.into_iter() {
        f.write_all(b"  r#\"").unwrap();
        f.write_all(word.as_bytes()).unwrap();
        f.write_all(b"\"#,\n").unwrap();
    }
    f.write_all(b"];\n").unwrap();
}
