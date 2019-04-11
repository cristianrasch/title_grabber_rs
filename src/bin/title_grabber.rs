use std::path::Path;
use std::process;

use clap::{App, Arg};

use num_cpus;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref TRUE_VALS: [&'static str; 5] = ["1", "t", "true", "True", "TRUE"];
}

use title_grabber_rs::{
    TitleGrabber, CONN_TO, DEF_OUT_PATH, MAX_REDIRECTS, MAX_RETRIES, NUM_CPUS, READ_TO,
};

fn main() {
    let def_conn_to = CONN_TO.to_string();
    let def_read_to = READ_TO.to_string();
    let def_max_redirects = MAX_REDIRECTS.to_string();
    let def_max_retries = MAX_RETRIES.to_string();
    let def_max_threads = NUM_CPUS.to_string();

    let matches = App::new("title_grabber")
        .version("0.1.0")
        .author("Cristian Rasch <cristianrasch@fastmail.fm>")
        .about("Grabs page & article titles from lists of URLs contained in files passed in as arguments")
        .arg(
            Arg::with_name("debug")
                .short("d")
                .long("debug")
                .env("DEBUG")
                .help("Log to STDOUT instead of to a file in the CWD.  Defaults to the value of the DEBUG env var or False"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .help(&format!("Output file (defaults to {})", DEF_OUT_PATH))
                .takes_value(true),
        )
        .arg(
            Arg::with_name("connect-timeout")
                .long("connect-timeout")
                .takes_value(true)
                .env("CONNECT_TIMEOUT")
                .default_value(&def_conn_to)
                // .default_value(str::from_utf8(&[CONN_TO]).unwrap())
                .help(&format!("HTTP connect timeout. Defaults to the value of the CONNECT_TIMEOUT env var or {}", CONN_TO)),
        )
        .arg(
            Arg::with_name("read-timeout")
                .long("read-timeout")
                .takes_value(true)
                .env("READ_TIMEOUT")
                .default_value(&def_read_to)
                .help(&format!("HTTP read timeout. Defaults to the value of the READ_TIMEOUT env var or {}", READ_TO)),
        )
        .arg(
            Arg::with_name("max-redirects")
                .long("max-redirects")
                .takes_value(true)
                .env("MAX_REDIRECTS")
                .default_value(&def_max_redirects)
                .help(&format!("Max. # of HTTP redirects to follow. Defaults to the value of the MAX_REDIRECTS env var or {}", MAX_REDIRECTS)),
        )
        .arg(
            Arg::with_name("max-retries")
                .short("r")
                .takes_value(true)
                .env("MAX_RETRIES")
                .default_value(&def_max_retries)
                .help(&format!("Max. # of times to retry failed HTTP reqs. Defaults to the value of the MAX_RETRIES env var or {}", MAX_RETRIES)),
        )
        .arg(
            Arg::with_name("max-threads")
                .short("t")
                .takes_value(true)
                .env("MAX_THREADS")
                .default_value(&def_max_threads)
                .help(&format!("Max. # of threads to use. Defaults to the value of the MAX_THREADS env var or the # of logical processors in the system ({})", def_max_threads)),
        )
        .arg(
            Arg::with_name("files")
                .index(1)
                .multiple(true)
                .takes_value(true)
                .help("1 or more CSV files containing URLs (1 per line)"),
        )
        .get_matches();

    println!("{:?}", matches);

    let out_path = matches.value_of("output").unwrap_or(DEF_OUT_PATH);

    if let Some(files) = matches.values_of("files") {
        let files: Vec<&Path> = files.map(|f| f.as_ref()).collect();

        // let mut instance = TitleGrabber::new(files, out_path.as_ref());
        let mut instance = TitleGrabber::new(files);

        if let Some(debug) = matches.value_of("debug") {
            if TRUE_VALS.iter().any(|&true_val| debug == true_val) {
                instance.enable_debug_mode();
            }
        }

        let conn_to = matches
            .value_of("connect-timeout")
            .unwrap()
            .parse()
            .unwrap_or(CONN_TO);
        instance.with_connect_timeout(conn_to);

        let read_to = matches
            .value_of("read-timeout")
            .unwrap()
            .parse()
            .unwrap_or(READ_TO);
        instance.with_read_timeout(read_to);

        let max_redirects = matches
            .value_of("max-redirects")
            .unwrap()
            .parse()
            .unwrap_or(MAX_REDIRECTS);
        instance.with_max_redirects(max_redirects);

        let max_retries = matches
            .value_of("max-retries")
            .unwrap()
            .parse()
            .unwrap_or(MAX_RETRIES);
        instance.with_max_retries(max_retries);

        let max_threads: usize = matches
            .value_of("max-threads")
            .unwrap()
            .parse()
            .unwrap_or(num_cpus::get());
        instance.with_max_threads(max_threads);

        if let Some(err) = instance.write_csv_to(out_path.as_ref()).err() {
            eprintln!("Error: {}", err.description());
        }
    } else {
        eprintln!("At least 1 input file is required!");
        process::exit(1);
    }
}
