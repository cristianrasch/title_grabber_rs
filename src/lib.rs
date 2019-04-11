// use std::fs;
use std::fs::File;
// use std::fs::{File, OpenOptions};
use std::error::Error;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::thread;
use std::time::Duration;

#[macro_use]
extern crate lazy_static;

#[macro_use] extern crate log;

use simplelog::*;

use num_cpus;

use regex::Regex;

use reqwest;

pub const DEF_OUT_PATH: &str = "out.csv";
pub const CONN_TO: u64 = 10;
pub const READ_TO: u64 = 15;
pub const MAX_REDIRECTS: usize = 5;
pub const MAX_RETRIES: u64 = 3;

lazy_static! {
    pub static ref NUM_CPUS: usize = num_cpus::get();
    static ref URL_RE: Regex = Regex::new(r"https?://\S+").unwrap();
}

pub struct TitleGrabber<'a> {
    files: Vec<&'a Path>,
    // out_path: &'a Path,
    debug: bool,
    connect_timeout: u64,
    read_timeout: u64,
    max_redirects: usize,
    max_retries: u64,
    max_threads: usize,
}

impl<'a> TitleGrabber<'a> {
    // pub fn new(files: Vec<&'a Path>, out_path: &'a Path) -> TitleGrabber<'a> {
    pub fn new(files: Vec<&'a Path>) -> TitleGrabber<'a> {
        let log_config = Config::default();
        if let Ok(log_file) = File::create("title_grabber.log") {
            WriteLogger::init(LevelFilter::Info, log_config, log_file).unwrap();
        } else {
            TermLogger::init(LevelFilter::Info, log_config).unwrap();
        }

        Self {
            files,
            // out_path,
            debug: false,
            connect_timeout: CONN_TO,
            read_timeout: READ_TO,
            max_redirects: MAX_REDIRECTS,
            max_retries: MAX_RETRIES,
            max_threads: num_cpus::get(),
            // max_threads: NUM_CPUS,
        }
    }

    pub fn enable_debug_mode(&mut self) -> &mut Self {
        self.debug = true;
        self
    }

    pub fn with_connect_timeout(&mut self, timeout: u64) -> &mut Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_read_timeout(&mut self, timeout: u64) -> &mut Self {
        self.read_timeout = timeout;
        self
    }

    pub fn with_max_redirects(&mut self, redirects: usize) -> &mut Self {
        self.max_redirects = redirects;
        self
    }

    pub fn with_max_retries(&mut self, retries: u64) -> &mut Self {
        self.max_retries = retries;
        self
    }

    pub fn with_max_threads(&mut self, threads: usize) -> &mut Self {
        self.max_threads = threads;
        self
    }

    pub fn write_csv_to(&self, _out_path: &'a Path) -> Result<(), Box<Error>> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.read_timeout))
            .connect_timeout(Duration::from_secs(self.connect_timeout))
            .redirect(reqwest::RedirectPolicy::limited(self.max_redirects))
            .build()?;

        for path in self.files.iter() {
            info!("FILE: {}", path.display());

            let file = File::open(path)?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line?;

                if let Some(url) = URL_RE.find(&line) {
                    let url = url.as_str();
                    let mut retries = 0;
                    let mut res = http_client.get(url).send();

                    while res.is_err() && retries < self.max_retries {
                        let err = res.as_ref().err().unwrap();
                        retries += 1;

                        let will_retry = (err.is_http() || err.is_timeout() || err.is_server_error()) && retries < self.max_retries;

                        if will_retry {
                            if let Some(status) = err.status() {
                                warn!("GET {} [{}] - Retry: {}", url, status, retries);
                            } else {
                                warn!("GET {} [{}] - Retry: {}", url, err, retries);
                            }

                            thread::sleep(Duration::from_secs(retries));
                            res = http_client.get(url).send();
                        } else {
                            break;
                        }
                    }

                    match res {
                        Ok(resp) => info!("GET {} - [{}]", url, resp.status()),
                        Err(err) => {
                            if let Some(status) = err.status() {
                                warn!("GET {} - [{}]", url, status);
                            } else {
                                warn!("GET {} - [{}]", url, err);
                            }
                        }
                    };
                }
            }
        }

        Ok(())
    }
}
