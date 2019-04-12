use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::thread;
use std::time::Duration;

use csv;
use itertools;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
use num_cpus;
use regex::Regex;
use reqwest;
#[macro_use]
extern crate serde_derive;
use scraper::{Html, Selector};
use simplelog::*;

pub const DEF_OUT_PATH: &str = "out.csv";
pub const CONN_TO: u64 = 10;
pub const READ_TO: u64 = 15;
pub const MAX_REDIRECTS: usize = 5;
pub const MAX_RETRIES: u64 = 3;
const END_URL_HEAD: &str = "end_url";
const PAGE_TIT_HEAD: &str = "page_title";
const ART_TIT_HEAD: &str = "article_title";

lazy_static! {
    pub static ref NUM_CPUS: usize = num_cpus::get();
    static ref URL_RE: Regex = Regex::new(r"https?://\S+").unwrap();
    static ref NEW_LINE_RE: Regex = Regex::new(r"\n").unwrap();
    static ref WHITESPACE_RE: Regex = Regex::new(r"\s{2,}").unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct CsvRow {
    url: String,
    end_url: String,
    page_title: Option<String>,
    article_title: Option<String>,
}

pub struct TitleGrabber<'a> {
    files: Vec<&'a Path>,
    out_path: &'a Path,
    connect_timeout: u64,
    read_timeout: u64,
    max_redirects: usize,
    max_retries: u64,
    max_threads: usize,
    processed_urls: HashMap<String, HashMap<&'static str, String>>,
}

impl<'a> TitleGrabber<'a> {
    pub fn new(
        files: Vec<&'a Path>,
        out_path: &'a Path,
        debugging_enabled: bool,
    ) -> TitleGrabber<'a> {
        let log_config = Config::default();
        let log_level = if debugging_enabled {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        };
        if let Ok(log_file) = File::create("title_grabber.log") {
            WriteLogger::init(log_level, log_config, log_file).unwrap();
        } else {
            TermLogger::init(log_level, log_config).unwrap();
        }

        let processed_urls = Self::read_already_processed_urls(out_path);

        Self {
            files,
            out_path,
            connect_timeout: CONN_TO,
            read_timeout: READ_TO,
            max_redirects: MAX_REDIRECTS,
            max_retries: MAX_RETRIES,
            max_threads: *NUM_CPUS,
            processed_urls,
        }
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

    fn read_already_processed_urls(
        out_path: &'a Path,
    ) -> HashMap<String, HashMap<&'static str, String>> {
        let mut processed_urls = HashMap::new();

        if out_path.exists() {
            if let Some(mut reader) = csv::Reader::from_path(out_path).ok() {
                for row in reader.deserialize() {
                    if row.is_err() {
                        continue;
                    }

                    let r: CsvRow = row.unwrap();
                    if r.page_title.is_some() || r.article_title.is_some() {
                        let val = [
                            (END_URL_HEAD, r.end_url),
                            (PAGE_TIT_HEAD, r.page_title.unwrap_or_default()),
                            (ART_TIT_HEAD, r.article_title.unwrap_or_default()),
                        ]
                        .iter()
                        .cloned()
                        .collect();

                        processed_urls.insert(r.url, val);
                    }
                }
            }
        }

        processed_urls
    }

    fn fix_whitespace(html: String) -> String {
        WHITESPACE_RE
            .replace_all(&NEW_LINE_RE.replace_all(html.trim(), " "), " ")
            .into_owned()
    }

    fn build_http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(self.read_timeout))
            .connect_timeout(Duration::from_secs(self.connect_timeout))
            .redirect(reqwest::RedirectPolicy::limited(self.max_redirects))
            .build()
            .unwrap()
    }

    pub fn write_csv_to(&self) -> Result<(), Box<Error>> {
        let http_client = self.build_http_client();

        for path in self.files.iter() {
            debug!("FILE: {}", path.display());

            let file = File::open(path)?;
            let reader = BufReader::new(file);
            let mut writer = csv::Writer::from_path(self.out_path)?;

            for line in reader.lines() {
                let line = line?;

                if let Some(url) = URL_RE.find(&line) {
                    let url = url.as_str();

                    if let Some(map) = self.processed_urls.get(url) {
                        writer.serialize(CsvRow {
                            url: url.to_owned(),
                            end_url: map.get(END_URL_HEAD).cloned().unwrap(),
                            page_title: map.get(PAGE_TIT_HEAD).cloned(),
                            article_title: map.get(ART_TIT_HEAD).cloned(),
                        })?;
                    } else {
                        let mut retries = 0;
                        let mut res = http_client.get(url).send();

                        while let Some(err) = res.as_ref().err() {
                            if retries < self.max_retries {
                                break;
                            }

                            retries += 1;

                            let will_retry =
                                (err.is_http() || err.is_timeout() || err.is_server_error())
                                    && retries < self.max_retries;

                            if will_retry {
                                if let Some(status) = err.status() {
                                    warn!("GET {} [code: {}] - Retry: {}", url, status, retries);
                                } else {
                                    warn!("GET {} [err: {}] - Retry: {}", url, err, retries);
                                }

                                thread::sleep(Duration::from_secs(retries));
                                res = http_client.get(url).send();
                            } else {
                                break;
                            }
                        }

                        match res {
                            Ok(resp) => {
                                info!("GET {} - [code: {}]", url, resp.status());
                                let res = resp.error_for_status();

                                if let Some(mut resp) = res.ok() {
                                    if let Some(html) = resp.text().ok() {
                                        let end_url = resp.url().as_str();
                                        debug!("GET {} - Size: {} bytes", end_url, html.len());

                                        let doc = Html::parse_document(&html);
                                        let page_tit_sel = Selector::parse("title").unwrap();
                                        let mut page_tit = None;
                                        if let Some(page_tit_el) = doc.select(&page_tit_sel).next()
                                        {
                                            page_tit.replace(Self::fix_whitespace(
                                                page_tit_el.inner_html(),
                                            ));
                                        }

                                        let mut art_tit_sel =
                                            Selector::parse("article h1").unwrap();
                                        let mut art_tit = None;
                                        if let Some(art_tit_el) = doc.select(&art_tit_sel).next() {
                                            art_tit.replace(Self::fix_whitespace(itertools::join(
                                                art_tit_el.text(),
                                                " ",
                                            )));
                                        } else {
                                            art_tit_sel = Selector::parse("h1").unwrap();
                                            if let Some(art_tit_el) =
                                                doc.select(&art_tit_sel).next()
                                            {
                                                art_tit.replace(Self::fix_whitespace(
                                                    itertools::join(art_tit_el.text(), " "),
                                                ));
                                            }
                                        }

                                        writer.serialize(CsvRow {
                                            url: url.to_owned(),
                                            end_url: end_url.to_owned(),
                                            page_title: page_tit,
                                            article_title: art_tit,
                                        })?;
                                    }
                                }
                            }
                            Err(err) => {
                                if let Some(status) = err.status() {
                                    error!("GET {} [code: {}] - Retry: {}", url, status, retries);
                                } else {
                                    error!("GET {} [err: {}] - Retry: {}", url, err, retries);
                                }
                            }
                        };
                    }
                }
            }
        }

        Ok(())
    }
}
