use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
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
use scoped_threadpool::Pool;
use scraper::{Html, Selector};
use simplelog::*;

pub const DEF_OUT_PATH: &str = "output.csv";
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

fn fix_whitespace(html: String) -> String {
    WHITESPACE_RE
        .replace_all(&NEW_LINE_RE.replace_all(html.trim(), " "), " ")
        .into_owned()
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
    output_path: &'a Path,
    connect_timeout: u64,
    read_timeout: u64,
    max_redirects: usize,
    max_retries: u64,
    max_threads: usize,
}

impl<'a> TitleGrabber<'a> {
    pub fn new(
        files: Vec<&'a Path>,
        output_path: &'a Path,
        debugging_enabled: bool,
    ) -> TitleGrabber<'a> {
        if env::var("TESTING").is_err() {
            let log_config = Config {
                time_format: Some("%F %T"),
                ..Config::default()
            };
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
        }

        Self {
            files,
            output_path,
            connect_timeout: CONN_TO,
            read_timeout: READ_TO,
            max_redirects: MAX_REDIRECTS,
            max_retries: MAX_RETRIES,
            max_threads: *NUM_CPUS,
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

    fn processed_urls(&self) -> HashMap<String, HashMap<&'static str, Option<String>>> {
        let mut res = HashMap::new();

        if self.output_path.exists() {
            if let Some(mut reader) = csv::Reader::from_path(self.output_path).ok() {
                for row in reader.deserialize() {
                    if row.is_err() {
                        continue;
                    }

                    let r: CsvRow = row.unwrap();
                    if r.page_title.is_some() || r.article_title.is_some() {
                        let mut url_data = HashMap::with_capacity(3);
                        url_data.insert(END_URL_HEAD, Some(r.end_url));
                        url_data.insert(PAGE_TIT_HEAD, r.page_title);
                        url_data.insert(ART_TIT_HEAD, r.article_title);

                        // let url_data = [
                        //     (END_URL_HEAD, Some(r.end_url)),
                        //     (PAGE_TIT_HEAD, r.page_title),
                        //     (ART_TIT_HEAD, r.article_title),
                        // ]
                        // .iter()
                        // .cloned()
                        // .collect();

                        res.insert(r.url, url_data);
                    }
                }
            }
        }

        res
    }

    fn build_http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(self.read_timeout))
            .connect_timeout(Duration::from_secs(self.connect_timeout))
            .redirect(reqwest::RedirectPolicy::limited(self.max_redirects))
            .build()
            .unwrap()
    }

    fn scrape_url(
        &self,
        url: String,
        http_client: Arc<reqwest::Client>,
        tx: mpsc::Sender<Option<CsvRow>>,
    ) {
        let mut retries = 0;
        let mut res = http_client.get(&url).send();

        while let Some(err) = res.as_ref().err() {
            if retries >= self.max_retries {
                break;
            }

            retries += 1;

            let will_retry = (err.is_http() || err.is_timeout() || err.is_server_error())
                && retries < self.max_retries;

            if will_retry {
                if let Some(status) = err.status() {
                    warn!("GET {} {} - Retry: {}", url, status, retries);
                } else {
                    warn!("GET {} Err: {} - Retry: {}", url, err, retries);
                }

                thread::sleep(Duration::from_secs(retries));
                res = http_client.get(&url).send();
            } else {
                break;
            }
        }

        let mut ret = None;
        match res {
            Ok(resp) => {
                info!("GET {} - {}", url, resp.status());
                let res = resp.error_for_status();

                if let Some(mut resp) = res.ok() {
                    if let Some(html) = resp.text().ok() {
                        let end_url = resp.url().as_str();
                        debug!("GET {} - {} bytes", end_url, html.len());

                        let doc = Html::parse_document(&html);
                        let page_tit_sel = Selector::parse("title").unwrap();
                        let mut page_tit = None;
                        if let Some(page_tit_el) = doc.select(&page_tit_sel).next() {
                            page_tit.replace(fix_whitespace(page_tit_el.inner_html()));
                        }

                        let mut art_tit_sel = Selector::parse("article h1").unwrap();
                        let mut art_tit = None;
                        if let Some(art_tit_el) = doc.select(&art_tit_sel).next() {
                            art_tit
                                .replace(fix_whitespace(itertools::join(art_tit_el.text(), " ")));
                        } else {
                            art_tit_sel = Selector::parse("h1").unwrap();
                            if let Some(art_tit_el) = doc.select(&art_tit_sel).next() {
                                art_tit.replace(fix_whitespace(itertools::join(
                                    art_tit_el.text(),
                                    " ",
                                )));
                            }
                        }

                        ret.replace(CsvRow {
                            url: url,
                            end_url: end_url.to_owned(),
                            page_title: page_tit,
                            article_title: art_tit,
                        });
                    }
                }
            }
            Err(err) => {
                if let Some(status) = err.status() {
                    error!("GET {} {} - Retry: {}", url, status, retries);
                } else {
                    error!("GET {} Err: {} - Retry: {}", url, err, retries);
                }
            }
        };

        let _res = tx.send(ret);
    }

    pub fn write_csv_file(&self) -> Result<(), Box<Error>> {
        let processed_urls = self.processed_urls();
        let http_client = Arc::new(self.build_http_client());
        let mut writer = csv::Writer::from_path(self.output_path)?;
        let mut pool = Pool::new(self.max_threads as u32);
        let work_queue = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        pool.scoped(|scoped| {
            for path in self.files.iter() {
                debug!("FILE: {}", path.display());

                if let Some(file) = File::open(path).ok() {
                    let reader = BufReader::new(file);

                    for line in reader.lines() {
                        if let Some(line) = line.ok() {
                            if let Some(url) = URL_RE.find(&line) {
                                let url = url.as_str();

                                if let Some(r) = processed_urls.get(url) {
                                    let res = writer.serialize(CsvRow {
                                        url: url.to_owned(),
                                        end_url: r.get(END_URL_HEAD).cloned().unwrap().unwrap(),
                                        page_title: r.get(PAGE_TIT_HEAD).cloned().unwrap(),
                                        article_title: r.get(ART_TIT_HEAD).cloned().unwrap(),
                                    });

                                    if let Some(_) = res.err() {
                                        error!(
                                            "Failed to reuse data for previously scraped URL: {}",
                                            url
                                        );
                                    }
                                } else {
                                    let url = url.to_owned();
                                    let http_client = http_client.clone();
                                    let tx = tx.clone();
                                    let work_queue = work_queue.clone();

                                    scoped.execute(move || {
                                        self.scrape_url(url, http_client, tx);
                                        work_queue.fetch_add(1, Ordering::SeqCst);
                                    });
                                }
                            }
                        }
                    }
                }
            }
        });

        for _ in 0..work_queue.load(Ordering::Relaxed) {
            if let Some(res) = rx.recv().ok() {
                if let Some(row) = res {
                    writer.serialize(row)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const TEST_OUT_PATH: &str = "tests/fixtures/out.csv";

    #[test]
    fn it_cleans_up_whitespace() {
        assert_eq!("1 2 3".to_string(), fix_whitespace("  1\n2 \t3 ".to_string()));
    }

    #[test]
    fn it_builds_a_new_instance_with_defaults() {
        let instance = TitleGrabber::new(vec![], Path::new(DEF_OUT_PATH), false);

        assert_eq!(0, instance.files.len());
        assert_eq!(Some(DEF_OUT_PATH), instance.output_path.to_str());
        assert_eq!(CONN_TO, instance.connect_timeout);
        assert_eq!(READ_TO, instance.read_timeout);
        assert_eq!(MAX_REDIRECTS, instance.max_redirects);
        assert_eq!(MAX_RETRIES, instance.max_retries);
        assert_eq!(num_cpus::get(), instance.max_threads);
    }

    #[test]
    fn it_allows_tweaking_its_conn_to() {
        env::set_var("TESTING", "1");
        let mut instance = TitleGrabber::new(vec![], Path::new(DEF_OUT_PATH), false);
        let timeout = 5;

        instance.with_connect_timeout(timeout);

        assert_eq!(timeout, instance.connect_timeout);
    }

    #[test]
    fn it_allows_tweaking_its_read_to() {
        env::set_var("TESTING", "1");
        let mut instance = TitleGrabber::new(vec![], Path::new(DEF_OUT_PATH), false);
        let timeout = 5;

        instance.with_read_timeout(timeout);

        assert_eq!(timeout, instance.read_timeout);
    }

    #[test]
    fn it_allows_tweaking_its_max_redirs() {
        env::set_var("TESTING", "1");
        let mut instance = TitleGrabber::new(vec![], Path::new(DEF_OUT_PATH), false);
        let redirects = 3;

        instance.with_max_redirects(redirects);

        assert_eq!(redirects, instance.max_redirects);
    }

    #[test]
    fn it_allows_tweaking_its_max_threads() {
        env::set_var("TESTING", "1");
        let mut instance = TitleGrabber::new(vec![], Path::new(DEF_OUT_PATH), false);
        let threads = 4;

        instance.with_max_threads(threads);

        assert_eq!(threads, instance.max_threads);
    }

    #[test]
    fn it_does_not_panic_on_file_not_found() {
        env::set_var("TESTING", "1");
        let inputs = vec![Path::new("tests/fixtures/does-not-exist.txt")];
        let out_path = Path::new(TEST_OUT_PATH);
        let instance = TitleGrabber::new(inputs, out_path, false);

        assert!(instance.write_csv_file().is_ok());

        assert!(out_path.exists());
        assert!(out_path.is_file());
        let mut out_f = File::open(out_path).expect(&format!("Unable to open output path '{}'", out_path.display()));
        let mut out_str = String::new();
        out_f.read_to_string(&mut out_str).expect(&format!("Unable to read from '{}'", out_path.display()));
        assert!(out_str.is_empty());
        assert!(fs::remove_file(out_path).is_ok());
    }

    #[test]
    fn it_skips_over_invalid_urls_in_inputs_files() {
        env::set_var("TESTING", "1");
        let inputs = vec![Path::new("tests/fixtures/invalid.txt")];
        let out_path = Path::new("tests/fixtures/output.csv");
        // let out_path = Path::new(TEST_OUT_PATH);
        let instance = TitleGrabber::new(inputs, out_path, false);

        assert!(instance.write_csv_file().is_ok());

        assert!(out_path.exists());
        assert!(out_path.is_file());
        let mut out_f = File::open(out_path).expect(&format!("Unable to open output path '{}'", out_path.display()));
        let mut out_str = String::new();
        out_f.read_to_string(&mut out_str).expect(&format!("Unable to read from '{}'", out_path.display()));
        assert!(out_str.is_empty());
        assert!(fs::remove_file(out_path).is_ok());
    }

    #[test]
    fn it_works() {
        env::set_var("TESTING", "1");
        let inputs = vec![Path::new("tests/fixtures/urls.txt")];
        let out_path = Path::new("tests/fixtures/result.csv");
        // let out_path = Path::new(TEST_OUT_PATH);
        let mut instance = TitleGrabber::new(inputs, out_path, false);
        instance.with_read_timeout(1);
        instance.with_max_redirects(1);

        assert!(instance.write_csv_file().is_ok());

        assert!(out_path.exists());
        assert!(out_path.is_file());
        let mut reader = csv::Reader::from_path(out_path).expect(&format!("Unable to read out CSV '{}'", out_path.display()));
        let mut iter = reader.records();
        let row = iter.next().expect(&format!("Output CSV '{}' should've have exactly 1 record", out_path.display()));
        let r = row.expect(&format!("Unable to read first row from output CSV '{}'", out_path.display()));
        let url = Some("https://www.jaylen.com.ar/");
        assert_eq!(url, r.get(0));
        let end_url = url;
        assert_eq!(end_url, r.get(1));
        assert_eq!(Some("Jaylen Informática"), r.get(2));
        assert_eq!(Some("Productos"), r.get(3));
        assert!(iter.next().is_none());
        assert!(fs::remove_file(out_path).is_ok());
    }
}
