use std::env;
use std::fs;
use std::path::Path;

use csv;

use title_grabber_rs::TitleGrabber;

#[test]
fn it_works_with_non_twitter_urls() {
    env::set_var("TESTING", "1");
    let inputs = vec![Path::new("tests/fixtures/urls.txt")];
    let out_path = Path::new("tests/fixtures/non_ttr_urls_res.csv");
    let mut instance = TitleGrabber::new(inputs, out_path, false);
    instance.with_read_timeout(1);
    instance.with_max_redirects(1);

    assert!(instance.write_csv_file().is_ok());

    assert!(out_path.exists());
    assert!(out_path.is_file());
    let mut reader = csv::Reader::from_path(out_path)
        .expect(&format!("Unable to read out CSV '{}'", out_path.display()));
    let mut iter = reader.records();
    let row = iter.next().expect(&format!(
        "Output CSV '{}' should've have exactly 1 record",
        out_path.display()
    ));
    let r = row.expect(&format!(
        "Unable to read first row from output CSV '{}'",
        out_path.display()
    ));
    let url = Some("https://www.jaylen.com.ar/");
    assert_eq!(url, r.get(0));
    let end_url = url;
    assert_eq!(end_url, r.get(1));
    assert_eq!(Some("Jaylen Informática"), r.get(2));
    assert_eq!(Some("Productos"), r.get(3));
    assert!(iter.next().is_none());
    assert!(fs::remove_file(out_path).is_ok());
}

#[test]
fn it_works_with_t_co_urls() {
    env::set_var("TESTING", "1");
    let inputs = vec![Path::new("tests/fixtures/t_co_urls.txt")];
    let out_path = Path::new("tests/fixtures/t_co_urls_res.csv");
    let instance = TitleGrabber::new(inputs, out_path, false);

    assert!(instance.write_csv_file().is_ok());

    assert!(out_path.exists());
    assert!(out_path.is_file());
    let mut reader = csv::Reader::from_path(out_path)
        .expect(&format!("Unable to read out CSV '{}'", out_path.display()));
    let mut iter = reader.records();
    let row = iter.next().expect(&format!(
        "Output CSV '{}' should've have exactly 1 record",
        out_path.display()
    ));
    let r = row.expect(&format!(
        "Unable to read first row from output CSV '{}'",
        out_path.display()
    ));
    let url = "https://t.co/7VDzp24y9N";
    assert_eq!(Some(url), r.get(0));
    let end_url = "https://startupmap.iamsterdam.com/dashboard,https://twitter.com/Startup_Adam";
    assert_eq!(Some(end_url), r.get(1));
    let page_tit = r
        .get(2)
        .expect(&format!("Page title from {} shouldn't be empty", url));
    assert_eq!(1, page_tit.matches("A new report shows that startups have become Amsterdam's leading job growth engine").count());
    assert_eq!(Some("Dealroom.co"), r.get(3));
    assert!(iter.next().is_none());
    assert!(fs::remove_file(out_path).is_ok());
}

#[test]
fn it_works_with_twitter_status_update_urls() {
    env::set_var("TESTING", "1");
    let inputs = vec![Path::new("tests/fixtures/twitter_status_update_urls.txt")];
    let out_path = Path::new("tests/fixtures/ttr_status_upd_urls_res.csv");
    let instance = TitleGrabber::new(inputs, out_path, false);

    assert!(instance.write_csv_file().is_ok());

    assert!(out_path.exists());
    assert!(out_path.is_file());
    let mut reader = csv::Reader::from_path(out_path)
        .expect(&format!("Unable to read out CSV '{}'", out_path.display()));
    let mut iter = reader.records();
    let row = iter.next().expect(&format!(
        "Output CSV '{}' should've have exactly 1 record",
        out_path.display()
    ));
    let r = row.expect(&format!(
        "Unable to read first row from output CSV '{}'",
        out_path.display()
    ));
    let url = "https://twitter.com/i/web/status/1116358879409995776";
    assert_eq!(Some(url), r.get(0));
    let end_url = "https://twitter.com/cityblockhealth,https://twitter.com/cityblockhealth/status/1116351442460315649";
    assert_eq!(Some(end_url), r.get(1));
    let page_tit = r
        .get(2)
        .expect(&format!("Page title from {} shouldn't be empty", url));
    assert_eq!(1, page_tit.matches("Cityblock Health has joined our global army of Health Transformers who are committed to improving the life and wellbeing of everyone in the world").count());
    let art_tit = r
        .get(3)
        .expect(&format!("Article title from {} shouldn't be empty", url));
    assert!(art_tit.starts_with("StartUp Health"));
    assert!(iter.next().is_none());
    assert!(fs::remove_file(out_path).is_ok());
}
