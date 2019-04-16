use std::env;
use std::fs;
use std::path::Path;

use csv;

use title_grabber_rs::TitleGrabber;

#[test]
fn it_works() {
    env::set_var("TESTING", "1");
    let inputs = vec![Path::new("tests/fixtures/urls.txt")];
    let out_path = Path::new("tests/fixtures/result.csv");
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
