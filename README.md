# title_grabber_rs


### Usage instructions

* Just feed it 1 or more files containing URLs (1 per line)

`cargo run -- /abs/path/2/urls1.csv rel/path/2/urls2.csv`

* Optionally, change the output file:

`cargo run -- -o output.csv /abs/path/2/urls1.csv rel/path/2/urls2.csv`

* See all available config options:

`cargo run -- -h`

    title_grabber 0.1.0
    Cristian Rasch <cristianrasch@fastmail.fm>
    Grabs page & article titles from lists of URLs contained in files passed in as arguments

    USAGE:
        title_grabber [OPTIONS] [files]...

    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information

    OPTIONS:
            --connect-timeout <connect-timeout>    HTTP connect timeout. Defaults to the value of the CONNECT_TIMEOUT env
                                                   var or 10 [env: CONNECT_TIMEOUT=]  [default: 10]
        -d, --debug <debug>                        Log to STDOUT instead of to a file in the CWD.  Defaults to the value of
                                                   the DEBUG env var or False [env: DEBUG=]
            --max-redirects <max-redirects>        Max. # of HTTP redirects to follow. Defaults to the value of the
                                                   MAX_REDIRECTS env var or 5 [env: MAX_REDIRECTS=]  [default: 5]
        -r <max-retries>                           Max. # of times to retry failed HTTP reqs. Defaults to the value of the
                                                   MAX_RETRIES env var or 3 [env: MAX_RETRIES=]  [default: 3]
        -t <max-threads>                           Max. # of threads to use. Defaults to the value of the MAX_THREADS env
                                                   var or the # of logical processors in the system (8) [env: MAX_THREADS=]
                                                   [default: 8]
        -o, --output <output>                      Output file (defaults to out.csv)
            --read-timeout <read-timeout>          HTTP read timeout. Defaults to the value of the READ_TIMEOUT env var or
                                                   15 [env: READ_TIMEOUT=]  [default: 15]

    ARGS:
        <files>...    1 or more CSV files containing URLs (1 per line)

### TODO

1. Add tests
2. Replace simplelog with log4rs in order to log in local time
