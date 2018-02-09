// Updated example from http://rosettacode.org/wiki/Hello_world/Web_server#Rust
// to work with Rust 1.0 beta

use std::net::{TcpStream, TcpListener};
use std::io::{Read, Write};
use std::thread;

extern crate postgres;
extern crate prometheus;

use postgres::{Connection, TlsMode};
use prometheus::{Opts, Registry, Counter, TextEncoder, Encoder};

fn handle_read(mut stream: &TcpStream) {
    let mut buf = [0u8 ;4096];
    match stream.read(&mut buf) {
        Ok(_) => {
            let req_str = String::from_utf8_lossy(&buf);
            println!("{}", req_str);
        },
        Err(e) => println!("Unable to read stream: {}", e),
    }
}

fn handle_write(mut stream: TcpStream) {
    let conn = Connection::connect("postgres://postgres:postgres@localhost:5461", TlsMode::None).unwrap();
    let res = &conn.query("
SELECT json_agg(row_to_json(x.*))::text FROM (
     SELECT
				pg_database.datname,
				tmp.state,
				COALESCE(count,0) as count,
				COALESCE(max_tx_duration,0) as max_tx_duration
			FROM
				(
				  VALUES ('active'),
				  		 ('idle'),
				  		 ('idle in transaction'),
				  		 ('idle in transaction (aborted)'),
				  		 ('fastpath function call'),
				  		 ('disabled')
				) AS tmp(state) CROSS JOIN pg_database
			LEFT JOIN
			(
				SELECT
					datname,
					state,
					count(*) AS count,
					MAX(EXTRACT(EPOCH FROM now() - xact_start))::float AS max_tx_duration
				FROM pg_stat_activity GROUP BY datname,state) AS tmp2
				ON tmp.state = tmp2.state AND pg_database.datname = tmp2.datname
) x
", &[]).unwrap();
    let row = res.get(0);
    let json: String = row.get(0);

    println!("RES: {}", json);



    // Create a Counter.
    let counter_opts = Opts::new("test_counter", "test counter help");
    let counter = Counter::with_opts(counter_opts).unwrap();

    // Create a Registry and register Counter.
    let r = Registry::new();
    r.register(Box::new(counter.clone())).unwrap();

    // Inc.
    counter.inc();

    // Gather the metrics.
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_familys = r.gather();
    encoder.encode(&metric_familys, &mut buffer).unwrap();

    // Output to the standard output.
    println!("{}", String::from_utf8(buffer).unwrap());

    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text; charset=UTF-8\r\n\r\n{}\r\n", json);

    match stream.write(response.as_bytes()) {
        Ok(_) => println!("Response sent"),
        Err(e) => println!("Failed sending response: {}", e),
    }
}

fn handle_client(stream: TcpStream) {
    handle_read(&stream);
    handle_write(stream);
}

fn main() {
    let port = 8787;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    println!("Listening for connections on port {}", 8787);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    handle_client(stream)
                });
            }
            Err(e) => {
                println!("Unable to connect: {}", e);
            }
        }
    }
}
