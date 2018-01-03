extern crate son_of_grid_engine as sge;

use sge::NetworkInfo::*;
use sge::JobType::*;

//use std::io::prelude::*;
use std::collections::HashMap;
use std::io::{Write, Read, BufRead, BufReader};
use std::net::{TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

fn main() {
    let port = 4223;
    let data_port = 4242;
    let max_runtime = Duration::new(2*60, 0);

    let start_time = Instant::now();
    let shutdown_time = start_time + max_runtime;
    let sge = sge::discover();
    let pool = sge.get_pinned_threadpool();

    match sge.networking {
        Master => {
            println!("master process");
            let listener = sge.listen_on_port(port);

            let (tx, rx) = channel();

            // accept new connections
            let tx_ = tx.clone();
            pool.execute(move || {
                for stream in listener.incoming() {
                    println!("new connection: {:?}", stream);
                    let stream = stream.unwrap();
                    stream.set_nonblocking(true).expect("set_nonblocking call failed");
                    stream.set_read_timeout(Some(Duration::from_millis(500))).expect("set_nonblocking call failed");
                    stream.set_nodelay(true).expect("unable to set_nonblocking");

                    let stream = BufReader::new(stream);

                    tx_.send(stream).unwrap();
                }
            });

            let sink = sge.listen_on_port(data_port);
            pool.execute(move || {
                for sink in sink.incoming() {
                    std::thread::spawn(move || {
                        let buf = &mut [0; 4096];
                        let mut sink = sink.unwrap();
                        sink.set_nonblocking(false).unwrap();
                        while sink.read(buf).is_ok() { }
                    });
                }
            });

            if let Array { id, first, last, step_size } = sge.job_type {
                let mut checkins = {
                    let mut h = HashMap::new();
                    let mut i = first;
                    while i <= last {
                        if i != id {
                            h.insert(i, Registered);
                        }
                        i += step_size;
                    };
                    h
                };

                let mut update_checkins = false;
                while Instant::now() < shutdown_time {
                    // check for new connections
                    if let Ok(mut stream) = rx.try_recv() {
                        println!("processing new stream");
                        let mut buf = vec![];
                        let reads = stream.read_until(b'\n', &mut buf);
                        println!("{:?}\t{:?}", reads, buf);

                        if let Ok(reads) = reads {
                            if reads == 0 {
                                println!("no data, reset connection");
                                // delay handling for one round
                                tx.send(stream).unwrap();
                            } else {
                                let buf = String::from_utf8_lossy(&*buf);
                                if buf.starts_with("hi ") {
                                    let num = buf.split_whitespace().nth(1).expect("nth 1")
                                        .parse().expect("unable to parse id");
                                    println!("parsed num: {}", num);
                                    checkins.insert(num, Connected(stream));
                                    update_checkins = true;
                                } else {
                                    println!("invalid pkg: {:?}", buf);
                                    // delay handling for one round
                                    tx.send(stream).unwrap();
                                }
                            }
                        }
                    }

                    for (id, node) in &mut checkins {
                        let mut gone = false;
                        if let &mut Connected(ref mut stream) = node {
                            // check for data
                            let mut buf = vec![];
                            let reads = stream.read_until(b'\n', &mut buf);

                            if let Ok(reads) = reads {
                                if reads > 0 {
                                    let buf = String::from_utf8_lossy(&*buf);
                                    if buf.trim() != "." {
                                        println!("msg from {}: {:?}", id, buf);
                                    }
                                }
                            }

                            // send heartbeat to clients
                            if let Err(_) = stream.get_mut().write(b"heartbeat\n") {
                                gone = true;
                            }
                        }

                        if gone {
                            *node = Gone;
                            update_checkins = true;
                        }
                    }

                    if update_checkins {
                        println!("checkin: {:?}", checkins);
                        update_checkins = false;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        },
        Client => {
            println!("client process");
            let mut connection = sge.connect_to_master(port);
            connection.set_nodelay(true).expect("unable to set_nonblocking");

            if let Array { id, .. } = sge.job_type {
                let pkg = format!("hi {}\n", id);
                connection.write(pkg.as_bytes()).unwrap();
                let mut connection = BufReader::new(connection);
                let master_up = &Arc::new(AtomicBool::new(false));

                let master_up_ = master_up.clone();
                pool.execute(move || {
                    while master_up_.load(Ordering::Relaxed) == false {
                        // wait for connection
                    }
                    let mut stream = sge.connect_to_master(data_port);

                    let buf = [0x42; 4096];
                    loop {
                        stream.write(&buf).unwrap();
                    }
                });

                while Instant::now() < shutdown_time {
                    let mut buf = vec![];
                    let reads = connection.read_until(b'\n', &mut buf);

                    if let Ok(reads) = reads {
                        if reads > 0 {
                            let buf = String::from_utf8_lossy(&*buf);
                            println!("msg from master: {:?}", buf);
                            connection.get_mut().write(b".\n").unwrap();
                            master_up.store(true, Ordering::Relaxed);
                        }
                    }

                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        },
        Localhost => {
            println!("no networking required, use channel");
        },
    };
}

enum CheckinStatus {
    Registered, Connected(BufReader<TcpStream>), Gone,
}
use CheckinStatus::*;

impl std::fmt::Debug for CheckinStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let l = match *self {
            Registered => "Registered",
            Connected(_) => "Connected",
            Gone => "Gone",
        };
        write!(f, "{}", l)
    }
}
