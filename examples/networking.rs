//! Demo data exchange with a command protocol.
//! The command protocol sends JSON commands seperated by '\n'.
//! The data protocol is binary.

extern crate rand;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate son_of_grid_engine as sge;

use sge::NetworkInfo::*;
use sge::JobType::*;
use rand::Rng;

//use std::io::prelude::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry::Occupied;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use std::thread::sleep;

const DATA_PORT: u16 = 4242;

fn main() {
    let port = 4223;
    let max_runtime = Duration::from_secs(10 * 60);
    let target_rotation_time = Duration::from_secs(5 * 60);

    let start_time = Instant::now();
    let shutdown_time = start_time + max_runtime;
    let sge = sge::discover();
    let pool = sge.get_pinned_threadpool();
    let mut nothing_to_do = true;

    match sge.networking {
        Master => {
            println!("master process");
            let listener = sge.listen_on_port(port)
                .expect("unable to bind master controll port");

            let (tx, rx) = channel();

            // accept new connections
            let tx_ = tx.clone();
            pool.execute(move || {
                for stream in listener.incoming() {
                    let stream = stream.unwrap();
                    stream
                        .set_nonblocking(true)
                        .expect("set_nonblocking call failed");
                    stream
                        .set_read_timeout(Some(Duration::from_millis(500)))
                        .expect("set_nonblocking call failed");
                    stream.set_nodelay(true).expect("unable to set_nonblocking");

                    let stream = BufReader::new(stream);

                    tx_.send(stream).unwrap();
                }
            });

            let sink = sge.listen_on_port(DATA_PORT);
            pool.execute(move || {
                let sink = sink.expect("unable to bind data sink port");
                for sink in sink.incoming() {
                    std::thread::spawn(move || {
                        let buf = &mut [0; 4096];
                        let mut sink = sink.unwrap();
                        sink.set_nonblocking(false).unwrap();
                        while sink.read(buf).is_ok() {}
                    });
                }
            });

            if let Array {
                id,
                first,
                last,
                step_size,
            } = sge.job_type
            {
                let mut checkins = {
                    let mut h = HashMap::new();
                    let mut i = first;
                    while i <= last {
                        if i != id {
                            h.insert(i, Registered);
                        }
                        i += step_size;
                    }
                    h
                };

                let mut update_checkins = false;
                let mut last_focus_point = Instant::now() - target_rotation_time;
                while Instant::now() < shutdown_time {
                    // check for new connections
                    if let Ok(mut stream) = rx.try_recv() {
                        nothing_to_do = false;
                        let mut buf = vec![];
                        let reads = stream.read_until(b'\n', &mut buf);

                        if let Ok(reads) = reads {
                            if reads == 0 {
                                println!("no data, reset connection from {:?}", stream);
                                // delay handling for one round
                                tx.send(stream).unwrap();
                            } else {
                                let pkg = serde_json::from_slice(&*buf);
                                if let Ok(ClientHello { id, data_port }) = pkg {
                                    checkins.insert(id, Connected { stream, data_port });
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
                        if let &mut Connected { ref mut stream, .. } = node {
                            // check for data
                            let mut buf = vec![];
                            let reads = stream.read_until(b'\n', &mut buf);

                            if let Ok(reads) = reads {
                                nothing_to_do = false;
                                if reads > 0 {
                                    let pkg = serde_json::from_slice(&*buf).unwrap_or(Heartbeat);
                                    if pkg != Heartbeat {
                                        println!("msg from {}: {:?}", id, pkg);
                                    }
                                }
                            }

                            // send heartbeat to clients
                            if let Err(_) = write(stream.get_mut(), &Heartbeat) {
                                gone = true;
                            }
                        }

                        if gone {
                            *node = Gone;
                            update_checkins = true;
                        }
                    }

                    if update_checkins {
                        nothing_to_do = false;
                        update_checkins = false;

                        let map = checkins
                            .iter()
                            .map(|(id, status)| {
                                (
                                    *id,
                                    if let &Connected {
                                        ref stream,
                                        data_port,
                                    } = status
                                    {
                                        let stream = stream.get_ref();
                                        Some((stream.peer_addr().unwrap().ip(), data_port))
                                    } else {
                                        None
                                    },
                                )
                            })
                            .collect();
                        let ips = &PeerIps(map);
                        for (_, node) in &mut checkins {
                            if let &mut Connected { ref mut stream, .. } = node {
                                write(stream.get_mut(), ips).is_ok();
                            }
                        }
                    }

                    if last_focus_point + target_rotation_time < Instant::now() {
                        nothing_to_do = false;
                        let ids = checkins
                            .iter()
                            .filter(|&(_, state)| {
                                if let Connected { .. } = *state {
                                    true
                                } else {
                                    false
                                }
                            })
                            .map(|(id, _)| id.clone())
                            .collect::<Vec<_>>();
                        if let Some(id) = rand::thread_rng().choose(&ids) {
                            println!("new focus point: {}", *id);
                            last_focus_point = Instant::now();
                            let id = &FocusPoint { id: *id };

                            for (_, node) in &mut checkins {
                                if let &mut Connected { ref mut stream, .. } = node {
                                    write(stream.get_mut(), id).is_ok();
                                }
                            }
                        }
                    }

                    if nothing_to_do {
                        sleep(Duration::from_secs(1));
                    }
                    nothing_to_do = true;
                }
            }
        }
        Client => {
            let data_port: u16 = rand::thread_rng().gen_range(20000, 65000);
            println!("client process {{ data_port: {} }}", data_port);
            let connection = &mut sge.connect_to_master(port);
            //connection.set_nodelay(true).expect("unable to set_nonblocking");

            if let Array { id, .. } = sge.job_type {
                let pkg = ClientHello { id, data_port };
                write(connection, &pkg).unwrap();
                let mut connection = BufReader::new(connection);
                let reload_config = Arc::new(AtomicBool::new(true));
                let config: HashMap<usize, Option<(IpAddr, u16)>> = HashMap::new();
                let config = Arc::new(Mutex::new((config, None)));

                let sink = sge.listen_on_port(data_port);
                pool.execute(move || {
                    let sink = sink.expect("unable to bind data sink port");
                    for sink in sink.incoming() {
                        std::thread::spawn(move || {
                            let buf = &mut [0; 4096];
                            let mut sink = sink.expect("unable to accept connection");
                            sink.set_nonblocking(false)
                                .expect("failed to set nonblocking on incomming sink connection");
                            while sink.read(buf).is_ok() {}
                        });
                    }
                });

                {
                    let config = config.clone();
                    let reload_config = reload_config.clone();
                    pool.execute(move || {
                        // TODO did I trick the mut checker with master_stream?
                        let master_stream = sge.connect_to_master(DATA_PORT);
                        let mut target_stream = None;

                        let buf = [0x42; 4096];
                        loop {
                            if reload_config.load(Ordering::Relaxed) {
                                let mut config = config.lock().unwrap();
                                reload_config.store(false, Ordering::SeqCst);
                                let focus_id = config.1;
                                let ref mut map = config.0;

                                if let Some(focus_peer) = get_focus_peer(map, &focus_id) {
                                    target_stream = Some(focus_peer);
                                } else {
                                    let candidates = map.values()
                                        .filter(|s| s.is_some())
                                        .map(|s| s.unwrap())
                                        .collect::<Vec<_>>();
                                    target_stream = rand::thread_rng()
                                        .choose(&candidates)
                                        .map(|&(addr, data_port)| {
                                            TcpStream::connect(SocketAddr::new(addr, data_port))
                                                .ok()
                                        })
                                        .unwrap_or(None);
                                }
                            }

                            if let Some(ref stream) = target_stream {
                                stream
                            } else {
                                &master_stream
                            }
                            .write(&buf)
                                .unwrap();

                            sleep(Duration::from_millis(42));
                        }
                    });
                }

                while Instant::now() < shutdown_time {
                    let mut buf = vec![];
                    let reads = connection.read_until(b'\n', &mut buf);

                    if let Ok(reads) = reads {
                        if reads > 0 {
                            nothing_to_do = false;
                            let pkg = serde_json::from_slice::<RemoteCommands>(&*buf)
                                .unwrap_or(Heartbeat);
                            /*if pkg != Heartbeat {
                                println!("msg from master: {:?}", pkg);
                            }*/

                            match pkg {
                                PeerIps(map) => {
                                    (*config.lock().unwrap()).0 = map;
                                    reload_config.store(true, Ordering::SeqCst);
                                }
                                FocusPoint { id } => {
                                    (*config.lock().unwrap()).1 = Some(id);
                                    reload_config.store(true, Ordering::SeqCst);
                                }
                                Heartbeat => {}
                                ClientHello { .. } => {
                                    println!("invalid pkg {:?}", pkg);
                                }
                            }
                            write(connection.get_mut(), &Heartbeat).unwrap();
                        }
                    }

                    if nothing_to_do {
                        sleep(Duration::from_secs(1));
                    }
                    nothing_to_do = true;
                }
            }
        }
        Localhost => {
            println!("no networking required, use channel");
        }
    };

    println!("clean shutdown");
}

fn get_focus_peer(
    map: &mut HashMap<usize, Option<(IpAddr, u16)>>,
    focus_id: &Option<usize>,
) -> Option<TcpStream> {
    if let Some(focus_id) = *focus_id {
        if let Occupied(a) = map.entry(focus_id) {
            if let Some((ip, data_port)) = *a.get() {
                for _ in 0..5 {
                    if let Ok(stream) = TcpStream::connect((ip, data_port)) {
                        return Some(stream);
                    } else {
                        sleep(Duration::from_millis(500));
                    }
                }
            }
        }
    }
    None
}

enum CheckinStatus {
    Registered,
    Connected {
        stream: BufReader<TcpStream>,
        data_port: u16,
    },
    Gone,
}
use CheckinStatus::*;

impl std::fmt::Debug for CheckinStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let l = match *self {
            Registered => "Registered",
            Connected { .. } => "Connected",
            Gone => "Gone",
        };
        write!(f, "{}", l)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum RemoteCommands {
    Heartbeat,
    ClientHello { id: usize, data_port: u16 },
    PeerIps(HashMap<usize, Option<(IpAddr, u16)>>),
    FocusPoint { id: usize },
}
use RemoteCommands::*;

fn write(stream: &mut TcpStream, pkg: &RemoteCommands) -> std::io::Result<usize> {
    let mut msg = serde_json::to_vec(&pkg).unwrap();
    msg.push(b'\n');
    stream.write(&*msg)
}
