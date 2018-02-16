//! This crate allows for easy detection of the cluster environment.
//!
//! ```
//! extern crate son_of_grid_engine as sge;
//! use std::thread::spawn;
//!
//! let cluster = sge::discover();
//! let (tx, rx) = std::sync::mpsc::channel();
//! for i in 0..cluster.available_cpus() {
//!     let tx = tx.clone();
//!     spawn(move || {
//!         tx.send(i).expect("channel is still aroun");
//!     });
//! }
//! drop(tx);
//!
//! assert_eq!(
//!     (0..cluster.available_cpus()).sum::<usize>(),
//!     rx.iter().sum()
//! );
//! ```

extern crate file;
extern crate libc;
extern crate num_cpus;
extern crate rand;
extern crate threadpool;

use std::env::var;
use std::path::PathBuf;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::io;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::Duration;
use threadpool::{Builder, ThreadPool};
use libc::{/*CPU_SETSIZE, */ cpu_set_t, /*sched_getaffinity, */ sched_setaffinity,
           /*CPU_ISSET, */ CPU_SET};

pub fn discover() -> SystemInfo {
    let cpus = var("SGE_BINDING")
        .unwrap_or("".into())
        .split_whitespace()
        .map(|s| s.parse().expect("must provide an array of numbers"))
        .collect();

    let job_id = var("JOB_ID")
        .map_err(err_to_string)
        .and_then(|s| s.parse().map_err(err_to_string))
        .unwrap_or(0);
    let job_type = parse_job_type();

    SystemInfo {
        cpus,
        scratch_path: parse_scratch_path(var("MCR_CACHE_ROOT")),
        queue_name: var("QUEUE").unwrap_or("".into()),
        networking: NetworkInfo::init(&job_type, job_id),
        job_id,
        job_type,
    }
}

#[derive(Debug)]
pub struct SystemInfo {
    cpus: Vec<usize>,
    pub scratch_path: PathBuf,
    pub queue_name: String,
    pub job_id: usize,
    pub job_type: JobType,
    pub networking: NetworkInfo,
}

impl SystemInfo {
    pub fn is_multicore(&self) -> bool {
        self.cpus.len() > 1
    }

    /// Fallback to detectable cpus
    pub fn available_cpus(&self) -> usize {
        let n = self.cpus.len();
        if n > 0 {
            n
        } else {
            num_cpus::get()
        }
    }

    /// Get a ThreadPool with the workers already pinned to the available cpus
    ///
    /// ```
    /// extern crate son_of_grid_engine as sge;
    ///
    /// let info = sge::discover();
    /// let pool = info.get_pinned_threadpool();
    ///
    /// for i in 0..128 {
    ///     pool.execute(move || {
    ///         println!("{}", i);
    ///     });
    /// }
    /// ```
    pub fn get_pinned_threadpool(&self) -> ThreadPool {
        let mut builder = Builder::new().num_threads(self.available_cpus())
                                        // 16MB Stack
                                        .thread_stack_size(16 * 1024 * 1024);
        let queue_name = self.queue_name.trim();
        if queue_name != "" {
            builder = builder.thread_name(queue_name.into());
        }
        let mut pool = builder.build();
        self.pin_workers(&mut pool);
        pool
    }

    pub fn pin_workers(&self, pool: &mut ThreadPool) {
        let n = self.available_cpus();
        let cpus = {
            if self.cpus.len() != n {
                (0..n).collect()
            } else {
                self.cpus.clone()
            }
        };
        pool.join();

        pool.set_num_threads(n);

        let barrier = Arc::new(Barrier::new(n));
        for core_id in cpus {
            let barrier = barrier.clone();
            pool.execute(move || {
                // wait until all workers are online
                barrier.wait();

                let mut set = new_cpu_set();
                unsafe {
                    // enable just one core
                    CPU_SET(core_id, &mut set);
                    sched_setaffinity(
                        0, // Defaults to current thread
                        std::mem::size_of::<cpu_set_t>(),
                        &set,
                    );
                }
            });
        }
        pool.join();
    }

    pub fn get_master_ips(&self) -> Vec<IpAddr> {
        match self.networking {
            Localhost | Master => vec!["::1".parse().expect("ipv6 localhost")],
            Client => {
                let name = gen_network_info_string(self.job_id);
                for _ in 0..23 {
                    match file::get(&*name).map(|b| String::from_utf8(b).unwrap()) {
                        Ok(master) => {
                            return master
                                .split("|")
                                .nth(0)
                                .unwrap()
                                .split_whitespace()
                                .map(|s| s.parse().unwrap())
                                .collect()
                        }
                        Err(_) => sleep(Duration::from_secs(2)),
                    }
                }
                panic!("unable to open master file: {:?}", name);
            }
        }
    }

    pub fn listen_on_port(&self, port: u16) -> io::Result<TcpListener> {
        TcpListener::bind(SocketAddr::new(
            "::".parse().expect("unable to parse ::"),
            port,
        ))
    }

    pub fn connect_to_master(&self, port: u16) -> TcpStream {
        let a = self.get_master_ips()
            .iter()
            .map(|i| SocketAddr::new(*i, port))
            .collect::<Vec<SocketAddr>>();

        let n_retries = 13;
        for _ in 0..n_retries {
            if let Ok(s) = TcpStream::connect(&a[..]) {
                return s;
            }
            sleep(Duration::from_secs(2));
        }
        panic!("unable to connect, failed after {}", n_retries);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobType {
    /// The default value
    Interactive,
    Batch,
    Array {
        id: usize,
        first: usize,
        last: usize,
        step_size: usize,
    },
}
use JobType::*;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum NetworkInfo {
    Localhost,
    Master,
    Client,
}
use NetworkInfo::*;

impl NetworkInfo {
    pub fn init(job_type: &JobType, job_id: usize) -> NetworkInfo {
        match *job_type {
            Interactive | Batch => Localhost,
            Array { id, first, .. } => {
                if id == first {
                    let ips: String = get_local_ips()
                        .iter()
                        .filter(|i| i.is_unspecified() == false && i.is_loopback() == false)
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let hostname = get_local_hostname();

                    file::put(
                        &*gen_network_info_string(job_id),
                        &*format!("{}|{}", ips, hostname),
                    ).expect("unable to write master info");
                    Master
                } else {
                    Client
                }
            }
        }
    }
}

fn gen_network_info_string(job_id: usize) -> String {
    let full = std::env::args().nth(0).unwrap();
    let binary_name = full.split("/").last().unwrap();
    format!("{}.{}.sge_rs", binary_name, job_id)
}

fn parse_job_type() -> JobType {
    let mut job_type = match &*var("ENVIRONMENT").unwrap_or("".into()) {
        "BATCH" => Batch,
        _ => Interactive,
    };

    if job_type == Batch {
        let envs = [
            var("SGE_TASK_FIRST"),
            var("SGE_TASK_ID"),
            var("SGE_TASK_LAST"),
            var("SGE_TASK_STEPSIZE"),
        ].iter()
            .map(|e| {
                e.clone()
                    .map_err(err_to_string)
                    .and_then(|s| s.parse::<usize>().map_err(err_to_string))
            })
            .collect::<Result<Vec<usize>, _>>();

        if let Ok(envs) = envs {
            let mut it = envs.iter();
            let first = *it.next().unwrap();
            let id = *it.next().unwrap();
            let last = *it.next().unwrap();
            let step_size = *it.next().unwrap();

            assert!(first < last);
            assert!(step_size > 0);
            assert!(id >= first);
            assert!(id <= last);
            job_type = Array {
                first,
                id,
                last,
                step_size,
            };
        }
    }

    job_type
}

fn err_to_string<E: std::fmt::Debug>(e: E) -> String {
    format!("{:?}", e)
}

fn parse_scratch_path(a: Result<String, std::env::VarError>) -> PathBuf {
    a.map(|s| s.into()).unwrap_or_else(|_| {
        let p = create_random_temp_dir();
        println!("parse_scratch_path() fallback to {:?}", p);
        p
    })
}

fn new_cpu_set() -> cpu_set_t {
    unsafe { std::mem::zeroed::<cpu_set_t>() }
}

fn create_random_temp_dir() -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("sge.fallback_{}", rand::random::<usize>()));
    std::fs::create_dir_all(&p).expect("unable to create temp_dir");
    p
}

fn get_local_hostname() -> String {
    file::get("/etc/hostname")
        .map(|b| String::from_utf8(b).unwrap())
        .unwrap_or("localhost".into())
}

fn get_local_ips() -> Vec<IpAddr> {
    let ip_a = exec("ip", &["a"])
        //.map(|ip4| ip4 + &*exec("ip", &["-6", "a"]).unwrap())
        .map(parse_ip);
    match ip_a {
        Ok(v) => v,
        Err(e) => {
            println!("fallback to ifconfig {:?}", e);
            exec("ifconfig", &[]).map(parse_ifconfig).unwrap_or(vec![])
        }
    }
}

fn parse_ip(s: String) -> Vec<IpAddr> {
    let ip_mask = s.lines()
        .filter(|l| l.contains("inet"))
        .map(|l| l.trim().split_whitespace().nth(1).unwrap());
    ip_mask
        .map(|s| s.split("/").nth(0).unwrap())
        .map(|s| s.parse().unwrap())
        .collect()
}

fn parse_ifconfig(s: String) -> Vec<IpAddr> {
    let ip_mask = s.lines().filter(|l| l.contains("inet")).collect::<Vec<_>>();
    ip_mask
        .iter()
        .map(|s| {
            let s = s.trim();
            s.split_whitespace()
                .nth(1)
                .expect("select ip and mask")
                .split("/")
                .nth(0)
//.map(|s| {println!("{:?}", s); s})
                .expect("select ip")
                .parse()
                .unwrap()
        })
        .collect()
}

fn exec(cmd: &str, args: &[&str]) -> std::io::Result<String> {
    Command::new(cmd)
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&*o.stdout).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_connect_master() {
        let info = SystemInfo {
            cpus: vec![1, 2],
            scratch_path: create_random_temp_dir(),
            queue_name: "test".into(),
            job_id: 0,
            job_type: Array {
                id: 1,
                first: 1,
                last: 42,
                step_size: 2,
            },
            networking: Master,
        };

        assert_eq!(
            vec!["::1".parse::<IpAddr>().unwrap()],
            info.get_master_ips()
        );

        std::fs::remove_dir_all(info.scratch_path).unwrap();
    }

    #[test]
    fn ifconfig() {
        let s = r##"
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP group default qlen 1000
    link/ether aa:bb:cc:dd:ee:ff brd ff:ff:ff:ff:ff:ff
    inet 42.42.42.42/24 brd 42.42.42.255 scope global eth0
       valid_lft forever preferred_lft forever
    inet6 fe80::21e:67ff:ffff:4242/64 scope link
       valid_lft forever preferred_lft forever
3: eth1: <BROADCAST,MULTICAST> mtu 1500 qdisc noop state DOWN group default qlen 1000
"##;

        let ips = parse_ifconfig(s.into());

        let res = [
            "127.0.0.1",
            "::1",
            "42.42.42.42",
            "fe80::21e:67ff:ffff:4242",
        ].iter()
            .map(|s| s.parse().unwrap())
            .collect::<Vec<IpAddr>>();
        assert_eq!(res, ips);
    }

    #[test]
    fn ip_a() {
        let s = r##"
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP group default qlen 1000
    link/ether aa:bb:cc:dd:ee:ff brd ff:ff:ff:ff:ff:ff
    inet 42.42.42.42/24 brd 42.42.42.255 scope global eth0
       valid_lft forever preferred_lft forever
    inet6 fe80::21e:67ff:ffff:4242/64 scope link
       valid_lft forever preferred_lft forever
3: eth1: <BROADCAST,MULTICAST> mtu 1500 qdisc noop state DOWN group default qlen 1000
    link/ether 00:11:22:33:44:55 brd ff:ff:ff:ff:ff:ff
"##;

        let ips = parse_ip(s.into());

        let res = [
            "127.0.0.1",
            "::1",
            "42.42.42.42",
            "fe80::21e:67ff:ffff:4242",
        ].iter()
            .map(|s| s.parse().unwrap())
            .collect::<Vec<IpAddr>>();
        assert_eq!(res, ips);
    }

    #[test]
    fn ip_6_a() {
        let s = r##"
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qlen 1000
    inet6 fe80::21e:67ff:ffff:4242/64 scope link
       valid_lft forever preferred_lft forever
"##;

        let ips = parse_ip(s.into());

        let res = ["::1", "fe80::21e:67ff:ffff:4242"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect::<Vec<IpAddr>>();
        assert_eq!(res, ips);
    }

}
