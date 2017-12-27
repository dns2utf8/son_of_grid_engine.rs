//! This crate allows for easy detection of the cluster environment.
//!
//! ```
//! extern crate son_of_grid_engine as sge;
//! use std::thread::spawn;
//!
//! let cluster = sge::SystemInfo::discover();
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

extern crate num_cpus;
extern crate threadpool;
extern crate libc;

use std::env::var;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use threadpool::{ThreadPool, Builder};
use libc::{/*CPU_ISSET, */ CPU_SET, /*CPU_SETSIZE, */ cpu_set_t, /*sched_getaffinity, */ sched_setaffinity};

#[derive(Debug)]
pub struct SystemInfo {
    cpus: Vec<usize>,
    scratch_path: PathBuf,
    queue_name: String,
    job_type: JobType,
    networking: NetworkInfo,
}

impl SystemInfo {
    pub fn discover() -> SystemInfo {
        let cpus = var("SGE_BINDING")
            .unwrap_or("".into())
            .split_whitespace()
            .map(|s| s.parse().expect("must provide an array of numbers"))
            .collect();

        let job_type = parse_job_type();

        SystemInfo {
            cpus,
            scratch_path: parse_scratch_path(var("MCR_CACHE_ROOT")),
            queue_name: var("QUEUE").unwrap_or("".into()),
            networking: NetworkInfo::init(&job_type),
            job_type,
        }
    }

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
    /// let info = sge::SystemInfo::discover();
    /// let pool = info.get_pinned_threadpool();
    ///
    /// for i in 0..128 {
    ///     pool.execute(move || {
    ///         println!("{}", i);
    ///     });
    /// }
    /// ```
    pub fn get_pinned_threadpool(&self) -> ThreadPool {
        let mut pool = Builder::new()
                        .num_threads(self.available_cpus())
                        .build();
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
                    sched_setaffinity(0, // Defaults to current thread
                              std::mem::size_of::<cpu_set_t>(),
                              &set);
                }
            });
        }
        pool.join();
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


#[derive(Debug)]
pub enum NetworkInfo {
    Localhost,
    Master,
    Client,
}
use NetworkInfo::*;

impl NetworkInfo {
    pub fn init(job_type: &JobType) -> NetworkInfo {
        match *job_type {
            Interactive | Batch => Localhost,
            Array { id, first, _} => {
                if id == first {
                    Master
                } else {
                    Client
                }
            }
        }
    }
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
            job_type = Array {
                first: *it.next().unwrap(),
                id: *it.next().unwrap(),
                last: *it.next().unwrap(),
                step_size: *it.next().unwrap(),
            };
        }
    }

    job_type
}

fn err_to_string<E: std::fmt::Debug>(e: E) -> String {
    format!("{:?}", e)
}

fn parse_scratch_path(a: Result<String, std::env::VarError>) -> PathBuf {
    a.map(|s| s.into()).unwrap_or(std::env::temp_dir())
}

fn new_cpu_set() -> cpu_set_t {
    unsafe { std::mem::zeroed::<cpu_set_t>() }
}

#[cfg(test)]
mod tests {}
