//! This crate allows for easy detection of the cluster environment.
//!
//! ```
//! extern crate son_of_grid_engine as sge;
//! use std::thread::spawn;
//!
//! let cluster = sge::SystemInfo::discover();
//! let (tx, rx) = std::sync::mpsc::channel();
//! for i in 0..cluster.available_cpus() {
//!     spawn(move || {
//!         tx.send(i).expect("channel is still aroun");
//!     });
//! }
//! drop(tx);
//!
//! assert_eq!(
//!     (0..cluster.available_cpus()).sum(),
//!     rx.iter().sum()
//! );
//! ```

extern crate num_cpus;

use std::env::var;
use std::path::PathBuf;

#[derive(Debug)]
pub struct SystemInfo {
    cpus: Vec<usize>,
    scratch_path: PathBuf,
    queue_name: String,
    job_type: JobType,
}

impl SystemInfo {
    pub fn discover() -> SystemInfo {
        let job_type = parse_job_type();

        let cpus = var("SGE_BINDING")
            .unwrap_or("".into())
            .split_whitespace()
            .map(|s| s.parse().expect("must provide an array of numbers"))
            .collect();

        SystemInfo {
            cpus,
            scratch_path: parse_scratch_path(var("MCR_CACHE_ROOT")),
            queue_name: var("QUEUE").unwrap_or("".into()),
            job_type,
        }
    }

    pub fn is_multicore(&self) -> bool {
        self.cpus.len() > 1
    }
    pub fn available_cpus(&self) -> usize {
        let n = self.cpus.len();
        if n > 0 {
            n
        } else {
            num_cpus::get()
        }
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

#[cfg(test)]
mod tests {}
