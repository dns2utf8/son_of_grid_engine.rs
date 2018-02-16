# Son of Grid Engine (SGE)

[![Travis Build Status](https://travis-ci.org/dns2utf8/son_of_grid_engine.rs.svg?branch=master)](https://travis-ci.org/dns2utf8/son_of_grid_engine.rs)

## Example

### Interactive mode (testing)

```rust
$ cargo run --example print

SystemInfo {
    cpus: [],
    scratch_path: "/tmp",
    queue_name: "",
    job_type: Interactive
}
```

### Batch mode with `-pe multicore 16`

```rust
$ qsub -pe multicore 16 -l h_vmem=25G ~/sge_example_print.sh

SystemInfo {
    cpus: [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35
    ],
    scratch_path: "/scratch/sge/tmp/184868.1.hmem.q/mcr_cache",
    queue_name: "hmem.q",
    job_type: Batch
}
```

### Array mode with `-t 97-128`

```rust
$ qsub -t 97-128 -l h_vmem=2G ~/sge_example_print.sh

SystemInfo {
    cpus: [
        15,
        31
    ],
    scratch_path: "/scratch/sge/tmp/184858.100.array.q/mcr_cache",
    queue_name: "array.q",
    job_type: Array {
        id: 100,
        first: 97,
        last: 128,
        step_size: 1
    }
}
```

# Testing

## Test Array mode

```bash
RUST_BACKTRACE=1 ENVIRONMENT=BATCH SGE_TASK_ID=42 SGE_TASK_FIRST=42 SGE_TASK_LAST=46 SGE_TASK_STEPSIZE=1 cargo run --example print
```
