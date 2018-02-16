extern crate son_of_grid_engine as sge;

use std::fs::File;
use std::io::Write;

fn main() {
    let info = sge::discover();

    println!("{:#?}", info);

    let mut p = info.scratch_path.clone();
    p.push("my_temp_file.txt");
    let mut f = File::create(p).expect("unable to open local file");

    f.write_all(b"my custom content").unwrap();
}
