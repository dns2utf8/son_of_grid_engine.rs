extern crate son_of_grid_engine as sge;

fn main() {
    let info = sge::SystemInfo::discover();

    println!("{:#?}", info);
}
