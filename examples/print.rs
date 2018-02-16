extern crate son_of_grid_engine as sge;

fn main() {
    let info = sge::discover();

    println!("{:#?}", info);
}
