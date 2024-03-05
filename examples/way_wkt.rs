/// Example program which reads a Way by ID from an .osmx file,
/// and prints its 'name' and its geometry in WKT form.
///
/// Usage: way_wkt OSMX_FILE WAY_ID
///
/// Ported from this C++ program, and should behave identically:
/// https://github.com/protomaps/OSMExpress/blob/main/examples/way_wkt.cpp
use std::error::Error;

use itertools::Itertools;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = std::path::PathBuf::from(&args[1]);
    let way_id: u64 = str::parse(&args[2])?;

    let db = osmx::Database::open(&file_path)?;
    let txn = osmx::Transaction::begin(&db)?;
    // let way = txn.get_way_by_id(way_id)?;
    let ways = txn.ways()?;
    let locations = txn.locations()?;

    let way = ways.get(way_id).expect("way not found");

    for (key, val) in way.tags() {
        if key == "name" {
            print!("{}", val);
        }
    }

    let coords = way.nodes().map(|node_id| {
        let loc = locations.get(node_id).unwrap();
        (loc.lon(), loc.lat())
    });

    println!(
        "\tLINESTRING ({})",
        coords
            .map(|(lon, lat)| format!("{:.7} {:.7}", lon, lat))
            .join(",")
    );

    Ok(())
}
