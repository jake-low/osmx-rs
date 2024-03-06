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

    // open the .osmx database file
    let db = osmx::Database::open(&file_path)?;
    // begin a read transaction (ensures that all reads see a coherent snapshot
    // of the data, even if another process is writing at the same time)
    let txn = osmx::Transaction::begin(&db)?;

    // get the ways table (containing tags, metadata, and node refs for each way)
    let ways = txn.ways()?;
    // get the locations table (containing coordinates for each node)
    let locations = txn.locations()?;

    // look up the given way ID in the ways table
    let way = ways.get(way_id).expect("way not found");

    // if the way has a "name" tag, print it
    if let Some(name) = way.tag("name") {
        print!("{}", name);
    }

    // get the way's node refs, and look up each node's location
    let coords = way.nodes().map(|node_id| {
        let loc = locations.get(node_id).unwrap();
        (loc.lon(), loc.lat())
    });

    // print the resulting coordinate sequence as a WKT linestring
    println!(
        "\tLINESTRING ({})",
        coords
            .map(|(lon, lat)| format!("{:.7} {:.7}", lon, lat))
            .join(",")
    );

    Ok(())
}
