/// Example program which finds all Ways in an .osmx file which are
/// within a given bounding box, and prints each one's 'name' and
/// its geometry in WKT form.
///
/// Usage: bbox_wkt OSMX_FILE MIN_LON MIN_LAT MAX_LON MAX_LAT
///
/// Ported from this C++ program, and should behave identically:
/// https://github.com/protomaps/OSMExpress/blob/main/examples/bbox_wkt.cpp
use std::error::Error;

use itertools::Itertools;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = std::path::PathBuf::from(&args[1]);

    let db = osmx::Database::open(&file_path)?;
    let txn = osmx::Transaction::begin(&db)?;

    let ways = txn.ways()?;
    let locations = txn.locations()?;

    let bbox: Vec<f64> = args[2..]
        .iter()
        .map(|s| s.parse::<f64>().unwrap())
        .collect();
    let region = osmx::Region::from_bbox(bbox[0], bbox[1], bbox[2], bbox[3]);

    // eprintln!("Cell covering size: {}", region.cells.0.len());

    // get IDs of all Nodes that match the given region
    let node_ids: roaring::RoaringTreemap = txn.get_node_ids_in_region(&region)?.collect();

    eprintln!("Nodes in region: {}", node_ids.len());

    // get IDs of Ways that contain the matched Nodes
    let mut way_ids = roaring::RoaringTreemap::new();

    for node_id in node_ids {
        way_ids.extend(txn.get_ways_for_node(node_id)?);
    }

    eprintln!("Ways in region: {}", way_ids.len());

    // Print names and WKT geometries for each way
    // let mut cursor = txn.open_ro_cursor(locations)?;
    for way_id in way_ids {
        let way = ways.get(way_id).unwrap();

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
    }

    Ok(())
}
