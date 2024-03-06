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

    // open the .osmx database file
    let db = osmx::Database::open(&file_path)?;
    // begin a read transaction (ensures that all reads see a coherent snapshot
    // of the data, even if another process is writing at the same time)
    let txn = osmx::Transaction::begin(&db)?;

    // get the ways table (containing way tags, metadata, and node refs)
    let ways = txn.ways()?;
    // get the locations table (containing coordinates for each node)
    let locations = txn.locations()?;
    // get the cell_nodes spatial index table (maps S2 Cell IDs to OSM Node IDs)
    let cell_nodes = txn.cell_nodes()?;
    // get the node_ways table (which maps Node IDs to Way IDs that contain those nodes)
    let node_ways = txn.node_ways()?;

    let bbox: Vec<f64> = args[2..]
        .iter()
        .map(|s| s.parse::<f64>().unwrap())
        .collect();
    let region = osmx::Region::from_bbox(bbox[0], bbox[1], bbox[2], bbox[3]);

    // Use the spatial index to get IDs of all Nodes within the given region.
    // You could also collect() the iterator into a Vec<u64>, but for large
    // regions a RoaringTreemap is likely to be faster.
    let node_ids: roaring::RoaringTreemap = cell_nodes.find_in_region(&region).collect();

    eprintln!("Nodes in region: {}", node_ids.len());

    // get IDs of Ways that contain the matched Nodes
    let mut way_ids = roaring::RoaringTreemap::new();

    for node_id in node_ids {
        way_ids.extend(node_ways.get(node_id));
    }

    eprintln!("Ways in region: {}", way_ids.len());

    // Print names and WKT geometries for each way
    for way_id in way_ids {
        let way = ways.get(way_id).unwrap();

        // if the way has a "name" tag, print it
        if let Some(name) = way.tag("name") {
            print!("{}", name);
        }

        // get the way's node refs, and look up each node's location
        let coords = way.nodes().map(|node_id| {
            let loc = locations.get(node_id).unwrap();
            (loc.lon(), loc.lat())
        });

        // print the resulting coords as a WKT linestring
        println!(
            "\tLINESTRING ({})",
            coords
                .map(|(lon, lat)| format!("{:.7} {:.7}", lon, lat))
                .join(",")
        );
    }

    Ok(())
}
