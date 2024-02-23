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
use lmdb::{Cursor, Transaction};

mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

use capnp::message::ReaderOptions;

const CELL_INDEX_LEVEL: u64 = 16;
const COORDINATE_PRECISION: i32 = 10000000;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = std::path::PathBuf::from(&args[1]);

    let env = lmdb::Environment::new()
        .set_flags(
            lmdb::EnvironmentFlags::NO_SUB_DIR
                | lmdb::EnvironmentFlags::NO_READAHEAD
                | lmdb::EnvironmentFlags::NO_SYNC,
        )
        .set_max_dbs(10)
        .set_map_size(50 * 1024 * 1024 * 1024) // 50 GiB
        .open(&file_path)?;

    let cells = env.open_db(Some("cell_node"))?;
    let node_ways = env.open_db(Some("node_way"))?;
    let ways = env.open_db(Some("ways"))?;
    let locations = env.open_db(Some("locations"))?;

    let txn = env.begin_ro_txn()?;

    let bbox: Vec<f64> = args[2..]
        .iter()
        .map(|s| s.parse::<f64>().unwrap())
        .collect();
    let rect = s2::rect::Rect::from_degrees(bbox[1], bbox[0], bbox[3], bbox[2]);

    let coverer = s2::region::RegionCoverer {
        min_level: 4,
        max_level: 16,
        level_mod: 1,
        max_cells: 8,
    };
    let covered_cells = coverer.covering(&rect);

    eprintln!("Cell covering size: {}", covered_cells.0.len());

    // get IDs of all Nodes that match the given region
    let mut node_ids = roaring::RoaringTreemap::new();
    let mut cursor = txn.open_ro_cursor(cells)?;

    for cell_id in covered_cells.0 {
        let start = cell_id.child_begin_at_level(CELL_INDEX_LEVEL);
        let end = cell_id.child_end_at_level(CELL_INDEX_LEVEL);

        for (_raw_key, raw_val) in cursor
            .iter_dup_from(&start.0.to_le_bytes())
            .flatten()
            .take_while(|&(raw_key, _raw_val)| {
                end.0 > u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"))
            })
        {
            let node_id =
                u64::from_le_bytes(raw_val.try_into().expect("val with incorrect length"));
            node_ids.insert(node_id);
        }
    }

    eprintln!("Nodes in region: {}", node_ids.len());

    // get IDs of Ways that contain the matched Nodes
    let mut way_ids = roaring::RoaringTreemap::new();
    let mut cursor = txn.open_ro_cursor(node_ways)?;

    for node_id in node_ids {
        // Look up the node ID in the nodes-to-ways table
        match cursor.iter_dup_of(&node_id.to_le_bytes()) {
            Ok(iter) => {
                // the iterator contains pairs of node IDs and way IDs; we only need the way IDs
                way_ids.extend(iter.map(|(_raw_key, raw_val)| {
                    u64::from_le_bytes(raw_val.try_into().expect("val with incorrect length"))
                }))
            }
            Err(lmdb::Error::NotFound) => {
                // It's okay if the node ID is not found (just means it's not part of any way)
                continue;
            }
            Err(e) => {
                // other errors are unexpected, so they are propagated to the caller
                Err(e)?;
            }
        }
    }

    eprintln!("Ways in region: {}", way_ids.len());

    // Print names and WKT geometries for each way
    // let mut cursor = txn.open_ro_cursor(locations)?;
    for way_id in way_ids {
        let raw_way = txn.get(ways, &way_id.to_le_bytes())?;
        let reader = capnp::serialize::read_message(raw_way, ReaderOptions::new()).unwrap();
        let way = reader.get_root::<messages_capnp::way::Reader>().unwrap();
        let tags = way.get_tags()?;
        for (key, val) in tags
            .iter()
            .map(|v| v.unwrap().to_str().unwrap())
            .tuples::<(&str, &str)>()
        {
            if key == "name" {
                print!("{}", val)
            }
        }

        let coords = way.get_nodes()?.iter().map(|node_id| {
            let raw_loc = txn
                .get(locations, &node_id.to_le_bytes())
                .expect("location not found");
            let loc: [u8; 12] = raw_loc.try_into().expect("loc with incorrect length");
            let lon_i32 = i32::from_le_bytes(loc[0..4].try_into().unwrap());
            let lat_i32 = i32::from_le_bytes(loc[4..8].try_into().unwrap());
            let _version = i32::from_le_bytes(loc[8..12].try_into().unwrap());
            let lon: f64 = lon_i32 as f64 / COORDINATE_PRECISION as f64;
            let lat: f64 = lat_i32 as f64 / COORDINATE_PRECISION as f64;

            (lon, lat)
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
