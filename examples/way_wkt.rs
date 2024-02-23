/// Example program which reads a Way by ID from an .osmx file,
/// and prints its 'name' and its geometry in WKT form.
///
/// Usage: way_wkt OSMX_FILE WAY_ID
///
/// Ported from this C++ program, and should behave identically:
/// https://github.com/protomaps/OSMExpress/blob/main/examples/way_wkt.cpp
use std::error::Error;

use itertools::Itertools;
use lmdb::Transaction;

mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

const COORDINATE_PRECISION: i32 = 10000000;

use capnp::message::ReaderOptions;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = std::path::PathBuf::from(&args[1]);
    let way_id: u64 = str::parse(&args[2])?;

    let env = lmdb::Environment::new()
        .set_flags(
            lmdb::EnvironmentFlags::NO_SUB_DIR
                | lmdb::EnvironmentFlags::NO_READAHEAD
                | lmdb::EnvironmentFlags::NO_SYNC,
        )
        .set_max_dbs(10)
        .set_map_size(50 * 1024 * 1024 * 1024) // 50 GiB
        .open(&file_path)?;

    let ways = env.open_db(Some("ways"))?;
    let locations = env.open_db(Some("locations"))?;

    let txn = env.begin_ro_txn()?;

    let raw_way = txn.get(ways, &way_id.to_le_bytes())?;
    let reader = capnp::serialize::read_message(raw_way, ReaderOptions::new()).unwrap();
    let way = reader.get_root::<messages_capnp::way::Reader>().unwrap();

    for (key, val) in way
        .get_tags()?
        .iter()
        .map(|v| v.unwrap().to_str().unwrap())
        .tuples::<(&str, &str)>()
    {
        if key == "name" {
            print!("{}", val);
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

    Ok(())
}
