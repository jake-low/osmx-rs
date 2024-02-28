// This crate does nothing right now; the only code is in the examples directory.
// I ported the examples first to get a feel for what abstractions this crate
// should provide; now that they are working I'm going to start refactoring
// repetitive code out of those programs and into here.

use std::char::UNICODE_VERSION;
use std::path::Path;
use std::{cell::OnceCell, error::Error};

use capnp::{
    message::{ReaderOptions, TypedReader},
    serialize::BufferSegments,
};
use genawaiter::rc::Gen;
use itertools::Itertools;

mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

use lmdb::{Cursor, Transaction as LmdbTransaction};
use s2::cellid::CellID;

const CELL_INDEX_LEVEL: u64 = 16;
const COORDINATE_PRECISION: i32 = 10000000;

pub struct Database {
    env: lmdb::Environment,

    // tables that store OSM object data (keyed by ID)
    locations: lmdb::Database,
    nodes: lmdb::Database,
    ways: lmdb::Database,
    relations: lmdb::Database,
    // spatial index table for nodes/locations (keyed by S2 cell ID)
    cell_node: lmdb::Database,
    // tables that map OSM object IDs to parent IDs
    node_way: lmdb::Database,
    node_relation: lmdb::Database,
    way_relation: lmdb::Database,
    relation_relation: lmdb::Database,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let env = lmdb::Environment::new()
            .set_flags(
                lmdb::EnvironmentFlags::NO_SUB_DIR
                    | lmdb::EnvironmentFlags::NO_READAHEAD
                    | lmdb::EnvironmentFlags::NO_SYNC,
            )
            .set_max_dbs(10)
            .set_map_size(50 * 1024 * 1024 * 1024) // 50 GiB
            .open(path.as_ref())?;

        let locations = env.open_db(Some("locations"))?;
        let nodes = env.open_db(Some("nodes"))?;
        let ways = env.open_db(Some("ways"))?;
        let relations = env.open_db(Some("relations"))?;
        let cell_node = env.open_db(Some("cell_node"))?;
        let node_way = env.open_db(Some("node_way"))?;
        let node_relation = env.open_db(Some("node_relation"))?;
        let way_relation = env.open_db(Some("way_relation"))?;
        let relation_relation = env.open_db(Some("relation_relation"))?;

        Ok(Self {
            env,
            // locations: env.open_db(Some("locations"))?,
            // nodes: env.open_db(Some("nodes"))?,
            // ways: env.open_db(Some("ways"))?,
            // relations: env.open_db(Some("relations"))?,
            // cell_node: env.open_db(Some("cell_node"))?,
            // node_way: env.open_db(Some("cell_node"))?,
            // node_relation: env.open_db(Some("cell_node"))?,
            // way_relation: env.open_db(Some("cell_node"))?,
            // relation_relation: env.open_db(Some("cell_node"))?,
            locations,
            nodes,
            ways,
            relations,
            cell_node,
            node_way,
            node_relation,
            way_relation,
            relation_relation,
        })
    }
}

pub struct Transaction<'db> {
    db: &'db Database,
    txn: lmdb::RoTransaction<'db>, // TODO support write txns
}

impl<'db> Transaction<'db> {
    pub fn begin(db: &'db Database) -> Result<Self, Box<dyn Error>> {
        let txn = db.env.begin_ro_txn()?;
        Ok(Self { db, txn })
    }

    pub fn get_way_by_id(&'db self, id: u64) -> Result<Way<'db>, Box<dyn Error>> {
        Ok(Way::from_bytes(
            self.txn.get(self.db.ways, &id.to_le_bytes())?,
        ))
    }

    pub fn get_location_by_id(&'db self, id: u64) -> Result<Location<'db>, Box<dyn Error>> {
        Ok(Location::from_bytes(
            self.txn.get(self.db.locations, &id.to_le_bytes())?,
        ))
    }

    pub fn get_ways_for_node(
        &'db self,
        id: u64,
    ) -> Result<impl Iterator<Item = u64> + 'db, Box<dyn Error>> {
        let mut cursor = self.txn.open_ro_cursor(self.db.node_way)?;

        let result = match cursor.iter_dup_of(&id.to_le_bytes()) {
            Ok(iter) => Some(iter),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => return Err(Box::new(e)),
        };

        Ok(Gen::new(|co| async move {
            let _cursor = cursor; // must move cursor into closure or SIGSEGV
            if let Some(iter) = result {
                for (_, raw_val) in iter {
                    let val =
                        u64::from_le_bytes(raw_val.try_into().expect("val with incorrect length"));
                    co.yield_(val).await;
                }
            }
        })
        .into_iter())
    }

    pub fn get_node_ids_in_region(
        &'db self,
        region: &'db Region,
    ) -> Result<impl Iterator<Item = u64> + 'db, Box<dyn Error>> {
        let mut cursor = self.txn.open_ro_cursor(self.db.cell_node)?;

        return Ok(Gen::new(|co| async move {
            for cell_id in region.cells.0.clone() {
                let start = cell_id.child_begin_at_level(CELL_INDEX_LEVEL);
                let end = cell_id.child_end_at_level(CELL_INDEX_LEVEL);

                for (_raw_key, raw_val) in cursor
                    .iter_dup_from(&start.0.to_le_bytes())
                    .flatten()
                    .take_while(|&(raw_key, _raw_val)| {
                        end.0
                            > u64::from_le_bytes(
                                raw_key.try_into().expect("key with incorrect length"),
                            )
                    })
                {
                    let node_id =
                        u64::from_le_bytes(raw_val.try_into().expect("val with incorrect length"));
                    co.yield_(node_id).await;
                }
            }
        })
        .into_iter());
    }
}

pub struct Region {
    cells: s2::cellunion::CellUnion,
}

const COVERER: OnceCell<s2::region::RegionCoverer> = OnceCell::new();

impl Region {
    pub fn from_bbox(west: f64, south: f64, east: f64, north: f64) -> Self {
        let rect = s2::rect::Rect::from_degrees(south, west, north, east);

        let cells = COVERER
            .get_or_init(|| s2::region::RegionCoverer {
                min_level: 4,
                max_level: 16,
                level_mod: 1,
                max_cells: 8,
            })
            .covering(&rect);

        Self { cells }
    }
}

pub struct Way<'a> {
    reader: TypedReader<BufferSegments<&'a [u8]>, messages_capnp::way::Owned>,
}

impl<'a> Way<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        let options = ReaderOptions::new();
        let segments = BufferSegments::new(bytes, options).unwrap();

        Self {
            reader: capnp::message::Reader::new(segments, options).into_typed(),
        }
    }

    pub fn tags(&'a self) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.reader
            .get()
            .unwrap()
            .get_tags()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_str().unwrap())
            .tuples::<(&'a str, &'a str)>()
    }

    pub fn nodes(&'a self) -> impl Iterator<Item = u64> + 'a {
        self.reader.get().unwrap().get_nodes().unwrap().iter()
    }
}

pub struct Location<'a> {
    buf: &'a [u8],
}

impl<'a> Location<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { buf: bytes }
    }

    pub fn lon(&self) -> f64 {
        let as_i32 = i32::from_le_bytes(self.buf[0..4].try_into().unwrap());
        as_i32 as f64 / COORDINATE_PRECISION as f64
    }

    pub fn lat(&self) -> f64 {
        let as_i32 = i32::from_le_bytes(self.buf[4..8].try_into().unwrap());
        as_i32 as f64 / COORDINATE_PRECISION as f64
    }
}

// pub struct Tag<'a>(&'a str, &'a str);
