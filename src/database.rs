// This crate does nothing right now; the only code is in the examples directory.
// I ported the examples first to get a feel for what abstractions this crate
// should provide; now that they are working I'm going to start refactoring
// repetitive code out of those programs and into here.

use std::error::Error;
use std::path::Path;

use genawaiter::rc::Gen;

use lmdb::{Cursor, Transaction as LmdbTransaction};

use crate::types::{Location, Node, Region, Relation, Way};

// use lmdb_sys::{MDB_FIRST, MDB_NEXT};

// use s2::cellid::CellID;

const CELL_INDEX_LEVEL: u64 = 16;

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

    pub fn locations(&self) -> Result<Locations, Box<dyn Error>> {
        Ok(Locations {
            txn: &self.txn,
            table: self.db.locations,
        })
    }

    pub fn nodes(&self) -> Result<Nodes, Box<dyn Error>> {
        Ok(Nodes {
            txn: &self.txn,
            table: self.db.nodes,
        })
    }

    pub fn ways(&self) -> Result<Ways, Box<dyn Error>> {
        Ok(Ways {
            txn: &self.txn,
            table: self.db.ways,
        })
    }

    pub fn relations(&self) -> Result<Relations, Box<dyn Error>> {
        Ok(Relations {
            txn: &self.txn,
            table: self.db.relations,
        })
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

        Ok(Gen::new(|co| async move {
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
        .into_iter())
    }
}

// TODO: Ways and Relations will need similar implementations; can they all be parameterized
// from the same type? e.g. type Nodes = Reader<Node> etc

pub struct Nodes<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'a, 's> Nodes<'a> {
    pub fn get(&self, id: u64) -> Option<Node<'a>> {
        match self.txn.get(self.table, &id.to_le_bytes()) {
            Ok(raw_val) => Some(Node::from_bytes(raw_val)),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Node<'a>)> {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();
        Gen::new(|co| async move {
            let mut cursor = cursor;
            for (raw_key, raw_val) in cursor.iter_start() {
                let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
                let node = Node::from_bytes(raw_val);

                co.yield_((id, node)).await;
            }
        })
        .into_iter()
    }
}

pub struct Ways<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'a, 's> Ways<'a> {
    pub fn get(&self, id: u64) -> Option<Way<'a>> {
        match self.txn.get(self.table, &id.to_le_bytes()) {
            Ok(raw_val) => Some(Way::from_bytes(raw_val)),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Way<'a>)> {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();
        Gen::new(|co| async move {
            let mut cursor = cursor;
            for (raw_key, raw_val) in cursor.iter_start() {
                let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
                let way = Way::from_bytes(raw_val);

                co.yield_((id, way)).await;
            }
        })
        .into_iter()
    }
}

pub struct Relations<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'a, 's> Relations<'a> {
    pub fn get(&self, id: u64) -> Option<Relation<'a>> {
        match self.txn.get(self.table, &id.to_le_bytes()) {
            Ok(raw_val) => Some(Relation::from_bytes(raw_val)),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Relation<'a>)> {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();
        Gen::new(|co| async move {
            let mut cursor = cursor;
            for (raw_key, raw_val) in cursor.iter_start() {
                let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
                let relation = Relation::from_bytes(raw_val);

                co.yield_((id, relation)).await;
            }
        })
        .into_iter()
    }
}

pub struct Locations<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'a, 's> Locations<'a> {
    pub fn get(&self, id: u64) -> Option<Location<'a>> {
        match self.txn.get(self.table, &id.to_le_bytes()) {
            Ok(raw_val) => Some(Location::from_bytes(raw_val)),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Location<'a>)> {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();
        Gen::new(|co| async move {
            let mut cursor = cursor;
            for (raw_key, raw_val) in cursor.iter_start() {
                let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
                let way = Location::from_bytes(raw_val);

                co.yield_((id, way)).await;
            }
        })
        .into_iter()
    }
}
