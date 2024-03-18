use std::error::Error;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use genawaiter::rc::Gen;
use lmdb_zero as lmdb;

use crate::types::{Location, Node, Region, Relation, Way};

pub const CELL_INDEX_LEVEL: u64 = 16;

/// A handle to an OSMX database file
pub struct Database {
    env: Arc<lmdb::Environment>,

    // tables that store OSM object data (keyed by ID)
    locations: lmdb::Database<'static>,
    nodes: lmdb::Database<'static>,
    ways: lmdb::Database<'static>,
    relations: lmdb::Database<'static>,
    // spatial index table for nodes/locations (keyed by S2 cell ID)
    cell_node: lmdb::Database<'static>,
    // tables that map OSM object IDs to parent IDs
    node_way: lmdb::Database<'static>,
    node_relation: lmdb::Database<'static>,
    way_relation: lmdb::Database<'static>,
    relation_relation: lmdb::Database<'static>,
}

impl Database {
    /// Open the given file path as an OSMX Database
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let mut builder = lmdb::EnvBuilder::new()?;
        builder.set_maxdbs(10)?;
        builder.set_mapsize(50 * 1024 * 1024 * 1024)?; // 50 GiB
        let env = unsafe {
            builder.open(
                path.as_ref().to_str().unwrap(),
                lmdb::open::NOSUBDIR | lmdb::open::NORDAHEAD | lmdb::open::NOSYNC,
                0o600,
            )?
        };

        let env = Arc::new(env);
        let options = lmdb::DatabaseOptions::defaults();

        let locations = lmdb::Database::open(env, Some("locations"), &options)?;
        let nodes = lmdb::Database::open(env, Some("nodes"), &options)?;
        let ways = lmdb::Database::open(env, Some("ways"), &options)?;
        let relations = lmdb::Database::open(env, Some("relations"), &options)?;
        let cell_node = lmdb::Database::open(env, Some("cell_node"), &options)?;
        let node_way = lmdb::Database::open(env, Some("node_way"), &options)?;
        let node_relation = lmdb::Database::open(env, Some("node_relation"), &options)?;
        let way_relation = lmdb::Database::open(env, Some("way_relation"), &options)?;
        let relation_relation = lmdb::Database::open(env, Some("relation_relation"), &options)?;

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

/// A handle which can be used to read from the Database. The handle
/// ensures that all reads see the same snapshot of the data, even if
/// it is being modified simultaneously by another process.
pub struct Transaction<'db> {
    db: &'db Database,
    txn: lmdb::ReadTransaction<'static>, // TODO support write txns?
}

impl<'db> Transaction<'db> {
    /// Create a new Transaction from the given Database.
    pub fn begin(db: &'db Database) -> Result<Self, Box<dyn Error>> {
        let txn = lmdb::ReadTransaction::new(db.env)?;
        Ok(Self { db, txn })
    }

    /// Get the Locations table, which maps OSM Node IDs to locations.
    pub fn locations(&self) -> Result<Locations, Box<dyn Error>> {
        Ok(Locations::new(&self.txn, &self.db.locations))
    }

    /// Get the Nodes table, which maps OSM Node IDs to their metadata and tags.
    pub fn nodes(&self) -> Result<Nodes, Box<dyn Error>> {
        Ok(Nodes::new(&self.txn, &self.db.nodes))
    }

    /// Get the Ways table, which maps OSM Way IDs to their metadata, tags, and node refs.
    pub fn ways(&self) -> Result<Ways, Box<dyn Error>> {
        Ok(Ways::new(&self.txn, &self.db.ways))
    }

    /// Get the Relations table, which maps OSM Relation IDs to their metadata, tags, and member refs.
    pub fn relations(&self) -> Result<Relations, Box<dyn Error>> {
        Ok(Relations::new(&self.txn, &self.db.relations))
    }

    /*
    /// Get the cell_nodes spatial index table which maps S2 Cell IDs to OSM Node IDs.
    pub fn cell_nodes(&self) -> Result<SpatialIndexTable, Box<dyn Error>> {
        Ok(SpatialIndexTable::new(&self.txn, &self.db.cell_node))
    }

    /// Get the join table which maps OSM Nodes to the Ways that the Node is part of.
    pub fn node_ways(&self) -> Result<JoinTable, Box<dyn Error>> {
        Ok(JoinTable::new(&self.txn, &self.db.node_way))
    }

    /// Get the join table which maps OSM Nodes to the Relations that the Node is a member of.
    pub fn node_relations(&self) -> Result<JoinTable, Box<dyn Error>> {
        Ok(JoinTable::new(&self.txn, &self.db.node_relation))
    }

    /// Get the join table which maps OSM Ways to the Relations that the Way is a member of.
    pub fn way_relations(&self) -> Result<JoinTable, Box<dyn Error>> {
        Ok(JoinTable::new(&self.txn, &self.db.way_relation))
    }

    /// Get the join table which maps OSM Relations to other Relations that they are members of.
    pub fn relation_relations(&self) -> Result<JoinTable, Box<dyn Error>> {
        Ok(JoinTable::new(&self.txn, &self.db.relation_relation))
    }
    */
}

/// A table that stores data associated with OSM elements, keyed by the element's ID.
/// The value type depends on what element is being stored. In an OSMX database, the
/// values are usually Cap'n Proto messages describing the element's properties.
pub struct ElementTable<'txn, E: TryFrom<&'txn [u8]> + 'txn> {
    txn: &'txn lmdb::ReadTransaction<'txn>,
    table: &'txn lmdb::Database<'txn>,
    phantom: PhantomData<E>,
}

impl<'txn, E: TryFrom<&'txn [u8]>> ElementTable<'txn, E> {
    fn new(txn: &'txn lmdb::ReadTransaction<'txn>, table: &'txn lmdb::Database) -> Self {
        Self {
            txn,
            table,
            phantom: PhantomData,
        }
    }

    /// Get an element by its ID. Returns None if the element is not found.
    pub fn get(&self, id: u64) -> Option<E> {
        let access = self.txn.access();
        match access.get(self.table, &id.to_le_bytes()) {
            Ok(raw_val) => Some(E::try_from(raw_val).ok().unwrap()),
            // Err(lmdb::Error::NotFound) => None,
            Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
        }
    }

    /// Iterate over all the elements in the table.
    pub fn iter<'s>(&'s self) -> impl Iterator<Item = (u64, E)> + 'txn {
        let access = self.txn.access();
        let cursor = self.txn.cursor(self.table).unwrap();
        Gen::new(|co| async move {
            let access = access;
            let mut cursor = cursor;
            while let Ok((raw_key, raw_val)) = cursor.next::<[u8], [u8]>(&access) {
                let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
                let elem = E::try_from(raw_val.clone()).ok().unwrap();

                co.yield_((id, elem)).await;
            }
        })
        .into_iter()
    }
}

/// A table which maps OSM Node IDs to structs containing the Node's lon/lat coordinates.
pub type Locations<'txn> = ElementTable<'txn, Location<'txn>>;

/// A table which maps OSM Node IDs to structs containing the Node's tags and metadata.
/// Untagged nodes are omitted from this table (they only exist in the Locations table).
pub type Nodes<'txn> = ElementTable<'txn, Node<'txn>>;

/// A table which maps OSM Way IDs to structs containing the Way's tags, metadata,
/// and the IDs of the Nodes that make up the Way.
pub type Ways<'txn> = ElementTable<'txn, Way<'txn>>;

/// A table which maps OSM Relation IDs to structs containing the Relations's tags,
/// metadata, and the IDs, types, and roles of the Relation's members.
pub type Relations<'txn> = ElementTable<'txn, Relation<'txn>>;

/*
/// A spatial index that permits fast spatial lookups of elements. Under the hood,
/// this is implemented as a table that maps S2 Cell IDs to OSM element IDs.
pub struct SpatialIndexTable<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'txn> SpatialIndexTable<'txn> {
    fn new(txn: &'txn lmdb::RoTransaction<'txn>, table: lmdb::Database) -> Self {
        Self { txn, table }
    }

    /// Given a Region, returns an iterator of IDs of elements that may fall within
    /// the region. There may be false positives (elements that are near, but not
    /// not truly within the given region) due to how the spatial index works.
    pub fn find_in_region(&self, region: &'txn Region) -> impl Iterator<Item = u64> + 'txn {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();

        Gen::new(|co| async move {
            let mut cursor = cursor;
            for cell_id in region.cells.0.clone() {
                let start = cell_id.child_begin_at_level(CELL_INDEX_LEVEL);
                let end = cell_id.child_end_at_level(CELL_INDEX_LEVEL);

                for (_, node_id) in cursor
                    .iter_dup_from(&start.0.to_le_bytes())
                    .flatten()
                    .map(|(raw_key, raw_val)| {
                        let cell_id = u64::from_le_bytes(
                            raw_key.try_into().expect("key with incorrect length"),
                        );
                        let node_id = u64::from_le_bytes(
                            raw_val.try_into().expect("val with incorrect length"),
                        );
                        (cell_id, node_id)
                    })
                    .take_while(|&(key, _)| end.0 > key)
                {
                    co.yield_(node_id).await;
                }
            }
        })
        .into_iter()
    }
}

/// A table that maps IDs of elements to IDs of other elements to which they are related.
/// For example, mapping Nodes to the Ways that they are part of, or mapping any elements
/// (Nodes, Ways, Relations) to the Relations that the elements are members of.
pub struct JoinTable<'txn> {
    txn: &'txn lmdb::RoTransaction<'txn>,
    table: lmdb::Database,
}

impl<'txn> JoinTable<'txn> {
    fn new(txn: &'txn lmdb::RoTransaction<'txn>, table: lmdb::Database) -> Self {
        Self { txn, table }
    }

    /// Given an element ID, returns the IDs of elements it is related to in this table.
    /// Returns an iterator since there may be multiple values for a given key.
    pub fn get(&self, id: u64) -> impl Iterator<Item = u64> + 'txn {
        let cursor = self.txn.open_ro_cursor(self.table).unwrap();

        Gen::new(|co| async move {
            let mut cursor = cursor;
            match cursor.iter_dup_of(&id.to_le_bytes()) {
                Ok(iter) => {
                    for (_, raw_val) in iter {
                        let val = u64::from_le_bytes(
                            raw_val.try_into().expect("key with incorrect length"),
                        );

                        co.yield_(val).await;
                    }
                }
                Err(lmdb::Error::NotFound) => (),
                Err(e) => unreachable!("Unexpected LMDB error: {:?}", e),
            }
        })
        .into_iter()
    }
}
*/
