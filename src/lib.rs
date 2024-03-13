#[macro_use]
extern crate lazy_static;

mod database;
mod types;

pub mod messages_capnp {
    // TODO should not be pub
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

pub use database::{Database, Locations, Nodes, Relations, Transaction, Ways, CELL_INDEX_LEVEL};
pub use types::{Location, Node, Region, Relation, RelationMember, Way};
