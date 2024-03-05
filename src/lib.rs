mod database;
mod types;

mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

pub use database::{Database, Locations, Nodes, Relations, Transaction, Ways};
pub use types::{Location, Node, Region, Relation, RelationMember, Way};
