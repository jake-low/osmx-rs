// use std::cmp::Reverse;
// use std::collections::{BinaryHeap, HashSet};
// use std::error::Error;
// use std::fs::File;
// use std::io::{BufReader, BufWriter, Write};
// use std::marker::PhantomData;
// use std::path::PathBuf;
// use std::sync::mpsc;
// use std::thread;

// use clap::Parser;
// use genawaiter::rc::Gen;
// use indicatif::{ProgressBar, ProgressStyle};
// use lmdb::Transaction;
// use serde::de::DeserializeOwned;
// use serde::{Deserialize, Serialize};

pub enum ElementType {
    Node,
    Way,
    Relation,
}

pub struct LocationBuilder {
    pub longitude: f64,
    pub latitude: f64,
    pub version: u32,
}

impl LocationBuilder {
    pub fn build(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(((self.longitude / 1e7).round() as i32).to_le_bytes());
        buf.extend(((self.latitude / 1e7).round() as i32).to_le_bytes());
        buf.extend(self.version.to_le_bytes());
        buf
    }
}

pub struct NodeBuilder {
    builder: capnp::message::TypedBuilder<osmx::messages_capnp::node::Owned>,
}

impl NodeBuilder {
    pub fn new() -> Self {
        Self {
            builder: capnp::message::TypedBuilder::<osmx::messages_capnp::node::Owned>::new_default(
            ),
        }
    }

    pub fn set_tags(&mut self, tags: &[&str]) -> &Self {
        let mut root = self.builder.init_root();
        root.set_tags(tags).unwrap();
        self
    }

    pub fn build(&self) -> Vec<u8> {
        let mut buf = vec![];
        capnp::serialize::write_message(&mut buf, self.builder.borrow_inner()).unwrap();
        buf
    }
}

pub struct WayBuilder {
    builder: capnp::message::TypedBuilder<osmx::messages_capnp::way::Owned>,
}

impl WayBuilder {
    pub fn new() -> Self {
        Self {
            builder: capnp::message::TypedBuilder::<osmx::messages_capnp::way::Owned>::new_default(
            ),
        }
    }

    pub fn set_tags(&mut self, tags: &[&str]) -> &Self {
        let mut root = self.builder.init_root();
        root.set_tags(tags).unwrap();
        self
    }

    pub fn set_nodes(&mut self, nodes: &[u64]) -> &Self {
        let mut root = self.builder.init_root();
        root.set_nodes(nodes).unwrap();
        self
    }

    pub fn build(&self) -> Vec<u8> {
        let mut buf = vec![];
        capnp::serialize::write_message(&mut buf, self.builder.borrow_inner()).unwrap();
        buf
    }
}

pub struct RelationBuilder {
    builder: capnp::message::TypedBuilder<osmx::messages_capnp::relation::Owned>,
}

impl RelationBuilder {
    pub fn new() -> Self {
        Self {
            builder:
                capnp::message::TypedBuilder::<osmx::messages_capnp::relation::Owned>::new_default(),
        }
    }

    pub fn set_tags(&mut self, tags: &[&str]) -> &Self {
        let mut root = self.builder.init_root();
        root.set_tags(tags).unwrap();
        self
    }

    pub fn set_members(&mut self, members: &[(ElementType, u64, String)]) -> &Self {
        let mut builder = self
            .builder
            .get_root()
            .unwrap()
            .init_members(members.len() as u32);

        for idx in 0..members.len() {
            let member = &members[idx];
            let mut mbuilder = builder.reborrow().get(idx as u32);

            let t = match member.0 {
                ElementType::Node => osmx::messages_capnp::relation_member::Type::Node,
                ElementType::Way => osmx::messages_capnp::relation_member::Type::Way,
                ElementType::Relation => osmx::messages_capnp::relation_member::Type::Relation,
            };

            mbuilder.set_type(t);
            mbuilder.set_ref(member.1);
            mbuilder.set_role(member.2.as_str());
        }

        self
    }

    pub fn build(&self) -> Vec<u8> {
        let mut buf = vec![];
        capnp::serialize::write_message(&mut buf, self.builder.borrow_inner()).unwrap();
        buf
    }
}
