// This crate does nothing right now; the only code is in the examples directory.
// I ported the examples first to get a feel for what abstractions this crate
// should provide; now that they are working I'm going to start refactoring
// repetitive code out of those programs and into here.

use std::cell::OnceCell;

use capnp::{
    message::{ReaderOptions, TypedReader},
    serialize::BufferSegments,
};

use itertools::Itertools;

use crate::messages_capnp;

const COORDINATE_PRECISION: i32 = 10000000;

pub struct Region {
    pub(crate) cells: s2::cellunion::CellUnion,
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

pub struct Node<'a> {
    reader: TypedReader<BufferSegments<&'a [u8]>, messages_capnp::node::Owned>,
}

impl<'a> Node<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        let options = ReaderOptions::new();
        let segments = BufferSegments::new(bytes, options).unwrap();

        Self {
            reader: capnp::message::Reader::new(segments, options).into_typed(),
        }
    }

    pub fn tag(&'a self, key: &str) -> Option<&'a str> {
        self.tags().find(|(k, _)| k == &key).map(|(_, v)| v)
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

    pub fn is_closed(&self) -> bool {
        // TODO: haven't considered if this is correct when way contains zero or one nodes
        let mut nodes = self.nodes();
        let first = nodes.next();
        let last = nodes.last();
        first == last
    }
}

pub struct Relation<'a> {
    reader: TypedReader<BufferSegments<&'a [u8]>, messages_capnp::relation::Owned>,
}

impl<'a> Relation<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        let options = ReaderOptions::new();
        let segments = BufferSegments::new(bytes, options).unwrap();

        Self {
            reader: capnp::message::Reader::new(segments, options).into_typed(),
        }
    }

    pub fn tag(&'a self, key: &str) -> Option<&'a str> {
        self.tags().find(|(k, _)| k == &key).map(|(_, v)| v)
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

    pub fn members(&'a self) -> impl Iterator<Item = RelationMember<'a>> {
        self.reader
            .get()
            .unwrap()
            .get_members()
            .unwrap()
            .iter()
            .map(|v| RelationMember { reader: v })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ElementId {
    Node(u64),
    Way(u64),
    Relation(u64),
}

pub struct RelationMember<'a> {
    reader: messages_capnp::relation_member::Reader<'a>,
}

impl<'a> RelationMember<'a> {
    pub fn id(&'a self) -> ElementId {
        use messages_capnp::relation_member::Type;
        let id_ref = self.reader.get_ref();

        match self.reader.get_type().unwrap() {
            Type::Node => ElementId::Node(id_ref),
            Type::Way => ElementId::Way(id_ref),
            Type::Relation => ElementId::Relation(id_ref),
        }
    }

    pub fn role(&'a self) -> &'a str {
        self.reader.get_role().unwrap().to_str().unwrap()
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
