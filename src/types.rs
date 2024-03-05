use crate::messages_capnp;
use capnp::message::{ReaderOptions, TypedReader};
use capnp::serialize::BufferSegments;
use itertools::Itertools;

#[derive(Debug, PartialEq, Eq)]
pub enum ElementId {
    Node(u64),
    Way(u64),
    Relation(u64),
}

/// A reader for values in the `locations` table, which store the coordinates of OSM Nodes.
pub struct Location<'a> {
    buf: &'a [u8],
}

const COORDINATE_PRECISION: i32 = 10000000;

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

/// A reader for a value in the `nodes` table, which stores the tags and metadata for OSM Nodes.
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

/// A reader for an OSM Way stored in the `ways` table, including its tags, metadata, and list of constituent Nodes.
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

/// A reader for an OSM Relation in the `relations` table, including its tags, metadata, and list of members.
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

/// A reader for a member reference of an OSM Relation. Created by calling [Relation::members]
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

pub struct Region {
    pub(crate) cells: s2::cellunion::CellUnion,
}

lazy_static! {
    static ref COVERER: s2::region::RegionCoverer = {
        s2::region::RegionCoverer {
            min_level: 4,
            max_level: 16,
            level_mod: 1,
            max_cells: 8,
        }
    };
}

impl Region {
    pub fn from_bbox(west: f64, south: f64, east: f64, north: f64) -> Self {
        let rect = s2::rect::Rect::from_degrees(south, west, north, east);
        let cells = COVERER.covering(&rect);
        Self { cells }
    }
}

// pub struct Tag<'a>(&'a str, &'a str);
