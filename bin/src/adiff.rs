use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use libxml::parser::Parser;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
enum ElementId {
    Node(u64),
    Way(u64),
    Relation(u64),
}

type ChangesetId = u64;

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
enum ActionType {
    Create,
    Modify,
    Delete,
}

#[derive(Clone, Debug)]
struct Node {
    id: u64,
    version: u32,
    visible: bool,
    metadata: Option<Metadata>,
    lon: f64,
    lat: f64,
    tags: Tags,
}

#[derive(Clone, Debug)]
struct Way {
    id: u64,
    version: u32,
    visible: bool,
    metadata: Option<Metadata>,
    nodes: Vec<u64>,
    tags: Tags,
}

#[derive(Clone, Debug)]
struct Relation {
    id: u64,
    version: u32,
    visible: bool,
    metadata: Option<Metadata>,
    members: Vec<Member>,
    tags: Tags,
}

#[derive(Clone, Debug)]
struct Member {
    id: ElementId,
    role: String,
}

#[derive(Clone, Debug)]
struct Metadata {
    changeset: u64,
    timestamp: DateTime<Utc>,
    uid: u32,
    user: String,
}

type Tags = BTreeMap<String, String>;

#[derive(Clone, Debug)]
enum Element {
    Node(Node),
    Way(Way),
    Relation(Relation),
}

#[derive(Clone, Debug)]
struct AugmentedWay {
    id: u64,
    version: u32,
    visible: bool,
    metadata: Option<Metadata>,
    nodes: Vec<WayNode>,
    tags: Tags,
}

#[derive(Clone, Debug)]
struct AugmentedRelation {
    id: u64,
    version: u32,
    visible: bool,
    metadata: Option<Metadata>,
    members: Vec<AugmentedMember>,
    tags: Tags,
}

#[derive(Clone, Debug)]
enum WayNode {
    Resolved(Node),
    /// A way node reference that could not be found in the database;
    /// uncommon but can happen if the database is referentially incomplete.
    Dangling {
        id: u64,
    },
}

#[derive(Clone, Debug)]
enum AugmentedMember {
    Resolved {
        id: ElementId,
        role: String,
        element: Box<AugmentedElement>,
    },
    /// A relation member whose corresponding element could not be found
    /// in the database.
    Dangling { id: ElementId, role: String },
}

impl AugmentedMember {
    fn id(&self) -> ElementId {
        match self {
            AugmentedMember::Resolved { id, .. } => *id,
            AugmentedMember::Dangling { id, .. } => *id,
        }
    }

    fn role(&self) -> &str {
        match self {
            AugmentedMember::Resolved { role, .. } => role,
            AugmentedMember::Dangling { role, .. } => role,
        }
    }
}

#[derive(Clone, Debug)]
enum AugmentedElement {
    Node(Node),
    Way(AugmentedWay),
    Relation(AugmentedRelation),
}

impl AugmentedElement {
    fn visible(&self) -> bool {
        match self {
            AugmentedElement::Node(node) => node.visible,
            AugmentedElement::Way(way) => way.visible,
            AugmentedElement::Relation(relation) => relation.visible,
        }
    }
}

enum Action {
    Create(AugmentedElement),
    Modify(AugmentedElement, AugmentedElement),
    Delete(AugmentedElement, AugmentedElement),
}

struct DiffBuilder<'txn> {
    elements: BTreeSet<ElementId>,
    before: Option<Snapshot<'txn>>,
    after: Option<Snapshot<'txn>>,
}

impl<'txn> DiffBuilder<'txn> {
    /// Builds the action for one element, or None if the element produces no
    /// action in this diff.
    fn action(&self, after: &Snapshot<'txn>, element_id: ElementId) -> Option<Action> {
        let before = self.before.as_ref().unwrap();
        let old = before.get_augmented_element(element_id);
        let new = after.get_augmented_element(element_id);

        match (old, new) {
            (Some(old), Some(new)) => {
                if new.visible() {
                    Some(Action::Modify(old, new))
                } else {
                    Some(Action::Delete(old, new))
                }
            }
            (Some(_old), None) => {
                // valid actions should always have a 'new' side (even deletes; the new
                // version has visible=false and has the changeset, user, timestamp etc.
                // that deleted the element)
                panic!("invalid delete (no new side) for {:?}", element_id);
            }
            (None, Some(new)) => {
                if !new.visible() {
                    // Element was created and then deleted within this diff window,
                    // so we don't emit any action
                    return None;
                }
                Some(Action::Create(new))
            }
            (None, None) => None,
        }
    }

    /// Writes the diff as augmented diff XML. Each action is built, written
    /// and dropped before the next one, because a diff that touches many large
    /// relations does not fit in memory as a whole.
    ///
    /// Actions come out sorted by element type (nodes, ways, relations) and then
    /// by id, which is the iteration order of `BTreeSet<ElementId>`.
    fn write<W: Write>(&self, out: &mut W) -> std::io::Result<()> {
        out.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")?;

        // The root's open tag is written with the first action, so that a diff
        // with no actions gets a self-closing root. `after` is None when the
        // .osc file was empty.
        let mut is_empty = true;
        if let Some(after) = self.after.as_ref() {
            for element_id in self.elements.iter() {
                let Some(action) = self.action(after, *element_id) else {
                    continue;
                };
                if is_empty {
                    out.write_all(b"<osm version=\"0.6\" generator=\"osmx-rs\">\n")?;
                    is_empty = false;
                }
                write_action(out, &action)?;
            }
        }

        if is_empty {
            out.write_all(b"<osm version=\"0.6\" generator=\"osmx-rs\"/>\n")
        } else {
            out.write_all(b"</osm>\n")
        }
    }
}

struct Snapshot<'txn> {
    txn: &'txn osmx::Transaction<'txn>,
    locations: osmx::Locations<'txn>,
    nodes: osmx::Nodes<'txn>,
    ways: osmx::Ways<'txn>,
    relations: osmx::Relations<'txn>,
    node_ways: osmx::JoinTable<'txn>,
    node_relations: osmx::JoinTable<'txn>,
    way_relations: osmx::JoinTable<'txn>,

    elements: HashMap<ElementId, Element>,
}

impl<'txn> Snapshot<'txn> {
    fn new(txn: &'txn osmx::Transaction<'txn>) -> Result<Self, Box<dyn Error>> {
        let locations = txn.locations()?;
        let nodes = txn.nodes()?;
        let ways = txn.ways()?;
        let relations = txn.relations()?;
        let node_ways = txn.node_ways()?;
        let node_relations = txn.node_relations()?;
        let way_relations = txn.way_relations()?;

        Ok(Self {
            txn,
            locations,
            nodes,
            ways,
            relations,
            node_ways,
            node_relations,
            way_relations,
            elements: HashMap::new(),
        })
    }

    fn get_augmented_element(&self, element_id: ElementId) -> Option<AugmentedElement> {
        if let Some(element) = self.elements.get(&element_id) {
            let augmented_element = match element {
                Element::Node(node) => AugmentedElement::Node(node.clone()),
                Element::Way(way) => {
                    let way = way.clone();
                    let id = way.id;
                    let version = way.version;
                    let visible = way.visible;
                    let metadata = way.metadata;
                    let nodes = way
                        .nodes
                        .iter()
                        .map(|node_id| self.resolve_way_node(id, *node_id))
                        .collect();
                    let tags = way.tags;
                    AugmentedElement::Way(AugmentedWay {
                        id,
                        version,
                        visible,
                        metadata,
                        nodes,
                        tags,
                    })
                }
                Element::Relation(relation) => {
                    let relation = relation.clone();
                    let id = relation.id;
                    let version = relation.version;
                    let visible = relation.visible;
                    let metadata = relation.metadata;
                    let members = relation
                        .members
                        .iter()
                        .map(|member| self.resolve_member(id, member.id, member.role.clone()))
                        .collect();
                    let tags = relation.tags;
                    AugmentedElement::Relation(AugmentedRelation {
                        id,
                        version,
                        visible,
                        metadata,
                        members,
                        tags,
                    })
                }
            };
            Some(augmented_element)
        } else {
            match element_id {
                ElementId::Node(id) => {
                    let loc = self.locations.get(id)?;
                    let version = loc.version();
                    let visible = true; // implicit (deleted elements are not kept in the osmx database)
                    let lon: f64 = loc.lon();
                    let lat: f64 = loc.lat();
                    let (metadata, tags) = if let Some(node) = self.nodes.get(id) {
                        let metadata = Some(Metadata {
                            changeset: node.metadata().changeset() as u64,
                            timestamp: Utc
                                .timestamp_opt(node.metadata().timestamp() as i64, 0)
                                .single()
                                .expect("invalid timestamp"),
                            uid: node.metadata().uid(),
                            user: node.metadata().user().to_string(),
                        });
                        let tags = node
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        (metadata, tags)
                    } else {
                        (None, BTreeMap::new())
                    };
                    Some(AugmentedElement::Node(Node {
                        id,
                        version,
                        visible,
                        metadata,
                        lon,
                        lat,
                        tags,
                    }))
                }
                ElementId::Way(id) => {
                    let way = self.ways.get(id)?;
                    let version = way.metadata().version();
                    let visible = true; // implicit (deleted elements are not kept in the osmx database)
                    let nodes = way
                        .nodes()
                        .map(|node_id| self.resolve_way_node(id, node_id))
                        .collect();
                    let tags = way
                        .tags()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    let metadata = Some(Metadata {
                        changeset: way.metadata().changeset() as u64,
                        timestamp: Utc
                            .timestamp_opt(way.metadata().timestamp() as i64, 0)
                            .single()
                            .expect("invalid timestamp"),
                        uid: way.metadata().uid(),
                        user: way.metadata().user().to_string(),
                    });
                    Some(AugmentedElement::Way(AugmentedWay {
                        id,
                        version,
                        visible,
                        metadata,
                        nodes,
                        tags,
                    }))
                }
                ElementId::Relation(id) => {
                    let relation = self.relations.get(id)?;
                    let version = relation.metadata().version();
                    let visible = true; // implicit (deleted elements are not kept in the osmx database)
                    let members = relation
                        .members()
                        .map(|m| {
                            let mid = match m.id() {
                                osmx::ElementId::Node(id) => ElementId::Node(id),
                                osmx::ElementId::Way(id) => ElementId::Way(id),
                                osmx::ElementId::Relation(id) => ElementId::Relation(id),
                            };
                            self.resolve_member(id, mid, m.role().to_string())
                        })
                        .collect();
                    let tags = relation
                        .tags()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    let metadata = Some(Metadata {
                        changeset: relation.metadata().changeset() as u64,
                        timestamp: Utc
                            .timestamp_opt(relation.metadata().timestamp() as i64, 0)
                            .single()
                            .expect("invalid timestamp"),
                        uid: relation.metadata().uid(),
                        user: relation.metadata().user().to_string(),
                    });
                    Some(AugmentedElement::Relation(AugmentedRelation {
                        id,
                        version,
                        visible,
                        metadata,
                        members,
                        tags,
                    }))
                }
            }
        }
    }

    fn set_element(&mut self, element_id: ElementId, element: Element) {
        self.elements.insert(element_id, element);
    }

    fn resolve_way_node(&self, parent_way_id: u64, node_id: u64) -> WayNode {
        match self.get_augmented_element(ElementId::Node(node_id)) {
            Some(AugmentedElement::Node(node)) => WayNode::Resolved(node),
            Some(other) => panic!("expected node for way ref, got {:?}", other),
            None => {
                eprintln!(
                    "warning: way {} references node {} which is not in the database",
                    parent_way_id, node_id
                );
                WayNode::Dangling { id: node_id }
            }
        }
    }

    fn resolve_member(
        &self,
        parent_relation_id: u64,
        member_id: ElementId,
        role: String,
    ) -> AugmentedMember {
        // For sub-relation members, don't recursively augment: relation graphs
        // in OSM can be deep or (in rare cases) cyclic, so recursing could
        // exhaust the stack.
        let element = match member_id {
            ElementId::Relation(rid) => self.get_relation_stub(rid),
            _ => self.get_augmented_element(member_id),
        };
        match element {
            Some(element) => AugmentedMember::Resolved {
                id: member_id,
                role,
                element: Box::new(element),
            },
            None => {
                eprintln!(
                    "warning: relation {} references member {:?} which is not in the database",
                    parent_relation_id, member_id
                );
                AugmentedMember::Dangling {
                    id: member_id,
                    role,
                }
            }
        }
    }

    /// Build an AugmentedElement::Relation without resolving its members.
    fn get_relation_stub(&self, id: u64) -> Option<AugmentedElement> {
        if let Some(Element::Relation(relation)) = self.elements.get(&ElementId::Relation(id)) {
            return Some(AugmentedElement::Relation(AugmentedRelation {
                id: relation.id,
                version: relation.version,
                visible: relation.visible,
                metadata: relation.metadata.clone(),
                members: Vec::new(),
                tags: relation.tags.clone(),
            }));
        }
        let relation = self.relations.get(id)?;
        let metadata = Some(Metadata {
            changeset: relation.metadata().changeset() as u64,
            timestamp: Utc
                .timestamp_opt(relation.metadata().timestamp() as i64, 0)
                .single()
                .expect("invalid timestamp"),
            uid: relation.metadata().uid(),
            user: relation.metadata().user().to_string(),
        });
        let tags = relation
            .tags()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Some(AugmentedElement::Relation(AugmentedRelation {
            id,
            version: relation.metadata().version(),
            visible: true,
            metadata,
            members: Vec::new(),
            tags,
        }))
    }
}

impl<'txn> Clone for Snapshot<'txn> {
    fn clone(&self) -> Self {
        Self {
            txn: self.txn,
            locations: self.txn.locations().unwrap(),
            nodes: self.txn.nodes().unwrap(),
            ways: self.txn.ways().unwrap(),
            relations: self.txn.relations().unwrap(),
            node_ways: self.txn.node_ways().unwrap(),
            node_relations: self.txn.node_relations().unwrap(),
            way_relations: self.txn.way_relations().unwrap(),
            elements: self.elements.clone(),
        }
    }
}

#[derive(clap::Parser)]
/// Generate an Augmented Diff file from an OsmChange (.osc) file.
pub struct CliArgs {
    /// Path to the osmx database file
    osmx_file: PathBuf,

    /// Path to the osc file to read
    osc_file: PathBuf,

    /// Path to write the augmented diff to. If --split is used, this should be a directory.
    output_path: PathBuf,

    /// Output one augmented diff per changeset, instead of a single augmented diff
    #[arg(long)]
    split: bool,
}

type Change = (ElementId, ChangesetId, ActionType, Element);

fn parse_osc(osc_filename: &str) -> Result<Vec<Change>, Box<dyn Error>> {
    let parse_options = libxml::parser::ParserOptions {
        no_blanks: true,
        ..Default::default()
    };
    let osc = Parser::default().parse_file_with_options(osc_filename, parse_options)?;

    let mut changes: Vec<Change> = Vec::new();
    for block in osc.get_root_element().unwrap().get_child_nodes() {
        if !block.is_element_node() {
            continue;
        }

        let action_type = match &block.get_name()[..] {
            "create" => ActionType::Create,
            "modify" => ActionType::Modify,
            "delete" => ActionType::Delete,
            other => panic!("unknown osc block: {}", other),
        };

        for element in block.get_child_nodes() {
            if !element.is_element_node() {
                continue;
            }

            let id: u64 = element
                .get_attribute("id")
                .expect("element has no id")
                .parse()
                .expect("failed to parse element id as u64");

            let changeset: u64 = element
                .get_attribute("changeset")
                .expect("element has no changeset")
                .parse()
                .expect("failed to parse changeset as u64");

            let element_id = match &element.get_name()[..] {
                "node" => ElementId::Node(id),
                "way" => ElementId::Way(id),
                "relation" => ElementId::Relation(id),
                other => panic!("unknown element type: {}", other),
            };

            let version: u32 = element.get_attribute("version").unwrap().parse().unwrap();
            // OSM replication files don't include a `visible` attribute; deleted
            // elements appear inside a <delete> block instead. This program tracks
            // visibility in state snapshots and emits a `visible` attribute into
            // output adiffs; when reading the input OSC file we derive visibility
            // from the action type.
            let visible = action_type != ActionType::Delete;

            let metadata = Some(Metadata {
                changeset,
                timestamp: DateTime::parse_from_rfc3339(
                    &element.get_attribute("timestamp").unwrap(),
                )
                .expect("failed to parse timestamp")
                .with_timezone(&Utc),
                uid: element.get_attribute("uid").unwrap().parse().unwrap(),
                user: element.get_attribute("user").unwrap().to_string(),
            });

            let tags: Tags = element
                .get_child_nodes()
                .iter()
                .filter(|child| child.get_name() == "tag")
                .map(|tag| {
                    (
                        tag.get_attribute("k").unwrap().to_string(),
                        tag.get_attribute("v").unwrap().to_string(),
                    )
                })
                .collect();

            let element = match element_id {
                ElementId::Node(id) => {
                    let lon: f64 = element.get_attribute("lon").unwrap().parse().unwrap();
                    let lat: f64 = element.get_attribute("lat").unwrap().parse().unwrap();
                    Element::Node(Node {
                        id,
                        version,
                        visible,
                        metadata,
                        lon,
                        lat,
                        tags,
                    })
                }
                ElementId::Way(id) => {
                    let nodes: Vec<u64> = element
                        .get_child_nodes()
                        .iter()
                        .filter(|child| child.get_name() == "nd")
                        .map(|nd| nd.get_attribute("ref").unwrap().parse().unwrap())
                        .collect();
                    Element::Way(Way {
                        id,
                        version,
                        visible,
                        metadata,
                        nodes,
                        tags,
                    })
                }
                ElementId::Relation(id) => {
                    let members: Vec<Member> = element
                        .get_child_nodes()
                        .iter()
                        .filter(|child| child.get_name() == "member")
                        .map(|member| {
                            let refnum: u64 = member.get_attribute("ref").unwrap().parse().unwrap();
                            let role = member.get_attribute("role").unwrap().to_string();

                            let mtype = member
                                .get_attribute("type")
                                .expect("member has no type attribute");
                            let id = match &mtype[..] {
                                "node" => ElementId::Node(refnum),
                                "way" => ElementId::Way(refnum),
                                "relation" => ElementId::Relation(refnum),
                                other => panic!("unknown member type: {}", other),
                            };

                            Member { id, role }
                        })
                        .collect();
                    Element::Relation(Relation {
                        id,
                        version,
                        visible,
                        metadata,
                        members,
                        tags,
                    })
                }
            };

            changes.push((element_id, changeset, action_type, element));
        }
    }

    Ok(changes)
}

pub fn run(args: &CliArgs) -> Result<(), Box<dyn Error>> {
    let osc_filename: &str = args.osc_file.as_os_str().to_str().unwrap();

    let changes = parse_osc(osc_filename)?;

    let db = osmx::Database::open(&args.osmx_file)?;
    let txn = osmx::Transaction::begin(&db)?;
    let mut snapshot = Snapshot::new(&txn)?;

    let all_changeset_ids: HashSet<ChangesetId> =
        changes.iter().map(|(_, cid, _, _)| *cid).collect();
    let last_indices: HashMap<ChangesetId, usize> = all_changeset_ids
        .iter()
        .map(|cid| {
            (
                *cid,
                changes
                    .iter()
                    .rposition(|(_, cid2, _, _)| *cid2 == *cid)
                    .unwrap(),
            )
        })
        .collect();

    let mut diff_builders: HashMap<ChangesetId, DiffBuilder> = HashMap::new();

    if !args.split {
        diff_builders.insert(
            0,
            DiffBuilder {
                elements: BTreeSet::new(),
                before: Some(snapshot.clone()),
                after: None,
            },
        );
    }

    for (idx, (element_id, changeset_id, action_type, element)) in changes.into_iter().enumerate() {
        let diff_builder = if args.split {
            diff_builders
                .entry(changeset_id)
                .or_insert_with(|| DiffBuilder {
                    elements: BTreeSet::new(),
                    before: Some(snapshot.clone()),
                    after: None,
                })
        } else {
            diff_builders.get_mut(&0).unwrap()
        };

        diff_builder.elements.insert(element_id);

        if action_type == ActionType::Modify {
            match element_id {
                ElementId::Node(node_id) => {
                    // Node moved (or had tags changed); propagate to parent ways and
                    // relations, plus the parent relations of those parent ways.
                    for way_id in snapshot.node_ways.get(node_id) {
                        diff_builder.elements.insert(ElementId::Way(way_id));
                        for relation_id in snapshot.way_relations.get(way_id) {
                            diff_builder
                                .elements
                                .insert(ElementId::Relation(relation_id));
                        }
                    }

                    for relation_id in snapshot.node_relations.get(node_id) {
                        diff_builder
                            .elements
                            .insert(ElementId::Relation(relation_id));
                    }
                }
                ElementId::Way(way_id) => {
                    // Way's node list or tags changed; propagate to parent relations.
                    for relation_id in snapshot.way_relations.get(way_id) {
                        diff_builder
                            .elements
                            .insert(ElementId::Relation(relation_id));
                    }
                }
                ElementId::Relation(_) => {}
            }
        }

        snapshot.set_element(element_id, element);

        if idx == last_indices[&changeset_id] {
            diff_builder.after = Some(snapshot.clone());
        }
    }

    if args.split {
        // Create the output directory if it doesn't exist
        std::fs::create_dir_all(&args.output_path)?;

        for (id, diff_builder) in diff_builders.iter() {
            let output_filename = args.output_path.join(format!("{}.adiff", id));
            write_adiff_file(&output_filename, diff_builder)?;
        }
    } else {
        write_adiff_file(&args.output_path, diff_builders.get(&0).unwrap())?;
    }

    Ok(())
}

fn write_adiff_file(path: &Path, diff_builder: &DiffBuilder) -> std::io::Result<()> {
    let mut out = BufWriter::new(std::fs::File::create(path)?);
    diff_builder.write(&mut out)?;
    out.flush()
}

// The adiff is written straight to the output as text instead of building an
// XML document in memory first. The layout (two-space indent, self-closing
// empty elements) matches what libxml2 produced before.

fn write_action<W: Write>(out: &mut W, action: &Action) -> std::io::Result<()> {
    match action {
        Action::Create(element) => {
            out.write_all(b"  <action type=\"create\">\n")?;
            write_element(out, element, 4)?;
        }
        Action::Modify(old, new) | Action::Delete(old, new) => {
            let action_type = match action {
                Action::Delete(..) => "delete",
                _ => "modify",
            };
            writeln!(out, "  <action type=\"{action_type}\">")?;
            out.write_all(b"    <old>\n")?;
            write_element(out, old, 6)?;
            out.write_all(b"    </old>\n")?;
            out.write_all(b"    <new>\n")?;
            write_element(out, new, 6)?;
            out.write_all(b"    </new>\n")?;
        }
    }
    out.write_all(b"  </action>\n")
}

fn write_indent<W: Write>(out: &mut W, indent: usize) -> std::io::Result<()> {
    write!(out, "{:indent$}", "")
}

/// Writes ` name="value"`, escaping the value the way libxml2 does.
fn write_attr<W: Write>(out: &mut W, name: &str, value: &str) -> std::io::Result<()> {
    write!(out, " {name}=\"")?;
    let mut rest = value;
    while let Some(i) = rest.find(['<', '>', '&', '"', '\n', '\r', '\t']) {
        out.write_all(rest[..i].as_bytes())?;
        let escaped = match rest.as_bytes()[i] {
            b'<' => "&lt;",
            b'>' => "&gt;",
            b'&' => "&amp;",
            b'"' => "&quot;",
            b'\n' => "&#10;",
            b'\r' => "&#13;",
            _ => "&#9;",
        };
        out.write_all(escaped.as_bytes())?;
        rest = &rest[i + 1..];
    }
    out.write_all(rest.as_bytes())?;
    out.write_all(b"\"")
}

/// Writes the attributes shared by every element's open tag. Nodes also carry
/// `lon` and `lat`, which go between `visible` and the metadata.
fn write_common_attrs<W: Write>(
    out: &mut W,
    id: u64,
    version: u32,
    visible: bool,
    lonlat: Option<(f64, f64)>,
    metadata: &Option<Metadata>,
) -> std::io::Result<()> {
    write!(out, " id=\"{id}\" version=\"{version}\"")?;
    if !visible {
        out.write_all(b" visible=\"false\"")?;
    }
    if let Some((lon, lat)) = lonlat {
        write!(out, " lon=\"{lon:.7}\" lat=\"{lat:.7}\"")?;
    }
    if let Some(metadata) = metadata {
        write!(
            out,
            " changeset=\"{}\" timestamp=\"{}\" uid=\"{}\"",
            metadata.changeset,
            metadata
                .timestamp
                .to_rfc3339_opts(SecondsFormat::Secs, true),
            metadata.uid
        )?;
        write_attr(out, "user", &metadata.user)?;
    }
    Ok(())
}

fn write_tags<W: Write>(out: &mut W, tags: &Tags, indent: usize) -> std::io::Result<()> {
    for (k, v) in tags {
        write_indent(out, indent)?;
        out.write_all(b"<tag")?;
        write_attr(out, "k", k)?;
        write_attr(out, "v", v)?;
        out.write_all(b"/>\n")?;
    }
    Ok(())
}

fn write_nds<W: Write>(out: &mut W, nodes: &[WayNode], indent: usize) -> std::io::Result<()> {
    for n in nodes {
        write_indent(out, indent)?;
        match n {
            WayNode::Resolved(node) => writeln!(
                out,
                "<nd ref=\"{}\" version=\"{}\" lon=\"{:.7}\" lat=\"{:.7}\"/>",
                node.id, node.version, node.lon, node.lat
            )?,
            WayNode::Dangling { id } => writeln!(out, "<nd ref=\"{id}\"/>")?,
        }
    }
    Ok(())
}

fn write_bounds<W: Write>(out: &mut W, b: &Bounds, indent: usize) -> std::io::Result<()> {
    write_indent(out, indent)?;
    writeln!(
        out,
        "<bounds minlat=\"{:.7}\" minlon=\"{:.7}\" maxlat=\"{:.7}\" maxlon=\"{:.7}\"/>",
        b.minlat, b.minlon, b.maxlat, b.maxlon
    )
}

/// Ends an open tag and reports whether a close tag must follow: an element
/// with no children is self-closing.
fn finish_open_tag<W: Write>(out: &mut W, has_children: bool) -> std::io::Result<bool> {
    let end = if has_children { ">\n" } else { "/>\n" };
    out.write_all(end.as_bytes())?;
    Ok(has_children)
}

fn write_close_tag<W: Write>(out: &mut W, name: &str, indent: usize) -> std::io::Result<()> {
    write_indent(out, indent)?;
    writeln!(out, "</{name}>")
}

fn write_element<W: Write>(
    out: &mut W,
    element: &AugmentedElement,
    indent: usize,
) -> std::io::Result<()> {
    let child_indent = indent + 2;
    write_indent(out, indent)?;

    match element {
        AugmentedElement::Node(node) => {
            out.write_all(b"<node")?;
            write_common_attrs(
                out,
                node.id,
                node.version,
                node.visible,
                Some((node.lon, node.lat)),
                &node.metadata,
            )?;
            if finish_open_tag(out, !node.tags.is_empty())? {
                write_tags(out, &node.tags, child_indent)?;
                write_close_tag(out, "node", indent)?;
            }
        }
        AugmentedElement::Way(way) => {
            out.write_all(b"<way")?;
            write_common_attrs(out, way.id, way.version, way.visible, None, &way.metadata)?;
            let bounds = bounds_for(element);
            let has_children = bounds.is_some() || !way.nodes.is_empty() || !way.tags.is_empty();
            if finish_open_tag(out, has_children)? {
                if let Some(b) = &bounds {
                    write_bounds(out, b, child_indent)?;
                }
                write_nds(out, &way.nodes, child_indent)?;
                write_tags(out, &way.tags, child_indent)?;
                write_close_tag(out, "way", indent)?;
            }
        }
        AugmentedElement::Relation(relation) => {
            out.write_all(b"<relation")?;
            write_common_attrs(
                out,
                relation.id,
                relation.version,
                relation.visible,
                None,
                &relation.metadata,
            )?;
            let bounds = bounds_for(element);
            let has_children =
                bounds.is_some() || !relation.members.is_empty() || !relation.tags.is_empty();
            if finish_open_tag(out, has_children)? {
                if let Some(b) = &bounds {
                    write_bounds(out, b, child_indent)?;
                }
                for member in &relation.members {
                    write_member(out, member, child_indent)?;
                }
                write_tags(out, &relation.tags, child_indent)?;
                write_close_tag(out, "relation", indent)?;
            }
        }
    }
    Ok(())
}

fn write_member<W: Write>(
    out: &mut W,
    member: &AugmentedMember,
    indent: usize,
) -> std::io::Result<()> {
    let (type_str, ref_id) = match member.id() {
        ElementId::Node(id) => ("node", id),
        ElementId::Way(id) => ("way", id),
        ElementId::Relation(id) => ("relation", id),
    };
    write_indent(out, indent)?;
    write!(out, "<member type=\"{type_str}\" ref=\"{ref_id}\"")?;
    write_attr(out, "role", member.role())?;

    let mut way_nodes: &[WayNode] = &[];
    if let AugmentedMember::Resolved { element, .. } = member {
        match &**element {
            AugmentedElement::Node(node) => {
                write!(out, " lat=\"{:.7}\" lon=\"{:.7}\"", node.lat, node.lon)?;
            }
            AugmentedElement::Way(way) => way_nodes = way.nodes.as_slice(),
            AugmentedElement::Relation(_) => {
                // TODO: should sub-relation members be recursively augmented?
                // For now, they are not.
            }
        }
    }

    if finish_open_tag(out, !way_nodes.is_empty())? {
        write_nds(out, way_nodes, indent + 2)?;
        write_close_tag(out, "member", indent)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    minlat: f64,
    minlon: f64,
    maxlat: f64,
    maxlon: f64,
}

fn collect_coords(element: &AugmentedElement, out: &mut Vec<(f64, f64)>) {
    match element {
        AugmentedElement::Node(_) => {}
        AugmentedElement::Way(way) => {
            for n in &way.nodes {
                if let WayNode::Resolved(node) = n {
                    out.push((node.lon, node.lat));
                }
            }
        }
        AugmentedElement::Relation(rel) => {
            for m in &rel.members {
                let AugmentedMember::Resolved { element, .. } = m else {
                    continue;
                };
                match &**element {
                    AugmentedElement::Node(node) => {
                        out.push((node.lon, node.lat));
                    }
                    AugmentedElement::Way(way) => {
                        for n in &way.nodes {
                            if let WayNode::Resolved(node) = n {
                                out.push((node.lon, node.lat));
                            }
                        }
                    }
                    AugmentedElement::Relation(_) => {
                        // TODO: should we recurse into sub-relations and
                        // include their members in the bbox calculation?
                    }
                }
            }
        }
    }
}

fn bounds_for(element: &AugmentedElement) -> Option<Bounds> {
    let mut coords = Vec::new();
    collect_coords(element, &mut coords);
    if coords.is_empty() {
        return None;
    }
    let mut minlon = f64::INFINITY;
    let mut minlat = f64::INFINITY;
    let mut maxlon = f64::NEG_INFINITY;
    let mut maxlat = f64::NEG_INFINITY;
    for (lon, lat) in coords {
        if lon < minlon {
            minlon = lon;
        }
        if lon > maxlon {
            maxlon = lon;
        }
        if lat < minlat {
            minlat = lat;
        }
        if lat > maxlat {
            maxlat = lat;
        }
    }
    Some(Bounds {
        minlon,
        minlat,
        maxlon,
        maxlat,
    })
}
