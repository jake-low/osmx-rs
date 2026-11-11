use std::collections::{BTreeMap, BTreeSet, HashMap};
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

impl Element {
    fn version(&self) -> u32 {
        match self {
            Element::Node(node) => node.version,
            Element::Way(way) => way.version,
            Element::Relation(relation) => relation.version,
        }
    }

    fn timestamp(&self) -> DateTime<Utc> {
        let metadata = match self {
            Element::Node(node) => &node.metadata,
            Element::Way(way) => &way.metadata,
            Element::Relation(relation) => &relation.metadata,
        };
        metadata.as_ref().unwrap().timestamp
    }
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

/// Represents a subset of the changes in the input augmented diff.
#[derive(Default)]
struct DiffBuilder {
    /// For each element ID, the start and end indices in the timeline of
    /// the edits that affected it (either directly or by propagation from
    /// a child element). In the resulting diff, this element's <old> side
    /// will be the state just before the first of these changes and the
    /// <new> side will be the state just after.
    spans: BTreeMap<ElementId, (usize, usize)>,
}

impl DiffBuilder {
    /// Records a change at `pos` affecting `element_id`. Must be called in
    /// timeline order.
    fn touch(&mut self, element_id: ElementId, pos: usize) {
        self.spans
            .entry(element_id)
            .and_modify(|(_, last)| *last = pos)
            .or_insert((pos, pos));
    }

    /// Writes the diff as augmented diff XML. Actions are emitted in order by
    /// their OSM element type and ID (nodes, then ways, then relations).
    fn write<W: Write>(
        &self,
        tables: &Tables,
        history: &History,
        out: &mut W,
    ) -> std::io::Result<()> {
        out.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")?;

        let mut is_empty = true;
        for (element_id, span) in self.spans.iter() {
            let Some(action) = action(tables, history, *element_id, *span) else {
                continue;
            };
            if is_empty {
                out.write_all(b"<osm version=\"0.6\" generator=\"osmx-rs\">\n")?;
                is_empty = false;
            }
            write_action(out, &action)?;
        }

        if is_empty {
            out.write_all(b"<osm version=\"0.6\" generator=\"osmx-rs\"/>\n")
        } else {
            out.write_all(b"</osm>\n")
        }
    }
}

/// Builds the action for one element whose changes span timeline positions
/// `first..=last`, or None if the element produces no action.
fn action(
    tables: &Tables,
    history: &History,
    element_id: ElementId,
    (first, last): (usize, usize),
) -> Option<Action> {
    let before = View {
        tables,
        history,
        cutoff: first,
    };
    let after = View {
        tables,
        history,
        cutoff: last + 1,
    };
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

/// The tables of the .osmx database
struct Tables<'txn> {
    locations: osmx::Locations<'txn>,
    nodes: osmx::Nodes<'txn>,
    ways: osmx::Ways<'txn>,
    relations: osmx::Relations<'txn>,
    node_ways: osmx::JoinTable<'txn>,
    node_relations: osmx::JoinTable<'txn>,
    way_relations: osmx::JoinTable<'txn>,
}

impl<'txn> Tables<'txn> {
    fn new(txn: &'txn osmx::Transaction<'txn>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            locations: txn.locations()?,
            nodes: txn.nodes()?,
            ways: txn.ways()?,
            relations: txn.relations()?,
            node_ways: txn.node_ways()?,
            node_relations: txn.node_relations()?,
            way_relations: txn.way_relations()?,
        })
    }
}

/// Every element version in the .osc file, tagged with its timeline position.
#[derive(Default)]
struct History {
    versions: HashMap<ElementId, Vec<(usize, Element)>>,

    // Memberships that appear in any way or relation version in the .osc file,
    // which the database's join tables don't know about. Whether a membership
    // holds at a given time is checked against that version of the parent when
    // building an action.
    node_ways: HashMap<u64, BTreeSet<u64>>,
    node_relations: HashMap<u64, BTreeSet<u64>>,
    way_relations: HashMap<u64, BTreeSet<u64>>,
}

impl History {
    /// Adds a version. Must be called in timeline order.
    fn push(&mut self, pos: usize, element_id: ElementId, element: Element) {
        match &element {
            Element::Node(_) => {}
            Element::Way(way) => {
                for node_id in &way.nodes {
                    self.node_ways.entry(*node_id).or_default().insert(way.id);
                }
            }
            Element::Relation(relation) => {
                for member in &relation.members {
                    let index = match member.id {
                        ElementId::Node(id) => self.node_relations.entry(id),
                        ElementId::Way(id) => self.way_relations.entry(id),
                        ElementId::Relation(_) => continue,
                    };
                    index.or_default().insert(relation.id);
                }
            }
        }
        self.versions
            .entry(element_id)
            .or_default()
            .push((pos, element));
    }

    /// The latest version of an element before timeline position `cutoff`, or
    /// None if the .osc file has no such version.
    fn get(&self, element_id: ElementId, cutoff: usize) -> Option<&Element> {
        let versions = self.versions.get(&element_id)?;
        let i = versions.partition_point(|(pos, _)| *pos < cutoff);
        i.checked_sub(1).map(|i| &versions[i].1)
    }
}

/// The state of the data at one point in the timeline: the database plus all
/// changes in the .osc file before position `cutoff`.
struct View<'a, 'txn> {
    tables: &'a Tables<'txn>,
    history: &'a History,
    cutoff: usize,
}

impl<'a, 'txn> View<'a, 'txn> {
    fn element(&self, element_id: ElementId) -> Option<&'a Element> {
        self.history.get(element_id, self.cutoff)
    }

    fn get_augmented_element(&self, element_id: ElementId) -> Option<AugmentedElement> {
        if let Some(element) = self.element(element_id) {
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
                    let loc = self.tables.locations.get(id)?;
                    let version = loc.version();
                    let visible = true; // implicit (deleted elements are not kept in the osmx database)
                    let lon: f64 = loc.lon();
                    let lat: f64 = loc.lat();
                    let (metadata, tags) = if let Some(node) = self.tables.nodes.get(id) {
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
                    let way = self.tables.ways.get(id)?;
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
                    let relation = self.tables.relations.get(id)?;
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
        if let Some(Element::Relation(relation)) = self.element(ElementId::Relation(id)) {
            return Some(AugmentedElement::Relation(AugmentedRelation {
                id: relation.id,
                version: relation.version,
                visible: relation.visible,
                metadata: relation.metadata.clone(),
                members: Vec::new(),
                tags: relation.tags.clone(),
            }));
        }
        let relation = self.tables.relations.get(id)?;
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

    /// Ids of the ways that contain a node.
    fn parent_ways(&self, node_id: u64) -> Vec<u64> {
        let in_db: BTreeSet<u64> = self.tables.node_ways.get(node_id).collect();
        let in_osc = self.history.node_ways.get(&node_id);
        let candidates: BTreeSet<u64> = in_db
            .iter()
            .chain(in_osc.into_iter().flatten())
            .copied()
            .collect();
        candidates
            .into_iter()
            .filter(|way_id| match self.element(ElementId::Way(*way_id)) {
                None => in_db.contains(way_id),
                Some(Element::Way(way)) => way.visible && way.nodes.contains(&node_id),
                Some(other) => panic!("expected way, got {:?}", other),
            })
            .collect()
    }

    /// Ids of the relations that have a node or way as a member.
    fn parent_relations(&self, child: ElementId) -> Vec<u64> {
        let (in_db, in_osc): (BTreeSet<u64>, _) = match child {
            ElementId::Node(id) => (
                self.tables.node_relations.get(id).collect(),
                self.history.node_relations.get(&id),
            ),
            ElementId::Way(id) => (
                self.tables.way_relations.get(id).collect(),
                self.history.way_relations.get(&id),
            ),
            ElementId::Relation(_) => panic!("parents of relations are not tracked"),
        };
        let candidates: BTreeSet<u64> = in_db
            .iter()
            .chain(in_osc.into_iter().flatten())
            .copied()
            .collect();
        candidates
            .into_iter()
            .filter(
                |relation_id| match self.element(ElementId::Relation(*relation_id)) {
                    None => in_db.contains(relation_id),
                    Some(Element::Relation(relation)) => {
                        relation.visible && relation.members.iter().any(|m| m.id == child)
                    }
                    Some(other) => panic!("expected relation, got {:?}", other),
                },
            )
            .collect()
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

/// Orders changes by timestamp, keeping file order for ties.
///
/// Replication files are sorted by (type, id, version) rather than by time, so
/// we need to sort them chronologically before construcing diffs from intervals.
fn timeline(changes: Vec<Change>) -> Vec<Change> {
    let mut order: Vec<usize> = (0..changes.len()).collect();
    order.sort_by_key(|&i| changes[i].3.timestamp());

    let mut last_versions: HashMap<ElementId, u32> = HashMap::new();
    for &i in &order {
        let (element_id, _, _, element) = &changes[i];
        let version = element.version();
        if let Some(last) = last_versions.insert(*element_id, version) {
            if version <= last {
                eprintln!(
                    "warning: timestamps of {:?} are not in version order; using file order",
                    element_id
                );
                return changes;
            }
        }
    }

    let mut changes: Vec<Option<Change>> = changes.into_iter().map(Some).collect();
    order.iter().map(|&i| changes[i].take().unwrap()).collect()
}

pub fn run(args: &CliArgs) -> Result<(), Box<dyn Error>> {
    let osc_filename: &str = args.osc_file.as_os_str().to_str().unwrap();

    let changes = timeline(parse_osc(osc_filename)?);
    let num_changes = changes.len();

    let db = osmx::Database::open(&args.osmx_file)?;
    let txn = osmx::Transaction::begin(&db)?;
    let tables = Tables::new(&txn)?;

    let mut history = History::default();
    let mut edits = Vec::with_capacity(num_changes);
    for (pos, (element_id, changeset_id, action_type, element)) in changes.into_iter().enumerate() {
        history.push(pos, element_id, element);
        edits.push((element_id, changeset_id, action_type));
    }

    let mut diff_builders: HashMap<ChangesetId, DiffBuilder> = HashMap::new();

    for (pos, (element_id, changeset_id, action_type)) in edits.into_iter().enumerate() {
        let key = if args.split { changeset_id } else { 0 };
        let diff_builder = diff_builders.entry(key).or_default();

        diff_builder.touch(element_id, pos);

        if action_type == ActionType::Modify {
            // Parents are looked up as of just before this change, to account
            // for membership changes from earlier in the .osc file
            let view = View {
                tables: &tables,
                history: &history,
                cutoff: pos,
            };
            match element_id {
                ElementId::Node(node_id) => {
                    // Node moved (or had tags changed); propagate to parent ways and
                    // relations, plus the parent relations of those parent ways.
                    for way_id in view.parent_ways(node_id) {
                        diff_builder.touch(ElementId::Way(way_id), pos);
                        for relation_id in view.parent_relations(ElementId::Way(way_id)) {
                            diff_builder.touch(ElementId::Relation(relation_id), pos);
                        }
                    }

                    for relation_id in view.parent_relations(element_id) {
                        diff_builder.touch(ElementId::Relation(relation_id), pos);
                    }
                }
                ElementId::Way(_) => {
                    // Way's node list or tags changed; propagate to parent relations.
                    for relation_id in view.parent_relations(element_id) {
                        diff_builder.touch(ElementId::Relation(relation_id), pos);
                    }
                }
                ElementId::Relation(_) => {}
            }
        }
    }

    if args.split {
        std::fs::create_dir_all(&args.output_path)?;

        for (id, diff_builder) in diff_builders.iter() {
            let output_filename = args.output_path.join(format!("{}.adiff", id));
            write_adiff_file(&output_filename, diff_builder, &tables, &history)?;
        }
    } else {
        let mut diff_builder = diff_builders.remove(&0).unwrap_or_default();
        for span in diff_builder.spans.values_mut() {
            *span = (0, num_changes - 1);
        }
        write_adiff_file(&args.output_path, &diff_builder, &tables, &history)?;
    }

    Ok(())
}

fn write_adiff_file(
    path: &Path,
    diff_builder: &DiffBuilder,
    tables: &Tables,
    history: &History,
) -> std::io::Result<()> {
    let mut out = BufWriter::new(std::fs::File::create(path)?);
    diff_builder.write(tables, history, &mut out)?;
    out.flush()
}

// The adiff is written straight to the output as text instead of building an
// XML document in memory first.

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

/// Writes ` name="value"`, escaping the value correctly.
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
