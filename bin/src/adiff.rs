use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::path::PathBuf;

use anyhow::anyhow;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use libxml::parser::Parser;
use libxml::tree::SaveOptions;
use libxml::tree::{Document, Node as XmlNode};

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
    fn id(&self) -> ElementId {
        match self {
            AugmentedElement::Node(node) => ElementId::Node(node.id),
            AugmentedElement::Way(way) => ElementId::Way(way.id),
            AugmentedElement::Relation(relation) => ElementId::Relation(relation.id),
        }
    }

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

impl Action {
    fn primary(&self) -> &AugmentedElement {
        match self {
            Action::Create(e) => e,
            Action::Modify(_, new) => new,
            Action::Delete(_, new) => new,
        }
    }
}

struct Diff {
    actions: Vec<Action>,
}

struct DiffBuilder<'txn> {
    elements: BTreeSet<ElementId>,
    before: Option<Snapshot<'txn>>,
    after: Option<Snapshot<'txn>>,
}

impl<'txn> DiffBuilder<'txn> {
    fn build(&self) -> Diff {
        let mut actions: Vec<Action> = Vec::new();
        let before = self.before.as_ref().unwrap();
        let Some(after) = self.after.as_ref() else {
            // if after is None, it means the .osc file was empty, so
            // we return an empty diff
            return Diff { actions };
        };

        for element_id in self.elements.iter() {
            let old = before.get_augmented_element(*element_id);
            let new = after.get_augmented_element(*element_id);

            let action = match (old, new) {
                (Some(old), Some(new)) => {
                    if new.visible() {
                        Action::Modify(old, new)
                    } else {
                        Action::Delete(old, new)
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
                        continue;
                    }
                    Action::Create(new)
                }
                (None, None) => {
                    continue;
                }
            };

            actions.push(action);
        }

        sort_actions(&mut actions);
        Diff { actions }
    }
}

fn sort_actions(actions: &mut [Action]) {
    actions.sort_by_key(|a| {
        let id = a.primary().id();
        let (kind, n) = match id {
            ElementId::Node(n) => (0u8, n),
            ElementId::Way(n) => (1u8, n),
            ElementId::Relation(n) => (2u8, n),
        };
        (kind, n)
    });
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
        match self.get_augmented_element(member_id) {
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
            let mut output_file = std::fs::File::create(output_filename)?;
            let diff = diff_builder.build();
            write!(&mut output_file, "{}", adiff_to_string(&diff)?)?;
        }
    } else {
        let mut output_file = std::fs::File::create(&args.output_path)?;
        let diff = diff_builders.get(&0).unwrap().build();
        write!(&mut output_file, "{}", adiff_to_string(&diff)?)?;
    }

    Ok(())
}

fn new_xml_node(name: &str, doc: &Document) -> anyhow::Result<XmlNode> {
    XmlNode::new(name, None, doc).map_err(|()| anyhow!("failed to create XML node `{name}`"))
}

trait XmlNodeExt {
    fn set_attr(&mut self, name: &str, value: &str) -> anyhow::Result<()>;
    fn set_text(&mut self, content: &str) -> anyhow::Result<()>;
    fn append(&mut self, child: &mut XmlNode) -> anyhow::Result<()>;
}

impl XmlNodeExt for XmlNode {
    fn set_attr(&mut self, name: &str, value: &str) -> anyhow::Result<()> {
        self.set_attribute(name, value).map_err(anyhow::Error::msg)
    }
    fn set_text(&mut self, content: &str) -> anyhow::Result<()> {
        self.set_content(content).map_err(anyhow::Error::msg)
    }
    fn append(&mut self, child: &mut XmlNode) -> anyhow::Result<()> {
        self.add_child(child).map_err(anyhow::Error::msg)
    }
}

fn add_tags<'a>(
    doc: &Document,
    element: &mut XmlNode,
    tags: impl Iterator<Item = (&'a String, &'a String)>,
) -> anyhow::Result<()> {
    for kv in tags {
        add_tag(doc, element, kv)?;
    }
    Ok(())
}

fn add_tag(doc: &Document, element: &mut XmlNode, kv: (&String, &String)) -> anyhow::Result<()> {
    let mut tag = new_xml_node("tag", doc)?;
    tag.set_attr("k", kv.0)?;
    tag.set_attr("v", kv.1)?;
    element.append(&mut tag)
}

fn add_metadata(element: &mut XmlNode, metadata: &Metadata) -> anyhow::Result<()> {
    element.set_attr("changeset", &metadata.changeset.to_string())?;
    element.set_attr(
        "timestamp",
        &metadata
            .timestamp
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )?;
    element.set_attr("uid", &metadata.uid.to_string())?;
    element.set_attr("user", &metadata.user)?;
    Ok(())
}

fn add_nds(doc: &Document, element: &mut XmlNode, nodes: &[WayNode]) -> anyhow::Result<()> {
    for n in nodes {
        let mut nd = new_xml_node("nd", doc)?;
        match n {
            WayNode::Resolved(node) => {
                nd.set_attr("ref", &node.id.to_string())?;
                nd.set_attr("version", &node.version.to_string())?;
                nd.set_attr("lon", &format!("{:.7}", node.lon))?;
                nd.set_attr("lat", &format!("{:.7}", node.lat))?;
            }
            WayNode::Dangling { id } => {
                nd.set_attr("ref", &id.to_string())?;
            }
        }
        element.append(&mut nd)?;
    }
    Ok(())
}

fn adiff_to_string(diff: &Diff) -> anyhow::Result<String> {
    let mut adiff = Document::new().map_err(|()| anyhow!("failed to create XML document"))?;
    let mut root = new_xml_node("osm", &adiff)?;
    root.set_attr("version", "0.6")?;
    root.set_attr("generator", "osmx-rs")?;
    adiff.set_root_element(&root);

    for action in &diff.actions {
        let mut action_element = new_xml_node("action", &adiff)?;
        match action {
            Action::Create(element) => {
                action_element.set_attr("type", "create")?;
                action_element.append(&mut element_to_xml(&adiff, element)?)?;
            }
            Action::Modify(old_element, new_element) => {
                action_element.set_attr("type", "modify")?;

                let mut old = new_xml_node("old", &adiff)?;
                let mut new = new_xml_node("new", &adiff)?;
                old.append(&mut element_to_xml(&adiff, old_element)?)?;
                new.append(&mut element_to_xml(&adiff, new_element)?)?;
                action_element.append(&mut old)?;
                action_element.append(&mut new)?;
            }
            Action::Delete(old_element, new_element) => {
                action_element.set_attr("type", "delete")?;
                let mut old = new_xml_node("old", &adiff)?;
                let mut new = new_xml_node("new", &adiff)?;
                old.append(&mut element_to_xml(&adiff, old_element)?)?;
                new.append(&mut element_to_xml(&adiff, new_element)?)?;
                action_element.append(&mut old)?;
                action_element.append(&mut new)?;
            }
        }
        root.append(&mut action_element)?;
    }

    let options = SaveOptions {
        format: true,
        non_significant_whitespace: true,
        ..Default::default()
    };

    Ok(adiff.to_string_with_options(options))
}

fn element_to_xml(doc: &Document, element: &AugmentedElement) -> anyhow::Result<XmlNode> {
    match element {
        AugmentedElement::Node(node) => {
            let mut xml_node = new_xml_node("node", doc)?;
            xml_node.set_attr("id", &format!("{}", node.id))?;
            xml_node.set_attr("version", &format!("{}", node.version))?;

            if !node.visible {
                xml_node.set_attr("visible", "false")?;
            }

            xml_node.set_attr("lon", &format!("{:.7}", node.lon))?;
            xml_node.set_attr("lat", &format!("{:.7}", node.lat))?;

            if let Some(metadata) = &node.metadata {
                add_metadata(&mut xml_node, metadata)?;
            }

            add_tags(doc, &mut xml_node, node.tags.iter())?;

            Ok(xml_node)
        }
        AugmentedElement::Way(way) => {
            let mut xml_node = new_xml_node("way", doc)?;
            xml_node.set_attr("id", &format!("{}", way.id))?;
            xml_node.set_attr("version", &format!("{}", way.version))?;

            if !way.visible {
                xml_node.set_attr("visible", "false")?;
            }

            if let Some(metadata) = &way.metadata {
                add_metadata(&mut xml_node, metadata)?;
            }

            if let Some(b) = bounds_for(element) {
                add_bounds(doc, &mut xml_node, &b)?;
            }
            add_nds(doc, &mut xml_node, &way.nodes)?;
            add_tags(doc, &mut xml_node, way.tags.iter())?;

            Ok(xml_node)
        }
        AugmentedElement::Relation(relation) => {
            let mut xml_node = new_xml_node("relation", doc)?;
            xml_node.set_attr("id", &format!("{}", relation.id))?;
            xml_node.set_attr("version", &format!("{}", relation.version))?;

            if !relation.visible {
                xml_node.set_attr("visible", "false")?;
            }

            if let Some(metadata) = &relation.metadata {
                add_metadata(&mut xml_node, metadata)?;
            }

            if let Some(b) = bounds_for(element) {
                add_bounds(doc, &mut xml_node, &b)?;
            }
            for member in &relation.members {
                let mut m = new_xml_node("member", doc)?;
                let (type_str, ref_str) = match member.id() {
                    ElementId::Node(id) => ("node", id.to_string()),
                    ElementId::Way(id) => ("way", id.to_string()),
                    ElementId::Relation(id) => ("relation", id.to_string()),
                };
                m.set_attr("type", type_str)?;
                m.set_attr("ref", &ref_str)?;
                m.set_attr("role", member.role())?;

                if let AugmentedMember::Resolved { element, .. } = member {
                    match &**element {
                        AugmentedElement::Node(node) => {
                            m.set_attr("lat", &format!("{:.7}", node.lat))?;
                            m.set_attr("lon", &format!("{:.7}", node.lon))?;
                        }
                        AugmentedElement::Way(way) => {
                            add_nds(doc, &mut m, &way.nodes)?;
                        }
                        AugmentedElement::Relation(_) => {
                            // TODO: should sub-relation members be recursively augmented?
                            // For now, they are not.
                        }
                    }
                }

                xml_node.append(&mut m)?;
            }
            add_tags(doc, &mut xml_node, relation.tags.iter())?;

            Ok(xml_node)
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    minlat: f64,
    minlon: f64,
    maxlat: f64,
    maxlon: f64,
}

fn add_bounds(doc: &Document, parent: &mut XmlNode, b: &Bounds) -> anyhow::Result<()> {
    let mut bounds = new_xml_node("bounds", doc)?;
    bounds.set_attr("minlat", &format!("{:.7}", b.minlat))?;
    bounds.set_attr("minlon", &format!("{:.7}", b.minlon))?;
    bounds.set_attr("maxlat", &format!("{:.7}", b.maxlat))?;
    bounds.set_attr("maxlon", &format!("{:.7}", b.maxlon))?;
    parent.append(&mut bounds)
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
