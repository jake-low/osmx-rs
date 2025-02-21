use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io::{self, Write};
use std::path::PathBuf;

use libxml::parser::Parser;
use libxml::tree::SaveOptions;
use libxml::tree::{Document, Node};
use osmx::messages_capnp::relation;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
enum ElementId {
    Node(u64),
    Way(u64),
    Relation(u64),
}

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
enum ActionType {
    Create,
    Modify,
    Delete,
}

impl ActionType {
    fn to_str(&self) -> &str {
        match self {
            ActionType::Create => "create",
            ActionType::Modify => "modify",
            ActionType::Delete => "delete",
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
}

pub fn run(args: &CliArgs) -> Result<(), Box<dyn Error>> {
    let osc_filename: &str = args.osc_file.as_os_str().to_str().unwrap();
    let osc = Parser::default().parse_file(osc_filename)?;

    // Pass 1
    // Parse the input OSC file and create a lookup table of the edits it contains

    let mut changes: BTreeMap<ElementId, (ActionType, Node)> = BTreeMap::new();
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

            let element_id = match &element.get_name()[..] {
                "node" => ElementId::Node(id),
                "way" => ElementId::Way(id),
                "relation" => ElementId::Relation(id),
                other => panic!("unknown element type: {}", other),
            };

            let mut element = element;
            element.unlink_node();
            changes.insert(element_id, (action_type, element));
        }
    }

    // Pass 2
    // Create the initial output XML tree of actions

    let db = osmx::Database::open(&args.osmx_file)?;
    let txn = osmx::Transaction::begin(&db)?;
    let locations = txn.locations()?;
    let nodes = txn.nodes()?;
    let ways = txn.ways()?;
    let relations = txn.relations()?;

    let mut adiff = Document::new().unwrap();
    adiff.set_root_element(&Node::new("osm", None, &adiff).unwrap());
    let mut root = adiff.get_root_element().unwrap();

    let update_element_from_metadata = |element: &mut Node, metadata: &osmx::Metadata| {
        // TODO: this helper function exists to avoid duplicating the logic in three places. I first
        // tried just extracting the Metadata from the node/way/rel in a match expression and then
        // acting on it, but that doesn't work because of current lifetime limitations in osmx-rs.
        // Specifically, metadata() returns a struct that cannot outlive the node/way/ relation it
        // came from, even though conceptually its only bound should be to the lifetime of the LMDB
        // transaction. I'm not sure how to fix this, I need to understand capnp/ better first
        element.set_attribute("version", &format!("{}", metadata.version()));
        element.set_attribute("changeset", &format!("{}", metadata.changeset()));
        let timestamp = chrono::DateTime::from_timestamp(metadata.timestamp() as i64, 0).unwrap();
        element.set_attribute(
            "timestamp",
            &timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        element.set_attribute("uid", &format!("{}", metadata.uid()));
        element.set_attribute("user", metadata.user());
    };

    let set_old_metadata =
        |element_id: ElementId, element: &mut Node| -> Result<(), Box<dyn Error>> {
            match element_id {
                ElementId::Node(id) => {
                    if let Some(loc) = locations.get(id) {
                        element.set_attribute("version", &format!("{}", loc.version()));

                        if let Some(node) = nodes.get(id) {
                            update_element_from_metadata(element, &node.metadata());
                        } else {
                            // element.set_attribute("changeset", "?");
                            // element.set_attribute("timestamp", "?");
                            // element.set_attribute("uid", "?");
                            // element.set_attribute("user", "?");
                        }
                    } else {
                        eprintln!("No old loc found for tagless node {}", id);
                    }
                }
                ElementId::Way(id) => {
                    if let Some(way) = ways.get(id) {
                        update_element_from_metadata(element, &way.metadata());
                    }
                }
                ElementId::Relation(id) => {
                    if let Some(rel) = relations.get(id) {
                        update_element_from_metadata(element, &rel.metadata());
                    }
                }
            };

            Ok(())
        };

    // Instead of adding actions directly to the XML tree, create them and store in BTreeMap
    let mut actions: BTreeMap<ElementId, Node> = BTreeMap::new();
    for (element_id, (action_type, element)) in changes.iter_mut() {
        let mut action = Node::new("action", None, &adiff).unwrap();
        action.set_attribute("type", action_type.to_str());

        let mut element = adiff.import_node(element).unwrap();

        match action_type {
            ActionType::Create => {
                action.add_child(&mut element)?;
            }
            ActionType::Modify | ActionType::Delete => {
                let mut new = Node::new("new", None, &adiff).unwrap();
                new.add_child(&mut element)?;

                if *action_type == ActionType::Delete {
                    element.set_attribute("visible", "false");
                }

                let mut old = Node::new("old", None, &adiff).unwrap();
                let mut old_element = Node::new(&element.get_name(), None, &adiff).unwrap();
                old_element.set_attribute("id", &element.get_attribute("id").unwrap());
                set_old_metadata(*element_id, &mut old_element)?;

                match element_id {
                    ElementId::Node(id) => {
                        if let Some(loc) = locations.get(*id) {
                            old_element.set_attribute("lon", &format!("{:.7}", loc.lon()));
                            old_element.set_attribute("lat", &format!("{:.7}", loc.lat()));

                            if let Some(node) = nodes.get(*id) {
                                add_tags(&adiff, &mut old_element, node.tags());
                            } else {
                                // tagless node, nothing to do
                            }
                        } else {
                            eprintln!("could not find {:?} in db, changing to create", element_id);
                            action.set_attribute("type", ActionType::Create.to_str());
                            element.unlink_node();
                            old.unlink_node();
                            new.unlink_node();
                            action.add_child(&mut element)?;
                        }
                    }
                    ElementId::Way(id) => {
                        if let Some(way) = ways.get(*id) {
                            for node_id in way.nodes() {
                                let mut nd = Node::new("nd", None, &adiff).unwrap();
                                nd.set_attribute("ref", &format!("{}", node_id));
                                old_element.add_child(&mut nd)?;
                            }

                            add_tags(&adiff, &mut old_element, way.tags());
                        } else {
                            eprintln!("could not find {:?} in db, changing to create", element_id);
                            action.set_attribute("type", ActionType::Create.to_str());
                            element.unlink_node();
                            old.unlink_node();
                            new.unlink_node();
                            action.add_child(&mut element)?;
                        }
                    }
                    ElementId::Relation(id) => {
                        if let Some(rel) = relations.get(*id) {
                            for m in rel.members() {
                                let mut member = Node::new("member", None, &adiff).unwrap();
                                match m.id() {
                                    osmx::ElementId::Node(id) => {
                                        member.set_attribute("type", "node");
                                        member.set_attribute("ref", &format!("{}", id));
                                    }
                                    osmx::ElementId::Way(id) => {
                                        member.set_attribute("type", "way");
                                        member.set_attribute("ref", &format!("{}", id));
                                    }
                                    osmx::ElementId::Relation(id) => {
                                        member.set_attribute("type", "relation");
                                        member.set_attribute("ref", &format!("{}", id));
                                    }
                                }
                                member.set_attribute("role", m.role());
                                old_element.add_child(&mut member)?;
                            }

                            add_tags(&adiff, &mut old_element, rel.tags());
                        } else {
                            eprintln!("could not find {:?} in db, changing to create", element_id);
                            action.set_attribute("type", ActionType::Create.to_str());
                            element.unlink_node();
                            old.unlink_node();
                            new.unlink_node();
                            action.add_child(&mut element)?;
                        }
                    }
                }

                old.add_child(&mut old_element)?;

                action.add_child(&mut old)?;
                action.add_child(&mut new)?;
            }
        }

        actions.insert(*element_id, action);
    }

    // Pass 3
    // Augment the elements in each action. Ways get node locations added to their <nd>
    // children and relations get their <member> elements inlined.

    let get_lon_lat = |node_id: u64, use_new: bool| {
        let element_id = ElementId::Node(node_id);
        if use_new && changes.contains_key(&element_id) {
            let (_, node) = changes.get(&element_id).unwrap();
            let lon: f64 = node.get_attribute("lon").unwrap().parse().unwrap();
            let lat: f64 = node.get_attribute("lat").unwrap().parse().unwrap();
            (lon, lat)
        } else {
            let loc = locations.get(node_id).unwrap();
            (loc.lon(), loc.lat())
        }
    };

    let augment_nd = |nd: &mut Node, use_new: bool| {
        let id: u64 = nd.get_attribute("ref").unwrap().parse().unwrap();
        let (lon, lat) = get_lon_lat(id, use_new);
        nd.set_attribute("lon", &format!("{:.7}", lon));
        nd.set_attribute("lat", &format!("{:.7}", lat));
    };

    let augment_member = |member: &mut Node, use_new: bool| -> Result<(), Box<dyn Error>> {
        let element_type = member.get_attribute("type").unwrap();
        let element_ref: u64 = member.get_attribute("ref").unwrap().parse().unwrap();

        match &element_type[..] {
            "node" => {
                let (lon, lat) = get_lon_lat(element_ref, use_new);
                member.set_attribute("lon", &format!("{:.7}", lon));
                member.set_attribute("lat", &format!("{:.7}", lat));
            }
            "way" => {
                let element_id = ElementId::Way(element_ref);
                if use_new && changes.contains_key(&element_id) {
                    let (_, way) = changes.get(&element_id).unwrap();
                    for child in way
                        .get_child_nodes()
                        .iter_mut()
                        .filter(|child| child.get_name() == "nd")
                    {
                        let node_id: u64 = child.get_attribute("ref").unwrap().parse().unwrap();
                        let mut nd = Node::new("nd", None, &adiff).unwrap();
                        nd.set_attribute("ref", &format!("{}", node_id));
                        let (lon, lat) = get_lon_lat(node_id, use_new);
                        nd.set_attribute("lon", &format!("{:.7}", lon));
                        nd.set_attribute("lat", &format!("{:.7}", lat));
                        member.add_child(&mut nd)?;
                    }
                } else {
                    let way = ways.get(element_ref).unwrap();
                    for node_id in way.nodes() {
                        let mut nd = Node::new("nd", None, &adiff).unwrap();
                        nd.set_attribute("ref", &format!("{}", node_id));
                        let (lon, lat) = get_lon_lat(node_id, use_new);
                        nd.set_attribute("lon", &format!("{:.7}", lon));
                        nd.set_attribute("lat", &format!("{:.7}", lat));
                        member.add_child(&mut nd)?;
                    }
                }
            }
            "relation" => {}
            _ => {}
        };

        Ok(())
    };

    let augment = |element: &mut Node, use_new: bool| {
        match &element.get_name()[..] {
            "way" => {
                for child in element
                    .get_child_nodes()
                    .iter_mut()
                    .filter(|child| child.get_name() == "nd")
                {
                    augment_nd(child, use_new);
                }
            }
            "relation" => {
                for child in element
                    .get_child_nodes()
                    .iter_mut()
                    .filter(|child| child.get_name() == "member")
                {
                    augment_member(child, use_new);
                }
            }
            _ => {}
        };
    };

    for (_, mut action) in actions.iter_mut() {
        match &action.get_attribute("type").unwrap()[..] {
            "create" => {
                let mut element = action.get_first_child().unwrap();
                augment(&mut element, true);
            }
            "modify" => {
                let old = action.get_first_child().unwrap();
                augment(&mut old.get_first_child().unwrap(), false);
                let new = old.get_next_sibling().unwrap();
                augment(&mut new.get_first_child().unwrap(), true);
            }
            "delete" => {
                let old = action.get_first_child().unwrap();
                augment(&mut old.get_first_child().unwrap(), false);
            }
            _ => unimplemented!(),
        }
    }

    // Pass 4: Find changes that propagate to referencing elements
    let node_ways = txn.node_ways()?;
    let node_relations = txn.node_relations()?;
    let way_relations = txn.way_relations()?;

    let mut affected_ways: BTreeMap<u64, ()> = BTreeMap::new();
    let mut affected_relations: BTreeMap<u64, ()> = BTreeMap::new();

    // find all affected elements
    for (element_id, (action_type, element)) in changes.iter() {
        if *action_type != ActionType::Modify {
            continue;
        }

        match element_id {
            ElementId::Node(node_id) => {
                // Check if node location changed
                let old_loc = if let Some(loc) = locations.get(*node_id) {
                    (loc.lat(), loc.lon())
                } else {
                    continue;
                };

                let new_loc = {
                    let lat: f64 = element.get_attribute("lat").unwrap().parse().unwrap();
                    let lon: f64 = element.get_attribute("lon").unwrap().parse().unwrap();
                    (lat, lon)
                };

                if old_loc != new_loc {
                    // Add all ways containing this node
                    for way_id in node_ways.get(*node_id) {
                        if !changes.contains_key(&ElementId::Way(way_id)) {
                            affected_ways.insert(way_id, ());

                            // Also add relations that contain this affected way
                            for rel_id in way_relations.get(way_id) {
                                if !changes.contains_key(&ElementId::Relation(rel_id)) {
                                    affected_relations.insert(rel_id, ());
                                }
                            }
                        }
                    }

                    // Add all relations containing this node
                    for rel_id in node_relations.get(*node_id) {
                        if !changes.contains_key(&ElementId::Relation(rel_id)) {
                            affected_relations.insert(rel_id, ());
                        }
                    }
                }
            }
            ElementId::Way(way_id) => {
                // Check if way's node list changed
                let old_way_nodes = if let Some(way) = ways.get(*way_id) {
                    way.nodes().collect::<Vec<_>>()
                } else {
                    continue;
                };

                let new_way_nodes = element
                    .get_child_nodes()
                    .iter()
                    .filter(|child| child.get_name() == "nd")
                    .map(|nd| nd.get_attribute("ref").unwrap().parse().unwrap())
                    .collect::<Vec<_>>();

                if old_way_nodes != new_way_nodes {
                    // Add all relations containing this way
                    for rel_id in way_relations.get(*way_id) {
                        if !changes.contains_key(&ElementId::Relation(rel_id)) {
                            affected_relations.insert(rel_id, ());
                        }
                    }
                }
            }
            ElementId::Relation(_) => {}
        }
    }

    // add affected ways to the output
    for way_id in affected_ways.keys() {
        let mut action = Node::new("action", None, &adiff).unwrap();
        action.set_attribute("type", "modify");

        let mut old = Node::new("old", None, &adiff).unwrap();
        let mut way_element = Node::new("way", None, &adiff).unwrap();
        way_element.set_attribute("id", &format!("{}", way_id));

        if let Some(way) = ways.get(*way_id) {
            update_element_from_metadata(&mut way_element, &way.metadata());

            for node_id in way.nodes() {
                let mut nd = Node::new("nd", None, &adiff).unwrap();
                nd.set_attribute("ref", &format!("{}", node_id));
                way_element.add_child(&mut nd)?;
            }

            add_tags(&adiff, &mut way_element, way.tags());
        }

        old.add_child(&mut way_element)?;
        augment(&mut way_element, false);

        // Create new version of the way (wish we could use clone() here but
        // trying to insert a cloned element into the tree is causing a runtime error)
        let mut new = Node::new("new", None, &adiff).unwrap();
        let mut new_way_element = Node::new("way", None, &adiff).unwrap();
        new_way_element.set_attribute("id", &format!("{}", way_id));

        if let Some(way) = ways.get(*way_id) {
            update_element_from_metadata(&mut new_way_element, &way.metadata());

            for node_id in way.nodes() {
                let mut nd = Node::new("nd", None, &adiff).unwrap();
                nd.set_attribute("ref", &format!("{}", node_id));
                new_way_element.add_child(&mut nd)?;
            }

            add_tags(&adiff, &mut new_way_element, way.tags());
        }

        new.add_child(&mut new_way_element)?;
        augment(&mut new_way_element, true);

        action.add_child(&mut old)?;
        action.add_child(&mut new)?;

        actions.insert(ElementId::Way(*way_id), action);
    }

    // add affected relations to the output
    for rel_id in affected_relations.keys() {
        let mut action = Node::new("action", None, &adiff).unwrap();
        action.set_attribute("type", "modify");

        let mut old = Node::new("old", None, &adiff).unwrap();
        let mut relation_element = Node::new("relation", None, &adiff).unwrap();
        relation_element.set_attribute("id", &format!("{}", rel_id));

        if let Some(rel) = relations.get(*rel_id) {
            update_element_from_metadata(&mut relation_element, &rel.metadata());

            for m in rel.members() {
                let mut member = Node::new("member", None, &adiff).unwrap();
                match m.id() {
                    osmx::ElementId::Node(id) => {
                        member.set_attribute("type", "node");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                    osmx::ElementId::Way(id) => {
                        member.set_attribute("type", "way");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                    osmx::ElementId::Relation(id) => {
                        member.set_attribute("type", "relation");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                }
                member.set_attribute("role", m.role());
                relation_element.add_child(&mut member)?;
            }

            add_tags(&adiff, &mut relation_element, rel.tags());
        }

        old.add_child(&mut relation_element)?;
        augment(&mut relation_element, false);

        // Create new version of the relation (wish we could use clone() here but
        // trying to insert a cloned element into the tree is causing a runtime error)
        let mut new = Node::new("new", None, &adiff).unwrap();
        let mut new_relation_element = Node::new("relation", None, &adiff).unwrap();
        new_relation_element.set_attribute("id", &format!("{}", rel_id));

        if let Some(rel) = relations.get(*rel_id) {
            update_element_from_metadata(&mut new_relation_element, &rel.metadata());

            for m in rel.members() {
                let mut member = Node::new("member", None, &adiff).unwrap();
                match m.id() {
                    osmx::ElementId::Node(id) => {
                        member.set_attribute("type", "node");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                    osmx::ElementId::Way(id) => {
                        member.set_attribute("type", "way");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                    osmx::ElementId::Relation(id) => {
                        member.set_attribute("type", "relation");
                        member.set_attribute("ref", &format!("{}", id));
                    }
                }
                member.set_attribute("role", m.role());
                new_relation_element.add_child(&mut member)?;
            }

            add_tags(&adiff, &mut new_relation_element, rel.tags());
        }

        new.add_child(&mut new_relation_element)?;
        augment(&mut new_relation_element, true);

        action.add_child(&mut old)?;
        action.add_child(&mut new)?;

        actions.insert(ElementId::Relation(*rel_id), action);
    }

    // After all passes are complete, add actions to the XML tree in sorted order
    for (_, mut action) in actions {
        root.add_child(&mut action)?;
    }

    // Pass 5: Add bounding boxes to elements
    for action in root.get_child_nodes().iter_mut() {
        if action.get_name() != "action" {
            continue;
        }

        let old = action.get_first_child().unwrap();
        if let Some(osm_obj) = old.get_first_child() {
            // Find all nodes in this element (including nested ones in ways/relations)
            let mut nodes = Vec::new();

            // Helper function to recursively find all nodes
            fn find_nodes(node: &Node, nodes: &mut Vec<(f64, f64)>) {
                for child in node.get_child_nodes() {
                    if child.get_name() == "nd" {
                        let lon: f64 = child.get_attribute("lon").unwrap().parse().unwrap();
                        let lat: f64 = child.get_attribute("lat").unwrap().parse().unwrap();
                        nodes.push((lon, lat));
                    } else if child.get_name() == "member" {
                        // For relation members, recursively search for nodes
                        find_nodes(&child, nodes);
                    }
                }
            }

            find_nodes(&osm_obj, &mut nodes);

            if !nodes.is_empty() {
                let min_lon = nodes
                    .iter()
                    .map(|(lon, _)| *lon)
                    .fold(f64::INFINITY, f64::min);
                let min_lat = nodes
                    .iter()
                    .map(|(_, lat)| *lat)
                    .fold(f64::INFINITY, f64::min);
                let max_lon = nodes
                    .iter()
                    .map(|(lon, _)| *lon)
                    .fold(f64::NEG_INFINITY, f64::max);
                let max_lat = nodes
                    .iter()
                    .map(|(_, lat)| *lat)
                    .fold(f64::NEG_INFINITY, f64::max);

                let mut bounds = Node::new("bounds", None, &adiff).unwrap();
                bounds.set_attribute("minlon", &format!("{:.7}", min_lon));
                bounds.set_attribute("minlat", &format!("{:.7}", min_lat));
                bounds.set_attribute("maxlon", &format!("{:.7}", max_lon));
                bounds.set_attribute("maxlat", &format!("{:.7}", max_lat));

                // Add bounds as first child
                let mut first_child = osm_obj.get_first_child().unwrap();
                first_child.add_prev_sibling(&mut bounds);
            }
        }
    }

    let mut options = SaveOptions::default();
    options.format = true;
    options.non_significant_whitespace = true;

    write!(&io::stdout(), "{}", &adiff.to_string_with_options(options))?; // FIXME: handle SIGPIPE

    Ok(())
}

fn add_tags<'a>(
    doc: &Document,
    element: &mut Node,
    tags: impl Iterator<Item = (&'a str, &'a str)>,
) {
    for kv in tags {
        add_tag(doc, element, kv)
    }
}

fn add_tag(doc: &Document, element: &mut Node, kv: (&str, &str)) {
    let mut tag = Node::new("tag", None, doc).unwrap();
    tag.set_attribute("k", &kv.0);
    tag.set_attribute("v", &kv.1);
    element.add_child(&mut tag);
}
