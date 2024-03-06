/// Example program which prints out details about a node, way, or
/// relation in the .osmx file.
///
/// Usage: show_element OSMX_FILE TYPE ID
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = std::path::PathBuf::from(&args[1]);
    let element_type = args[2].as_str();
    let element_id: u64 = str::parse(&args[3])?;

    // open the .osmx file
    let db = osmx::Database::open(&file_path)?;
    // begin a read transaction (this ensures reads all get a coherent snapshot of
    // the data, even if another process is writing simultaneously)
    let txn = osmx::Transaction::begin(&db)?;

    match element_type {
        "node" => {
            // get the nodes table (containing node metadata and tags for tagged nodes)
            // and the locations table (containing coordinates for all nodes)
            let nodes = txn.nodes()?;
            let locations = txn.locations()?;

            // look up the location and metadata for the node
            let location = locations.get(element_id).expect("node not found");
            let node = nodes.get(element_id); // may be None for untagged nodes

            println!("Node {}", element_id);
            println!("Location: {:.7} {:.7}", location.lon(), location.lat());

            if let Some(node) = node {
                println!("Tags ({})", node.tags().count());
                for (key, val) in node.tags() {
                    println!("  {:?} = {:?}", key, val);
                }
            }

            let node_ways = txn.node_ways()?;
            let node_relations = txn.node_relations()?;

            println!("Part of {} Ways", node_ways.get(element_id).count());
            for way_id in node_ways.get(element_id) {
                println!("  {}", way_id);
            }

            println!(
                "Member of {} Relations",
                node_relations.get(element_id).count()
            );
            for relation_id in node_relations.get(element_id) {
                println!("  {}", relation_id);
            }
        }
        "way" => {
            // get the ways table
            let ways = txn.ways()?;
            // look up the way by its ID
            let way = ways.get(element_id).expect("way not found");

            println!("Way {}", element_id);

            println!("Tags ({})", way.tags().count());
            for (key, val) in way.tags() {
                println!("  {:?} = {:?}", key, val);
            }

            println!("Nodes ({})", way.nodes().count());
            for node_id in way.nodes() {
                println!("  {}", node_id);
            }

            let way_relations = txn.way_relations()?;

            println!(
                "Member of {} Relations",
                way_relations.get(element_id).count()
            );
            for relation_id in way_relations.get(element_id) {
                println!("  {}", relation_id);
            }
        }
        "relation" => {
            // get the relations table
            let relations = txn.relations()?;
            // look up the relation by its ID
            let relation = relations.get(element_id).expect("relation not found");

            println!("Relation {}", element_id);

            println!("Tags ({})", relation.tags().count());
            for (key, val) in relation.tags() {
                println!("  {:?} = {:?}", key, val);
            }

            println!("Members ({})", relation.members().count());
            for member in relation.members() {
                println!("  {:?} {}", member.id(), member.role());
            }

            let relation_relations = txn.relation_relations()?;

            println!(
                "Member of {} Relations",
                relation_relations.get(element_id).count()
            );
            for relation_id in relation_relations.get(element_id) {
                println!("  {}", relation_id);
            }
        }
        _ => {
            eprintln!(
                "bad type {} (expected 'node', 'way', or 'relation')",
                element_type
            );
            std::process::exit(1)
        }
    }

    Ok(())
}
