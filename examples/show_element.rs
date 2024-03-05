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

            if let Some(node) = node {
                println!("Tags:");
                for (key, val) in node.tags() {
                    println!("  {:?} = {:?}", key, val);
                }
            }

            println!("Location: {:.7} {:.7}", location.lon(), location.lat());
        }
        "way" => {
            // get the ways table
            let ways = txn.ways()?;
            // look up the way by its ID
            let way = ways.get(element_id).expect("way not found");

            println!("Way {}", element_id);

            println!("Tags:");
            for (key, val) in way.tags() {
                println!("  {:?} = {:?}", key, val);
            }

            println!("Nodes:");
            for node_id in way.nodes() {
                println!("  {}", node_id);
            }
        }
        "relation" => {
            // get the relations table
            let relations = txn.relations()?;
            // look up the relation by its ID
            let relation = relations.get(element_id).expect("relation not found");

            println!("Relation {}", element_id);

            println!("Tags:");
            for (key, val) in relation.tags() {
                println!("  {:?} = {:?}", key, val);
            }

            println!("Members:");
            for member in relation.members() {
                println!("  {:?} {}", member.id(), member.role());
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
