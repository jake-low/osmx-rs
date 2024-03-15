/// Example program which finds the node with the most tags and prints its ID
/// and tag count.
///
/// Usage: most_tags_seq OSMX_FILE
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_path = PathBuf::from(&args[1]);

    let db = osmx::Database::open(&file_path)?;
    let txn = osmx::Transaction::begin(&db)?;

    let nodes = txn.nodes()?;

    let mut best_count = 0;
    let mut best_id: Option<u64> = None;

    for (id, node) in nodes.iter() {
        let count = node.tags().count();
        if count > best_count {
            best_count = count;
            best_id = Some(id);
        }
    }

    println!("best node: {} with {} tags", best_id.unwrap(), best_count);
    Ok(())
}
