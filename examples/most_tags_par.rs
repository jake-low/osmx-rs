/// Example program which finds the node with the most tags and prints its ID
/// and tag count. Like most_tags_seq, but uses multiple threads to read faster.
///
/// Usage: most_tags_par NUM_THREADS OSMX_FILE
use std::error::Error;
use std::path::PathBuf;
use std::thread;

use lmdb::{Cursor, Transaction};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();

    let num_threads = str::parse::<usize>(args[1].as_str())?;
    let file_path = PathBuf::from(&args[2]);

    let env = lmdb::Environment::new()
        .set_flags(
            lmdb::EnvironmentFlags::NO_SUB_DIR
                | lmdb::EnvironmentFlags::NO_READAHEAD
                | lmdb::EnvironmentFlags::NO_SYNC,
        )
        .set_max_dbs(10)
        .set_map_size(50 * 1024 * 1024 * 1024) // 50 GiB
        .open(file_path.as_ref())?;

    let nodes = env.open_db(Some("nodes"))?;

    let highest_node_id: u64 = get_last_key(&env, nodes);
    let nodes_per_thread = f64::ceil(highest_node_id as f64 / num_threads as f64) as u64;
    eprintln!("highest node id: {}", highest_node_id);

    thread::scope(|s| {
        let env = &env;
        let mut handles: Vec<thread::ScopedJoinHandle<(u64, usize)>> = vec![];

        for start in (0..highest_node_id).step_by(nodes_per_thread as usize) {
            let end = start + nodes_per_thread;
            eprintln!("starting thread for {}..{}", start, end);
            let h = s.spawn(move || do_scan(env, nodes, start, end).unwrap());
            handles.push(h);
        }

        let (best_id, best_count) = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .reduce(|a, b| std::cmp::max_by(a, b, |a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0))))
            .unwrap();
        println!("best node: {} with {} tags", best_id, best_count);
    });

    Ok(())
}

fn do_scan(
    env: &lmdb::Environment,
    db: lmdb::Database,
    start: u64,
    end: u64,
) -> Result<(u64, usize), Box<dyn Error>> {
    // TODO: creating a new txn here isn't quite right; we want to be
    // creating a new child txn so that all of the threads see the same snapshot
    // of the db. But this doesn't seem possible since the Txn is !Send.
    // I think this might be a limitation of the lmdb crate. The lmdb-zero crate
    // uses Supercow and supports multiple ownership modes, maybe that is to
    // address this issue? Is it worth trying to port this crate to use lmdb-zero?
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;

    // sequential approach
    let mut best_count = 0;
    let mut best_id: Option<u64> = None;

    for (raw_key, raw_val) in cursor.iter_from(start.to_ne_bytes()) {
        let id = u64::from_le_bytes(raw_key.try_into().expect("key with incorrect length"));
        if id >= end {
            break;
        }

        let node = osmx::Node::try_from(raw_val).ok().unwrap();

        let count = node.tags().count();
        if count > best_count {
            best_count = count;
            best_id = Some(id);
        }
    }

    Ok((best_id.unwrap(), best_count))
}

pub fn get_last_key(env: &lmdb::Environment, db: lmdb::Database) -> u64 {
    let txn = env.begin_ro_txn().unwrap();
    let cursor = txn.open_ro_cursor(db).unwrap();
    let (raw_key, _) = cursor.get(None, None, lmdb_sys::MDB_LAST).unwrap();
    let id = u64::from_le_bytes(raw_key.unwrap().try_into().unwrap());
    id
}
