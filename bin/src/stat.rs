use std::error::Error;
use std::path::PathBuf;

use clap::Parser;
use lmdb::Transaction;

const TABLE_NAMES: &[&str] = &[
    "locations",
    "nodes",
    "ways",
    "relations",
    "cell_node",
    "node_way",
    "node_relation",
    "way_relation",
    "relation_relation",
];

#[derive(Parser)]
/// Print stats about the contents of an OSMX database
pub struct CliArgs {
    /// Path to the .osmx file to read
    input_file: PathBuf,
}

pub fn run(args: &CliArgs) -> Result<(), Box<dyn Error>> {
    let env = lmdb::Environment::new()
        .set_flags(
            lmdb::EnvironmentFlags::NO_SUB_DIR
                | lmdb::EnvironmentFlags::NO_READAHEAD
                | lmdb::EnvironmentFlags::NO_SYNC,
        )
        .set_max_dbs(10)
        .set_map_size(50 * 1024 * 1024 * 1024) // 50 GiB
        .open(args.input_file.as_ref())?;

    println!(
        "{:<18} {:>12} {:>12} {:>12} {:>9} {:>9} {:>9}",
        "NAME", "ENTRIES", "SIZE (KiB)", "TOTAL PAGES", "BRANCH", "LEAF", "OVERFLOW"
    );
    for name in TABLE_NAMES {
        let db = env.open_db(Some(name))?;
        let txn = env.begin_ro_txn()?;

        let mut stat = lmdb_sys::MDB_stat {
            ms_psize: 0,
            ms_depth: 0,
            ms_branch_pages: 0,
            ms_leaf_pages: 0,
            ms_overflow_pages: 0,
            ms_entries: 0,
        };

        unsafe {
            lmdb_sys::mdb_stat(txn.txn(), db.dbi(), &mut stat);
        }

        let total_pages = stat.ms_leaf_pages + stat.ms_branch_pages + stat.ms_overflow_pages;
        let size = stat.ms_psize * total_pages as u32;

        println!(
            "{:<18} {:>12} {:>12} {:>12} {:>9} {:>9} {:>9}",
            name,
            stat.ms_entries,
            size / 1024,
            total_pages,
            stat.ms_branch_pages,
            stat.ms_leaf_pages,
            stat.ms_overflow_pages
        );
    }

    Ok(())
}
