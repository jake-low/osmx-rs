use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use lmdb::Transaction;
use serde::{Deserialize, Serialize};

use crate::builders::{ElementType, LocationBuilder, NodeBuilder, RelationBuilder, WayBuilder};
use crate::sorter::Sorter;

#[derive(Parser)]
/// Convert an OSM PBF file to an OSMX database
pub struct CliArgs {
    /// Path of an .osm.pbf file to read
    input_file: PathBuf,
    /// Path of the .osmx file to create
    output_file: PathBuf,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
struct IDPair(u64, u64);

/// Reads sorted tuples from a Sorter and appends them to an LMDB table
fn insert_sorted_tuples(
    sorter: Sorter<IDPair>,
    txn: &mut lmdb::RwTransaction,
    table: lmdb::Database,
) {
    let bar = ProgressBar::new(sorter.count());
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {msg:>20} [{bar:40}] {pos}/{len}")
            .unwrap()
            .progress_chars("=> "),
    );
    bar.set_message(sorter.name().to_string());

    for IDPair(key, val) in sorter.sorted() {
        match txn.put(
            table,
            &key.to_le_bytes(),
            &val.to_le_bytes(),
            lmdb::WriteFlags::APPEND_DUP,
        ) {
            Ok(_) => {
                // eprintln!("Ok       {} {}", node, way);
            }
            Err(e) => {
                eprintln!("{:?} {} {}", e, key, val);
            }
        }
        // eprintln!("{} {}", node, way);
        bar.inc(1);
    }
    bar.finish();
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
        .open(args.output_file.as_ref())?;

    let element_flags = lmdb::DatabaseFlags::INTEGER_KEY;
    let index_flags = lmdb::DatabaseFlags::INTEGER_KEY
        | lmdb::DatabaseFlags::INTEGER_DUP
        | lmdb::DatabaseFlags::DUP_SORT
        | lmdb::DatabaseFlags::DUP_FIXED;

    let metadata = env.create_db(Some("metadata"), lmdb::DatabaseFlags::empty())?;
    let locations = env.create_db(Some("locations"), element_flags)?;
    let nodes = env.create_db(Some("nodes"), element_flags)?;
    let ways = env.create_db(Some("ways"), element_flags)?;
    let relations = env.create_db(Some("relations"), element_flags)?;
    let cell_node = env.create_db(Some("cell_node"), index_flags)?;
    let node_way = env.create_db(Some("node_way"), index_flags)?;
    let node_relation = env.create_db(Some("node_relation"), index_flags)?;
    let way_relation = env.create_db(Some("way_relation"), index_flags)?;
    let relation_relation = env.create_db(Some("relation_relation"), index_flags)?;

    let mut txn = env.begin_rw_txn()?;

    let tempdir = PathBuf::from(format!("{}-tmp", args.output_file.to_str().unwrap()));
    std::fs::create_dir_all(&tempdir).unwrap();

    let mut cell_node_sorter: Sorter<IDPair> = Sorter::new(&tempdir, "cell_node");
    let mut node_way_sorter: Sorter<IDPair> = Sorter::new(&tempdir, "node_way");
    let mut node_relation_sorter: Sorter<IDPair> = Sorter::new(&tempdir, "node_relation");
    let mut way_relation_sorter: Sorter<IDPair> = Sorter::new(&tempdir, "way_relation");
    let mut relation_relation_sorter: Sorter<IDPair> = Sorter::new(&tempdir, "relation_relation");

    // write metadata table

    let header = osmpbf::BlobReader::new(BufReader::new(File::open(&args.input_file)?))
        .map(|r| r.unwrap())
        .filter(|blob| match blob.get_type() {
            osmpbf::BlobType::OsmHeader => true,
            _ => false,
        })
        .next()
        .unwrap()
        .to_headerblock()?;

    if let Some(timestamp) = header.osmosis_replication_timestamp() {
        txn.put(
            metadata,
            &"osmosis_replication_timestamp".as_bytes(),
            &timestamp.to_ne_bytes(),
            lmdb::WriteFlags::empty(),
        )?;
    }

    if let Some(seqno) = header.osmosis_replication_timestamp() {
        txn.put(
            metadata,
            &"osmosis_replication_timestamp".as_bytes(),
            &seqno.to_ne_bytes(),
            lmdb::WriteFlags::empty(),
        )?;
    }

    txn.put(
        metadata,
        &"import_filename".as_bytes(),
        &args.input_file.as_os_str().as_encoded_bytes(),
        lmdb::WriteFlags::empty(),
    )?;

    // read .osm.pbf file and process each element

    let reader = osmpbf::ElementReader::from_path(&args.input_file)?;
    reader.for_each(|elem| match elem {
        osmpbf::Element::Node(node) => {
            let id = node.id() as u64;

            let location = LocationBuilder {
                longitude: node.lon(),
                latitude: node.lat(),
                version: node.info().version().unwrap() as u32,
            };

            txn.put(
                locations,
                &id.to_ne_bytes(),
                &location.build(),
                lmdb::WriteFlags::APPEND,
            )
            .unwrap();

            let latlng = s2::latlng::LatLng::from_degrees(node.lat(), node.lon());
            let cell = s2::cellid::CellID::from(latlng).parent(osmx::CELL_INDEX_LEVEL);
            cell_node_sorter.push(IDPair(cell.0, id));

            if node.tags().len() == 0 {
                return;
            }

            let tags: Vec<&str> = node.tags().map(|(k, v)| [k, v]).flatten().collect();

            let buf = NodeBuilder::new().set_tags(&tags[..]).build();

            txn.put(nodes, &id.to_ne_bytes(), &buf, lmdb::WriteFlags::APPEND)
                .unwrap();
        }
        osmpbf::Element::DenseNode(node) => {
            let id = node.id() as u64;

            let location = LocationBuilder {
                longitude: node.lon(),
                latitude: node.lat(),
                version: node.info().unwrap().version() as u32,
            };

            txn.put(
                locations,
                &id.to_ne_bytes(),
                &location.build(),
                lmdb::WriteFlags::APPEND,
            )
            .unwrap();

            let latlng = s2::latlng::LatLng::from_degrees(node.lat(), node.lon());
            let cell = s2::cellid::CellID::from(latlng).parent(osmx::CELL_INDEX_LEVEL);
            cell_node_sorter.push(IDPair(cell.0, id));

            if node.tags().len() == 0 {
                return;
            }

            let tags: Vec<&str> = node.tags().map(|(k, v)| [k, v]).flatten().collect();

            let buf = NodeBuilder::new().set_tags(&tags[..]).build();

            txn.put(nodes, &id.to_ne_bytes(), &buf, lmdb::WriteFlags::APPEND)
                .unwrap();
        }
        osmpbf::Element::Way(way) => {
            let way_id = way.id() as u64;
            let tags: Vec<&str> = way.tags().map(|(k, v)| [k, v]).flatten().collect();
            let nodes: Vec<u64> = way.refs().map(|id| id as u64).collect();

            let mut builder = WayBuilder::new();

            builder.set_tags(&tags[..]);
            builder.set_nodes(&nodes[..]);

            txn.put(
                ways,
                &way_id.to_ne_bytes(),
                &builder.build(),
                lmdb::WriteFlags::APPEND,
            )
            .unwrap();

            let nodes_set: HashSet<u64> = nodes.iter().cloned().collect();
            for node_id in nodes_set {
                node_way_sorter.push(IDPair(node_id, way_id));
            }
        }
        osmpbf::Element::Relation(rel) => {
            let rel_id = rel.id() as u64;
            let tags: Vec<&str> = rel.tags().map(|(k, v)| [k, v]).flatten().collect();

            let members: Vec<(ElementType, u64, String)> = rel
                .members()
                .map(|member| {
                    let t = match member.member_type {
                        osmpbf::RelMemberType::Node => ElementType::Node,
                        osmpbf::RelMemberType::Way => ElementType::Way,
                        osmpbf::RelMemberType::Relation => ElementType::Relation,
                    };
                    (
                        t,
                        member.member_id as u64,
                        member.role().unwrap().to_string(),
                    )
                })
                .collect();

            let mut builder = RelationBuilder::new();

            builder.set_tags(&tags[..]);
            builder.set_members(&members[..]);

            txn.put(
                relations,
                &rel_id.to_ne_bytes(),
                &builder.build(),
                lmdb::WriteFlags::APPEND,
            )
            .unwrap();

            let node_members: HashSet<u64> = rel
                .members()
                .filter(|m| m.member_type == osmpbf::RelMemberType::Node)
                .map(|m| m.member_id as u64)
                .collect();

            for member_id in node_members {
                node_relation_sorter.push(IDPair(member_id, rel_id));
            }

            let way_members: HashSet<u64> = rel
                .members()
                .filter(|m| m.member_type == osmpbf::RelMemberType::Way)
                .map(|m| m.member_id as u64)
                .collect();

            for member_id in way_members {
                way_relation_sorter.push(IDPair(member_id, rel_id));
            }

            let relation_members: HashSet<u64> = rel
                .members()
                .filter(|m| m.member_type == osmpbf::RelMemberType::Relation)
                .map(|m| m.member_id as u64)
                .collect();

            for member_id in relation_members {
                relation_relation_sorter.push(IDPair(member_id, rel_id));
            }
        }
    })?;

    eprintln!("done reading {}", args.input_file.to_str().unwrap());

    insert_sorted_tuples(cell_node_sorter, &mut txn, cell_node);
    insert_sorted_tuples(node_way_sorter, &mut txn, node_way);
    insert_sorted_tuples(node_relation_sorter, &mut txn, node_relation);
    insert_sorted_tuples(way_relation_sorter, &mut txn, way_relation);
    insert_sorted_tuples(relation_relation_sorter, &mut txn, relation_relation);

    txn.commit()?;

    eprintln!("committed transaction.");

    std::fs::remove_dir_all(&tempdir).unwrap();

    Ok(())
}
