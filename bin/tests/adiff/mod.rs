use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::bail;
use tempfile::TempDir;

#[macro_export]
macro_rules! adiff_test {
    ($test_name:ident) => {
        #[test]
        fn $test_name() -> anyhow::Result<()> {
            use crate::TestContext;
            use std::mem;
            use std::path::Path;

            let ctx = TestContext::new()?;
            let test_dir = Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/adiff/",
                stringify!($test_name)
            ));

            ctx.setup_test(test_dir)?;
            ctx.run_adiff(test_dir, false)?;

            let expected = std::fs::read_to_string(test_dir.join("expected.adiff"))?;
            let actual = std::fs::read_to_string(ctx.temp_dir.path().join("output.adiff"))?;

            match ctx.compare(&expected, &actual) {
                Ok(_) => {}
                Err(e) => {
                    mem::forget(ctx.temp_dir);
                    return Err(e);
                }
            }

            Ok(())
        }
    };
}

#[macro_export]
macro_rules! adiff_test_split {
    ($test_name:ident) => {
        mod $test_name {
            #[test]
            fn split() -> anyhow::Result<()> {
                use crate::TestContext;
                use std::mem;
                use std::path::Path;

                let ctx = TestContext::new()?;
                let test_dir = Path::new(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/adiff/",
                    stringify!($test_name)
                ));

                ctx.setup_test(test_dir)?;
                ctx.run_adiff(test_dir, true)?;

                for file in test_dir.join("expected").read_dir()? {
                    let file = file.unwrap();
                    let expected = std::fs::read_to_string(file.path())?;
                    let actual = std::fs::read_to_string(
                        ctx.temp_dir.path().join("actual").join(file.file_name()),
                    )?;
                    match ctx.compare(&expected, &actual) {
                        Ok(_) => {}
                        Err(e) => {
                            mem::forget(ctx.temp_dir);
                            return Err(e);
                        }
                    }
                }

                Ok(())
            }
        }
    };
}

adiff_test!(create_cafe_node);
adiff_test!(modify_bench_node);
adiff_test!(delete_crossing_node);
adiff_test!(modify_building_alignment);
adiff_test!(modify_farmland_shape);
adiff_test!(modify_highway_tags);

adiff_test!(split_two_changesets_one_node);
adiff_test!(same_changeset_modify_twice);
adiff_test!(same_changeset_create_then_modify);
adiff_test!(same_changeset_create_then_delete);
adiff_test!(same_changeset_modify_then_delete);

adiff_test!(modify_relation_tags);
adiff_test!(modify_node_in_relation);

adiff_test!(delete_tagless_node);
adiff_test!(modify_not_in_db_becomes_create);
adiff_test!(xml_escape_in_tags);
adiff_test!(empty_osc);

adiff_test!(way_geom_change_propagates_to_relation);
adiff_test!(modify_node_in_way_in_relation);
adiff_test!(delete_way_with_tags);
adiff_test!(cross_changeset_modify);

adiff_test!(tag_only_node_modify_propagates);
adiff_test!(tag_only_way_modify_propagates_to_relation);
adiff_test!(modify_relation_member_list);

adiff_test!(relation_with_missing_member);
adiff_test!(modify_relation_add_node_member);
adiff_test!(modify_relation_remove_member);
adiff_test!(modify_relation_change_role);
adiff_test!(modify_way_reverse_nodes);
adiff_test!(modify_way_remove_node);
adiff_test!(cross_changeset_create_then_delete);
adiff_test!(cross_changeset_modify_then_delete);

adiff_test_split!(create_cafe_node);
adiff_test_split!(split_two_changesets_one_node);
adiff_test_split!(cross_changeset_modify);
adiff_test_split!(cross_changeset_modify_way);
adiff_test_split!(cross_changeset_modify_relation);
adiff_test_split!(cross_changeset_create_then_modify);
adiff_test_split!(cross_changeset_create_then_delete);
adiff_test_split!(cross_changeset_modify_then_delete);

pub struct TestContext {
    pub temp_dir: TempDir,
    pub osmx_db: PathBuf,
}

impl TestContext {
    pub fn new() -> anyhow::Result<TestContext> {
        let temp_dir = tempfile::tempdir()?;
        let osmx_db = temp_dir.path().join("test.osmx");

        Ok(Self { temp_dir, osmx_db })
    }

    pub fn setup_test(&self, test_dir: &Path) -> anyhow::Result<()> {
        // Convert OSM XML to PBF using osmium
        let osm_pbf = self.temp_dir.path().join("input.pbf");
        let osm_xml = test_dir.join("input.osm");

        let output = Command::new("osmium")
            .args([
                "cat",
                osm_xml.to_str().unwrap(),
                "-o",
                osm_pbf.to_str().unwrap(),
            ])
            .output()?;

        if !output.status.success() {
            bail!(
                "Failed to convert OSM XML to PBF: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Convert PBF to OSMX database using the in-tree osmx-rs expand
        let output = Command::new(Self::binary_path())
            .args([
                "expand",
                osm_pbf.to_str().unwrap(),
                self.osmx_db.to_str().unwrap(),
            ])
            .output()?;

        if !output.status.success() {
            bail!(
                "Failed to convert PBF to OSMX database: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    fn binary_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("target")
            .join("debug")
            .join("osmx-rs")
    }

    pub fn run_adiff(&self, test_dir: &Path, split: bool) -> anyhow::Result<()> {
        let osc_file = test_dir.join("edits.osc");
        let output_path = if split {
            self.temp_dir.path().join("actual")
        } else {
            self.temp_dir.path().join("output.adiff")
        };

        let binary_path = Self::binary_path();

        let mut args = vec![
            "augmented-diff",
            self.osmx_db.to_str().unwrap(),
            osc_file.to_str().unwrap(),
            output_path.to_str().unwrap(),
        ];

        if split {
            args.push("--split");
        }

        let output = Command::new(binary_path).args(args).output()?;

        std::io::stdout().write_all(&output.stdout);
        std::io::stderr().write_all(&output.stderr);

        if !output.status.success() {
            bail!(
                "adiff command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    pub fn compare(&self, expected: &str, actual: &str) -> anyhow::Result<()> {
        // Write actual output to a temporary file
        let actual_path = self.temp_dir.path().join("actual.adiff");
        std::fs::write(&actual_path, actual)?;

        // Write expected output to a temporary file
        let expected_path = self.temp_dir.path().join("expected.adiff");
        std::fs::write(&expected_path, &expected)?;

        // Canonicalize both files
        let actual_canonical = Command::new("xmlstarlet")
            .args(["canonic", actual_path.to_str().unwrap()])
            .output()?;

        let expected_canonical = Command::new("xmlstarlet")
            .args(["canonic", expected_path.to_str().unwrap()])
            .output()?;

        if !actual_canonical.status.success() || !expected_canonical.status.success() {
            eprintln!("Failed to canonicalize XML files");
            if !actual_canonical.status.success() {
                eprintln!(
                    "Actual XML error: {}",
                    String::from_utf8_lossy(&actual_canonical.stderr)
                );
            }
            if !expected_canonical.status.success() {
                eprintln!(
                    "Expected XML error: {}",
                    String::from_utf8_lossy(&expected_canonical.stderr)
                );
            }
            bail!("Failed to canonicalize XML files");
        }

        let actual_canonical = String::from_utf8(actual_canonical.stdout)?;
        let expected_canonical = String::from_utf8(expected_canonical.stdout)?;

        if actual_canonical != expected_canonical {
            // Run git diff on the canonicalized versions; this requires first writing
            // the canonicalized versions out to temporary files
            let actual_canonical_path = self.temp_dir.path().join("actual.canonical.adiff");
            let expected_canonical_path = self.temp_dir.path().join("expected.canonical.adiff");

            std::fs::write(&actual_canonical_path, actual_canonical)?;
            std::fs::write(&expected_canonical_path, expected_canonical)?;
            let output = Command::new("git")
                .args([
                    "diff",
                    "--no-index",
                    "--color=always",
                    "--",
                    expected_canonical_path.to_str().unwrap(),
                    actual_canonical_path.to_str().unwrap(),
                ])
                .output()?;

            if output.status.success() {
                eprintln!("No differences found in canonicalized XML (but files don't match?)");
            } else {
                eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            }

            bail!("Output does not match expected file");
        }

        Ok(())
    }
}
