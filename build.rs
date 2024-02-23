fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/messages.capnp")
        .run()
        .expect("schema compiler command");
}
