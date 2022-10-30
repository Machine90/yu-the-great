
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out_dir = env::current_dir()?;
    out_dir.push("src");
    env::set_var("OUT_DIR", out_dir);

    let compile_protos = vec![
        "RaftLog", "RaftConf", "RaftPayload", "RaftGroup", "Multi"
    ];
    for proto in compile_protos {
        prost_build::compile_protos(&[format!("proto/{}.proto", proto)], &["proto/".to_string()])?;
    }
    Ok(())
}
