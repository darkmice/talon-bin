use std::{path::PathBuf, process::Command};
fn git(root: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .expect("git identity is required for the native producer");
    assert!(
        output.status.success(),
        "native producer Git identity unavailable"
    );
    String::from_utf8(output.stdout)
        .expect("Git identity must be UTF8")
        .trim()
        .to_string()
}
fn main() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    let commit = git(&root, &["rev-parse", "HEAD"]);
    if let Ok(expected) = std::env::var("TALON_RUNTIME_PRODUCER_COMMIT") {
        assert_eq!(expected, commit, "producer commit mismatch");
    }
    let dirty = !git(&root, &["status", "--porcelain"]).is_empty();
    println!("cargo:rustc-env=TALON_RUNTIME_PRODUCER_COMMIT={commit}");
    println!("cargo:rustc-env=TALON_RUNTIME_PRODUCER_DIRTY={dirty}");
    println!("cargo:rerun-if-env-changed=TALON_RUNTIME_PRODUCER_COMMIT");
    println!("cargo:rerun-if-changed=sources.lock.json");
    println!("cargo:rerun-if-changed=build.rs");
}
