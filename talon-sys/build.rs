use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // ── 优先使用本地库路径（开发环境）──
    // 设置 TALON_LIB_DIR 环境变量指向包含 libtalon.a 的目录，跳过下载。
    // 例如：TALON_LIB_DIR=/path/to/superclaw-db/target/release cargo build
    if let Ok(local_dir) = env::var("TALON_LIB_DIR") {
        let path = PathBuf::from(&local_dir);
        if path.exists() {
            eprintln!("cargo:warning=Using local Talon library from {local_dir}");
            println!("cargo:rustc-link-search=native={local_dir}");
            println!("cargo:rustc-link-lib=static=talon");
            link_system_libs();
            println!("cargo:rerun-if-changed=build.rs");
            println!("cargo:rerun-if-env-changed=TALON_LIB_DIR");
            return;
        }
        eprintln!("cargo:warning=TALON_LIB_DIR={local_dir} does not exist, falling back to download");
    }

    // ── 从 GitHub Release 下载预编译库 ──
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lib_dir = out_dir.join("talon-lib");
    fs::create_dir_all(&lib_dir).unwrap();

    let (target_name, lib_file) = match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") => ("talon-linux-amd64", "libtalon.a"),
        ("linux", "aarch64") => ("talon-linux-arm64", "libtalon.a"),
        ("macos", "x86_64") => ("talon-macos-amd64", "libtalon.a"),
        ("macos", "aarch64") => ("talon-macos-arm64", "libtalon.a"),
        (os, arch) => {
            panic!("Unsupported platform: {os}-{arch}. Talon supports linux/macos on x86_64/aarch64.");
        }
    };

    let lib_path = lib_dir.join(lib_file);

    if !lib_path.exists() {
        let version = env!("CARGO_PKG_VERSION");
        let archive_name = format!("libtalon-{target_name}.tar.gz");
        let url = format!(
            "https://github.com/darkmice/talon-bin/releases/download/v{version}/{archive_name}"
        );

        eprintln!("cargo:warning=Downloading Talon library from {url}");

        let response = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client")
            .get(&url)
            .send()
            .unwrap_or_else(|e| panic!("Failed to download {url}: {e}"));

        if !response.status().is_success() {
            panic!(
                "Failed to download {url}: HTTP {}. Make sure release v{version} exists.",
                response.status()
            );
        }

        let bytes = response.bytes().expect("Failed to read response body");

        let decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut archive = tar::Archive::new(decoder);
        archive
            .unpack(&lib_dir)
            .expect("Failed to extract library archive");
    }

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=talon");
    link_system_libs();
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=TALON_LIB_DIR");
}

/// 静态链接时需要显式链接系统库（Rust runtime 依赖）。
fn link_system_libs() {
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=framework=Security");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=dylib=iconv");
    } else if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=dl");
        println!("cargo:rustc-link-lib=dylib=m");
    }
}
