use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lib_dir = out_dir.join("talon-lib");
    fs::create_dir_all(&lib_dir).unwrap();

    // Determine platform-specific library name
    let (target_name, lib_file) = match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") => ("talon-linux-amd64", "libtalon.so"),
        ("linux", "aarch64") => ("talon-linux-arm64", "libtalon.so"),
        ("macos", "x86_64") => ("talon-macos-amd64", "libtalon.dylib"),
        ("macos", "aarch64") => ("talon-macos-arm64", "libtalon.dylib"),
        (os, arch) => {
            panic!("Unsupported platform: {os}-{arch}. Talon supports linux/macos on x86_64/aarch64.");
        }
    };

    let lib_path = lib_dir.join(lib_file);

    // Skip download if library already exists (cached build)
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

        // Extract tar.gz
        let decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut archive = tar::Archive::new(decoder);
        archive
            .unpack(&lib_dir)
            .expect("Failed to extract library archive");
    }

    // Tell cargo where to find the library
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=talon");

    // Set rpath for runtime linking
    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path");
    }

    println!("cargo:rerun-if-changed=build.rs");
}
