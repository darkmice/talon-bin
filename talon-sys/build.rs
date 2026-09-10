/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target = env::var("TARGET").unwrap();
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // ── 优先使用本地库路径（开发环境）──
    // 设置 TALON_LIB_DIR 环境变量指向包含 libtalon.a / libtalon_bundle_evocore.a 的目录。
    let has_evocore = env::var("CARGO_FEATURE_EVOCORE").is_ok();
    let lib_name = if has_evocore {
        "talon_bundle_evocore"
    } else {
        "talon"
    };

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=TALON_LIB_DIR");
    println!("cargo:rerun-if-env-changed=TALON_LIB_SHA256");
    println!("cargo:rerun-if-env-changed=TALON_RELEASE_DIR");
    println!("cargo:rerun-if-env-changed=TALON_SOURCE_ROOT");
    println!("cargo:rerun-if-env-changed=TALON_SOURCE_COMMIT");
    println!("cargo:rerun-if-env-changed=TALON_ALLOW_NETWORK_DOWNLOAD");
    println!("cargo:rerun-if-env-changed=TALON_ARCHIVE_SHA256");

    if let Some(local_dir) = validate_env_lib_dir(&target_os, lib_name) {
        emit_static_link(&target_os, &local_dir, lib_name);
        link_system_libs();
        return;
    }

    if let Some(local_dir) = try_build_local_source(&target_os, &target, has_evocore, lib_name) {
        emit_static_link(&target_os, &local_dir, lib_name);
        link_system_libs();
        return;
    }

    // ── 从固定 GitHub Release 下载预编译库（必须显式启用并给出哈希）──
    if env::var("TALON_ALLOW_NETWORK_DOWNLOAD").as_deref() != Ok("1") {
        panic!(
            "No verified Talon Core library was supplied. Set TALON_LIB_DIR + \
             TALON_LIB_SHA256 for an offline artifact, or explicitly set \
             TALON_ALLOW_NETWORK_DOWNLOAD=1 + TALON_ARCHIVE_SHA256 for the fixed release."
        );
    }
    let expected_archive_sha = required_sha_env("TALON_ARCHIVE_SHA256");
    let lib_dir = out_dir.join("talon-lib");
    fs::create_dir_all(&lib_dir).unwrap();

    let (platform_suffix, lib_file) = match (target_os.as_str(), target_arch.as_str()) {
        ("linux", "x86_64") => ("linux-amd64", "libtalon.a"),
        ("linux", "aarch64") => ("linux-arm64", "libtalon.a"),
        ("macos", "x86_64") => ("macos-amd64", "libtalon.a"),
        ("macos", "aarch64") => ("macos-arm64", "libtalon.a"),
        ("windows", "x86_64") => ("windows-amd64", "talon.lib"),
        ("linux", "loongarch64") => ("linux-loongarch64", "libtalon.a"),
        ("linux", "riscv64") | ("linux", "riscv64gc") => ("linux-riscv64", "libtalon.a"),
        (os, arch) => {
            panic!(
                "Unsupported platform: {os}-{arch}. Talon supports linux/macos/windows on x86_64/aarch64/loongarch64/riscv64."
            );
        }
    };

    let lib_path = lib_dir.join(lib_file);

    // 预编译库版本 — 仅在底层 C 库变更时更新。下载地址固定到版本，
    // 归档哈希由调用方/签名 manifest 提供，不能从 tag 推导。
    const TALON_LIB_VERSION: &str = "0.1.42";
    let archive_prefix = if has_evocore {
        "libtalon-evocore"
    } else {
        "libtalon-core"
    };
    let archive_name = format!("{archive_prefix}-{platform_suffix}.tar.gz");
    let archive_path = lib_dir.join(&archive_name);
    let url = format!(
        "https://github.com/darkmice/talon-bin/releases/download/v{TALON_LIB_VERSION}/{archive_name}"
    );

    let bytes = if archive_path.exists() {
        fs::read(&archive_path).unwrap_or_else(|err| {
            panic!(
                "Failed to read cached archive {}: {err}",
                archive_path.display()
            )
        })
    } else {
        eprintln!("cargo:warning=Downloading fixed Talon library from {url}");
        let response = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client")
            .get(&url)
            .send()
            .unwrap_or_else(|e| panic!("Failed to download {url}: {e}"));
        if !response.status().is_success() {
            panic!(
                "Failed to download {url}: HTTP {}. Make sure release v{TALON_LIB_VERSION} exists.",
                response.status()
            );
        }
        let bytes = response
            .bytes()
            .expect("Failed to read response body")
            .to_vec();
        fs::write(&archive_path, &bytes).unwrap_or_else(|err| {
            panic!(
                "Failed to cache verified archive {}: {err}",
                archive_path.display()
            )
        });
        bytes
    };
    let actual_archive_sha = sha256_bytes(&bytes);
    if actual_archive_sha != expected_archive_sha {
        panic!(
            "Talon archive SHA-256 mismatch: expected {expected_archive_sha}, got {actual_archive_sha}"
        );
    }

    // Re-extract on every build-script run. A verified cached archive is the source
    // of truth; an independently modified extracted library is never reused.
    let decoder = flate2::read::GzDecoder::new(&bytes[..]);
    let mut archive = tar::Archive::new(decoder);
    extract_exact_file(&mut archive, lib_file, &lib_path);

    // evocore archives ship libtalon.a but link as talon_bundle_evocore.
    if has_evocore {
        let dst = lib_dir.join(if target_os == "windows" {
            "talon_bundle_evocore.lib"
        } else {
            "libtalon_bundle_evocore.a"
        });
        fs::rename(&lib_path, &dst)
            .unwrap_or_else(|e| panic!("Failed to rename {lib_path:?} → {dst:?}: {e}"));
        eprintln!(
            "cargo:warning=Renamed {} → {} for evocore feature",
            lib_path.display(),
            dst.display()
        );
    }

    emit_static_link(&target_os, &lib_dir, lib_name);
    link_system_libs();
}

fn validate_env_lib_dir(target_os: &str, lib_name: &str) -> Option<PathBuf> {
    let local_dir = env::var("TALON_LIB_DIR").ok()?;
    let path = PathBuf::from(&local_dir);
    if !path.exists() {
        panic!("TALON_LIB_DIR={local_dir} does not exist");
    }
    let lib_path = path.join(required_lib_filename(target_os, lib_name));
    if !lib_path.exists() {
        panic!(
            "TALON_LIB_DIR={local_dir} is missing {}",
            lib_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("required static library")
        );
    }
    let expected = required_sha_env("TALON_LIB_SHA256");
    let actual = sha256_file(&lib_path).unwrap_or_else(|err| {
        panic!(
            "Failed to hash local Talon library {}: {err}",
            lib_path.display()
        )
    });
    if actual != expected {
        panic!(
            "Local Talon library SHA-256 mismatch for {}: expected {expected}, got {actual}",
            lib_path.display()
        );
    }
    eprintln!("cargo:warning=Using local Talon library ({lib_name}) from {local_dir}");
    Some(path)
}

fn try_build_local_source(
    target_os: &str,
    target: &str,
    has_evocore: bool,
    link_lib_name: &str,
) -> Option<PathBuf> {
    let local_source_profile = "release";
    let source_root = detect_source_root()?;
    if has_evocore {
        panic!(
            "Verified local source builds are Core-only. Supply an already built and hashed \
             EvoCore library with TALON_LIB_DIR + TALON_LIB_SHA256."
        );
    }
    validate_source_checkout(&source_root);
    let bundle_lib_name = "talon";
    let dependency_dirs: &[&str] = &[];
    let bundle_dir = source_root.clone();
    let bundle_manifest = bundle_dir.join("Cargo.toml");
    if !bundle_manifest.exists() {
        panic!("TALON_SOURCE_ROOT does not contain Cargo.toml");
    }

    let staged_dir = talon_release_dir(&source_root, target, local_source_profile);
    let staged_lib = staged_dir.join(required_lib_filename(target_os, link_lib_name));

    println!("cargo:rerun-if-changed={}", bundle_manifest.display());
    println!(
        "cargo:rerun-if-changed={}",
        bundle_dir.join("src").display()
    );

    if let Some(workspace_root) = source_root.parent() {
        for dependency_dir in dependency_dirs {
            let dep_path = workspace_root.join(dependency_dir);
            if dep_path.exists() {
                println!(
                    "cargo:rerun-if-changed={}",
                    dep_path.join("Cargo.toml").display()
                );
                println!("cargo:rerun-if-changed={}", dep_path.join("src").display());
            }
        }
    }

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(&bundle_manifest)
        .arg("--locked")
        .arg("--lib")
        .arg("--target")
        .arg(target)
        .arg("--release")
        .env_remove("TALON_LIB_DIR")
        .env("CARGO_TARGET_DIR", bundle_dir.join("target"))
        .env("CARGO_TERM_COLOR", "never");

    let status = match command.status() {
        Ok(status) => status,
        Err(err) => {
            panic!(
                "Failed to invoke local Talon source build at {}: {err}",
                bundle_manifest.display()
            );
        }
    };
    if !status.success() {
        panic!(
            "Local Talon source build failed at {}",
            bundle_manifest.display()
        );
    }
    validate_source_checkout(&source_root);

    let produced_lib = bundle_dir
        .join("target")
        .join(target)
        .join(local_source_profile)
        .join(required_lib_filename(target_os, bundle_lib_name));
    if !produced_lib.exists() {
        panic!(
            "Local Talon source build succeeded but produced library was not found at {}",
            produced_lib.display()
        );
    }

    fs::create_dir_all(&staged_dir).unwrap();
    fs::copy(&produced_lib, &staged_lib).unwrap_or_else(|err| {
        panic!(
            "Failed to stage local Talon library {} -> {}: {err}",
            produced_lib.display(),
            staged_lib.display()
        )
    });
    eprintln!(
        "cargo:warning=Using local Talon source build ({link_lib_name}) from {} -> {}",
        bundle_manifest.display(),
        staged_lib.display()
    );
    Some(staged_dir)
}

fn detect_source_root() -> Option<PathBuf> {
    let root = env::var("TALON_SOURCE_ROOT").ok()?;
    let path = PathBuf::from(root);
    if !path.exists() {
        panic!("TALON_SOURCE_ROOT={} does not exist", path.display());
    }
    Some(path)
}

fn validate_source_checkout(source_root: &Path) {
    let expected = env::var("TALON_SOURCE_COMMIT")
        .unwrap_or_else(|_| panic!("TALON_SOURCE_COMMIT is required with TALON_SOURCE_ROOT"));
    if expected.len() != 40
        || !expected
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        panic!("TALON_SOURCE_COMMIT must be a full lowercase 40-character commit SHA");
    }
    let actual = command_output(source_root, &["rev-parse", "HEAD"]);
    if actual != expected {
        panic!("Talon source commit mismatch: expected {expected}, got {actual}");
    }
    let status = command_output(source_root, &["status", "--porcelain=v1"]);
    if !status.is_empty() {
        panic!("Talon source worktree is dirty; refusing an unverifiable local build");
    }
}

fn command_output(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .unwrap_or_else(|err| panic!("Failed to run git {}: {err}", args.join(" ")));
    if !output.status.success() {
        panic!("git {} failed", args.join(" "));
    }
    String::from_utf8(output.stdout)
        .expect("git output is not UTF-8")
        .trim()
        .to_string()
}

fn required_sha_env(name: &str) -> String {
    let value = env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        panic!("{name} must be a lowercase SHA-256 hex digest");
    }
    value
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn extract_exact_file<R: Read>(
    archive: &mut tar::Archive<R>,
    expected_name: &str,
    destination: &Path,
) {
    let mut found = false;
    let entries = archive
        .entries()
        .expect("Failed to read Talon archive entries");
    for entry in entries {
        let mut entry = entry.expect("Failed to read Talon archive entry");
        let path = entry.path().expect("Invalid Talon archive path");
        if path.as_ref() != Path::new(expected_name) {
            continue;
        }
        if found || !entry.header().entry_type().is_file() {
            panic!("Talon archive contains a duplicate or non-file {expected_name}");
        }
        let mut output = fs::File::create(destination)
            .unwrap_or_else(|err| panic!("Failed to create {}: {err}", destination.display()));
        std::io::copy(&mut entry, &mut output)
            .unwrap_or_else(|err| panic!("Failed to extract {expected_name}: {err}"));
        output
            .flush()
            .expect("Failed to flush extracted Talon library");
        found = true;
    }
    if !found {
        panic!("Talon archive does not contain required file {expected_name}");
    }
}

fn talon_release_dir(_source_root: &Path, _target: &str, _profile: &str) -> PathBuf {
    env::var("TALON_RELEASE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| panic!("TALON_RELEASE_DIR is required with TALON_SOURCE_ROOT"))
}

fn required_lib_filename(target_os: &str, lib_name: &str) -> String {
    if target_os == "windows" {
        format!("{lib_name}.lib")
    } else {
        format!("lib{lib_name}.a")
    }
}

fn emit_static_link(target_os: &str, lib_dir: &Path, lib_name: &str) {
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    let actual_lib_file = required_lib_filename(target_os, lib_name);

    // 三个平台都使用 "全量包含" 策略，确保 #[no_mangle] C ABI 函数不被 dead-strip：
    // - macOS:   -force_load
    // - Linux:   --whole-archive
    // - Windows: /WHOLEARCHIVE
    if target_os == "macos" {
        let lib_full = lib_dir.join(&actual_lib_file);
        println!("cargo:rustc-link-lib=static={lib_name}");
        println!(
            "cargo:rustc-link-arg=-Wl,-force_load,{}",
            lib_full.display()
        );
    } else if target_os == "linux" {
        println!("cargo:rustc-link-arg=-Wl,--whole-archive");
        println!("cargo:rustc-link-lib=static={lib_name}");
        println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");
    } else if target_os == "windows" {
        println!("cargo:rustc-link-lib=static={lib_name}");
        println!("cargo:rustc-link-arg=/WHOLEARCHIVE:{lib_name}.lib");
    } else {
        println!("cargo:rustc-link-lib=static={lib_name}");
    }
}

/// 静态链接时需要显式链接系统库（Rust runtime 依赖）。
fn link_system_libs() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=framework=Security");
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=dylib=iconv");
    } else if target_os == "linux" {
        println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=dl");
        println!("cargo:rustc-link-lib=dylib=m");
    } else if target_os == "windows" {
        println!("cargo:rustc-link-lib=dylib=ws2_32");
        println!("cargo:rustc-link-lib=dylib=bcrypt");
        println!("cargo:rustc-link-lib=dylib=userenv");
        println!("cargo:rustc-link-lib=dylib=ntdll");
    }
}
