/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target = env::var("TARGET").unwrap();
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
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
    println!("cargo:rerun-if-env-changed=TALON_SOURCE_ROOT");

    if let Some(local_dir) = validate_env_lib_dir(&target_os, lib_name) {
        emit_static_link(&target_os, &local_dir, lib_name);
        link_system_libs();
        return;
    }

    if let Some(local_dir) = try_build_local_source(
        &out_dir,
        &target_os,
        &target,
        &profile,
        has_evocore,
        lib_name,
    ) {
        emit_static_link(&target_os, &local_dir, lib_name);
        link_system_libs();
        return;
    }

    // ── 从 GitHub Release 下载预编译库 ──
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

    if !lib_path.exists() {
        // 预编译库版本 — 仅在底层 C 库（talon/talon-ai/talon-evo-core）变更时更新。
        // talon-sys 的 Rust FFI 绑定代码变更不需要更新此版本。
        const TALON_LIB_VERSION: &str = "0.1.37";

        // evocore feature 启用时下载 libtalon-evocore-*，否则 libtalon-*
        let archive_prefix = if has_evocore {
            "libtalon-evocore"
        } else {
            "libtalon"
        };
        let archive_name = format!("{archive_prefix}-{platform_suffix}.tar.gz");
        let url = format!(
            "https://github.com/darkmice/talon-bin/releases/download/v{TALON_LIB_VERSION}/{archive_name}"
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
                "Failed to download {url}: HTTP {}. Make sure release v{TALON_LIB_VERSION} exists.",
                response.status()
            );
        }

        let bytes = response.bytes().expect("Failed to read response body");

        let decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut archive = tar::Archive::new(decoder);
        archive
            .unpack(&lib_dir)
            .expect("Failed to extract library archive");

        // evocore archive ships files as libtalon.a / libtalon.dylib,
        // but we link as talon_bundle_evocore — rename after extraction.
        if has_evocore {
            let src = lib_dir.join(lib_file);
            let dst = lib_dir.join(if target_os == "windows" {
                "talon_bundle_evocore.lib"
            } else {
                "libtalon_bundle_evocore.a"
            });
            if src.exists() && !dst.exists() {
                fs::rename(&src, &dst)
                    .unwrap_or_else(|e| panic!("Failed to rename {src:?} → {dst:?}: {e}"));
                eprintln!(
                    "cargo:warning=Renamed {} → {} for evocore feature",
                    src.display(),
                    dst.display()
                );
            }
        }
    }

    emit_static_link(&target_os, &lib_dir, lib_name);
    link_system_libs();
}

fn validate_env_lib_dir(target_os: &str, lib_name: &str) -> Option<PathBuf> {
    let local_dir = env::var("TALON_LIB_DIR").ok()?;
    let path = PathBuf::from(&local_dir);
    if !path.exists() {
        eprintln!(
            "cargo:warning=TALON_LIB_DIR={local_dir} does not exist, falling back to source/download"
        );
        return None;
    }
    let lib_path = path.join(required_lib_filename(target_os, lib_name));
    if !lib_path.exists() {
        eprintln!(
            "cargo:warning=TALON_LIB_DIR={local_dir} is missing {}, falling back to source/download",
            lib_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("required static library")
        );
        return None;
    }
    eprintln!("cargo:warning=Using local Talon library ({lib_name}) from {local_dir}");
    Some(path)
}

fn try_build_local_source(
    out_dir: &Path,
    target_os: &str,
    target: &str,
    profile: &str,
    has_evocore: bool,
    link_lib_name: &str,
) -> Option<PathBuf> {
    let source_root = detect_source_root()?;
    let (bundle_dir_name, bundle_lib_name, dependency_dirs): (&str, &str, &[&str]) = if has_evocore
    {
        (
            "talon-bundle-evocore",
            "talon_bundle_evocore",
            &[
                "superclaw-db",
                "talon-ai",
                "talon-llm",
                "talon-agent",
                "talon-trace",
                "talon-sandbox",
                "talon-evo-core",
            ],
        )
    } else {
        (
            "talon-bundle",
            "talon_bundle",
            &["superclaw-db", "talon-ai", "talon-llm", "talon-agent"],
        )
    };

    let bundle_dir = source_root.join(bundle_dir_name);
    let bundle_manifest = bundle_dir.join("Cargo.toml");
    if !bundle_manifest.exists() {
        return None;
    }

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
        .arg("--lib")
        .arg("--target")
        .arg(target)
        .env_remove("TALON_LIB_DIR")
        .env("CARGO_TERM_COLOR", "never");
    if profile == "release" {
        command.arg("--release");
    }

    let status = match command.status() {
        Ok(status) => status,
        Err(err) => {
            eprintln!(
                "cargo:warning=Failed to invoke local Talon source build at {}: {err}",
                bundle_manifest.display()
            );
            return None;
        }
    };
    if !status.success() {
        eprintln!(
            "cargo:warning=Local Talon source build failed at {}, falling back to prebuilt library download",
            bundle_manifest.display()
        );
        return None;
    }

    let produced_lib = bundle_dir
        .join("target")
        .join(target)
        .join(profile)
        .join(required_lib_filename(target_os, bundle_lib_name));
    if !produced_lib.exists() {
        eprintln!(
            "cargo:warning=Local Talon source build succeeded but produced library was not found at {}",
            produced_lib.display()
        );
        return None;
    }

    let staged_dir = out_dir.join("talon-local-source");
    fs::create_dir_all(&staged_dir).unwrap();
    let staged_lib = staged_dir.join(required_lib_filename(target_os, link_lib_name));
    fs::copy(&produced_lib, &staged_lib).unwrap_or_else(|err| {
        panic!(
            "Failed to stage local Talon library {} -> {}: {err}",
            produced_lib.display(),
            staged_lib.display()
        )
    });
    eprintln!(
        "cargo:warning=Using local Talon source build ({link_lib_name}) from {}",
        bundle_manifest.display()
    );
    Some(staged_dir)
}

fn detect_source_root() -> Option<PathBuf> {
    if let Ok(root) = env::var("TALON_SOURCE_ROOT") {
        let path = PathBuf::from(root);
        if path.exists() {
            return Some(path);
        }
        eprintln!(
            "cargo:warning=TALON_SOURCE_ROOT={} does not exist, skipping local source build",
            path.display()
        );
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").ok()?);
    manifest_dir.parent().map(Path::to_path_buf)
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
