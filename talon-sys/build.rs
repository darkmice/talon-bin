/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();

    // ── 优先使用本地库路径（开发环境）──
    // 设置 TALON_LIB_DIR 环境变量指向包含 libtalon.a / libtalon_bundle_evocore.a 的目录。
    let has_evocore = env::var("CARGO_FEATURE_EVOCORE").is_ok();
    let lib_name = if has_evocore { "talon_bundle_evocore" } else { "talon" };

    if let Ok(local_dir) = env::var("TALON_LIB_DIR") {
        let path = PathBuf::from(&local_dir);
        if path.exists() {
            eprintln!("cargo:warning=Using local Talon library ({lib_name}) from {local_dir}");
            println!("cargo:rustc-link-search=native={local_dir}");
            if target_os == "macos" {
                let lib_full = format!("{local_dir}/lib{lib_name}.a");
                println!("cargo:rustc-link-lib=static={lib_name}");
                println!("cargo:rustc-link-arg=-Wl,-force_load,{lib_full}");
            } else if target_os == "linux" {
                println!("cargo:rustc-link-arg=-Wl,--whole-archive");
                println!("cargo:rustc-link-lib=static={lib_name}");
                println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");
            } else if target_os == "windows" {
                // MSVC linker: /WHOLEARCHIVE 强制包含静态库中的所有 object，
                // 防止 talon_bundle_init_ai 等 #[no_mangle] 符号被 dead-strip。
                // 等价于 macOS 的 -force_load / Linux 的 --whole-archive。
                println!("cargo:rustc-link-lib=static={lib_name}");
                println!("cargo:rustc-link-arg=/WHOLEARCHIVE:{lib_name}.lib");
            } else {
                println!("cargo:rustc-link-lib=static={lib_name}");
            }
            link_system_libs();
            println!("cargo:rerun-if-changed=build.rs");
            println!("cargo:rerun-if-env-changed=TALON_LIB_DIR");
            return;
        }
        eprintln!(
            "cargo:warning=TALON_LIB_DIR={local_dir} does not exist, falling back to download"
        );
    }

    // ── 从 GitHub Release 下载预编译库 ──
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lib_dir = out_dir.join("talon-lib");
    fs::create_dir_all(&lib_dir).unwrap();

    let (target_name, lib_file) = match (target_os.as_str(), target_arch.as_str()) {
        ("linux", "x86_64") => ("talon-linux-amd64", "libtalon.a"),
        ("linux", "aarch64") => ("talon-linux-arm64", "libtalon.a"),
        ("macos", "x86_64") => ("talon-macos-amd64", "libtalon.a"),
        ("macos", "aarch64") => ("talon-macos-arm64", "libtalon.a"),
        ("windows", "x86_64") => ("talon-windows-amd64", "talon.lib"),
        ("linux", "loongarch64") => ("talon-linux-loongarch64", "libtalon.a"),
        ("linux", "riscv64") | ("linux", "riscv64gc") => ("talon-linux-riscv64", "libtalon.a"),
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
        const TALON_LIB_VERSION: &str = "0.1.27";

        // evocore feature 启用时下载 libtalon-evocore-*，否则 libtalon-*
        let archive_prefix = if has_evocore { "libtalon-evocore" } else { "libtalon" };
        let archive_name = format!("{archive_prefix}-{target_name}.tar.gz");
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
                eprintln!("cargo:warning=Renamed {} → {} for evocore feature", src.display(), dst.display());
            }
        }
    }

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    // 实际的静态库文件名（evocore 模式下已重命名）
    let actual_lib_file = if target_os == "windows" {
        format!("{lib_name}.lib")
    } else {
        format!("lib{lib_name}.a")
    };
    // 三个平台都使用 "全量包含" 策略，确保 #[no_mangle] C ABI 函数不被 dead-strip：
    // - macOS:   -force_load
    // - Linux:   --whole-archive
    // - Windows: /WHOLEARCHIVE
    if target_os == "macos" {
        let lib_full = format!("{}/{}", lib_dir.display(), actual_lib_file);
        println!("cargo:rustc-link-lib=static={lib_name}");
        println!("cargo:rustc-link-arg=-Wl,-force_load,{lib_full}");
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
    link_system_libs();
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=TALON_LIB_DIR");
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
