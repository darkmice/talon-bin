# Talon Binary Releases

Pre-built binaries and libraries for [Talon](https://github.com/darkmice/talon-bin) — AI-native multi-model data engine.

Enterprise embedded consumers should use the pure-Core, signed and pinned
artifact contract documented in [`supply-chain/README.md`](supply-chain/README.md).
That path never resolves `latest` and does not pull AI / LLM / Agent modules.

## What is Talon?

Talon is a multi-model data engine designed for AI applications. It combines **SQL + KV + TimeSeries + MessageQueue + Vector** capabilities in a single binary with zero external dependencies.

## Download

Go to the [Releases](https://github.com/darkmice/talon-bin/releases) page to download the latest pre-built binaries.

### Binaries

| Artifact | Contains | Notes |
|----------|----------|-------|
| `talon-core-<platform>.tar.gz` | `talon` + `talon-cli` + `talon-tui` | 纯 Talon Core 服务端 |
| `talon-<platform>.tar.gz` | `talon` + `talon-cli` + `talon-tui` | 默认 bundle：core + ai + llm + agent |
| `talon-full-<platform>.tar.gz` | `talon` + `talon-cli` + `talon-tui` | 全量 bundle：默认 + trace + sandbox + evocore |

`talon` 用于启动服务；`talon-cli` 和 `talon-tui` 用于通过 TCP 方式连接服务端。

### Libraries (only Talon modules)

| Artifact | Contains | Notes |
|----------|----------|-------|
| `libtalon-core-<platform>.tar.gz` | Talon core only | 不带 AI / LLM / Agent |
| `libtalon-<platform>.tar.gz` | Talon core + `talon-ai` + `talon-llm` + `talon-agent` | 默认 bundle |
| `libtalon-evocore-<platform>.tar.gz` | Default bundle + `talon-trace` + `talon-sandbox` + `talon-evo-core` | 全量 bundle（保留旧前缀做兼容） |

三个归档都包含 `talon.h`。

## Quick Start (Binary)

The following `latest` example is retained only for legacy interactive use. It
is not an acceptable Enterprise or reproducible-build installation source.

```bash
# Download and extract (example: macOS Apple Silicon, default bundle)
curl -LO https://github.com/darkmice/talon-bin/releases/latest/download/talon-macos-arm64.tar.gz
tar xzf talon-macos-arm64.tar.gz

# Start the server
./talon --addr 127.0.0.1:7720 --tcp-addr 127.0.0.1:7729

# Or use the CLI
./talon-cli --url "talon://127.0.0.1:7729"
```

## Use as Rust Dependency

This repo includes a `talon-sys` crate that provides safe Rust bindings. Its
default artifact is pure Talon Core. Local and downloaded libraries must be
matched to an explicit SHA-256; unverified paths are rejected.

Add to your `Cargo.toml`:

```toml
[dependencies]
talon-sys = { git = "https://github.com/darkmice/talon-bin.git", rev = "replace-with-full-40-character-commit" }
```

Replace the example value with the exact `release.talon_bin_commit` from the
verified manifest. Do not omit `rev` for Enterprise builds.

For an offline build, extract the signed Enterprise Core package, verify its
manifest first, then provide the exact static library and digest:

```bash
PINNED_TALON_LIB_SHA256="sha256-from-manifest-artifact-files"
TALON_LIB_DIR=/opt/talon/lib \
TALON_LIB_SHA256="$PINNED_TALON_LIB_SHA256" \
cargo build --locked --offline
```

Network download is disabled by default. The legacy fixed-version download
requires both `TALON_ALLOW_NETWORK_DOWNLOAD=1` and the independently trusted
`TALON_ARCHIVE_SHA256`; it never resolves `latest`.

An explicit local source build requires `TALON_SOURCE_ROOT`, the full clean
checkout SHA in `TALON_SOURCE_COMMIT`, and an explicit `TALON_RELEASE_DIR`.
Sibling-directory discovery and cached unverified source outputs are disabled.

Example usage:

```rust
use talon_sys::Talon;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Talon::open("./my-data")?;

    // SQL
    let result = db.run_sql("CREATE TABLE users (id INT, name TEXT)")?;

    // KV
    db.kv_set(b"key", b"value", 0)?;
    let val = db.kv_get(b"key")?;

    // Vector search
    db.vector_insert("embeddings", 1, &[0.1, 0.2, 0.3])?;
    let results = db.vector_search("embeddings", &[0.1, 0.2, 0.3], 10, "cosine")?;

    // Generic JSON command
    let resp = db.execute(r#"{"module":"kv","action":"set","params":{"key":"k","value":"v"}}"#)?;

    Ok(())
}
```

## Use as C/C++ Library

Download the library archive for your platform, then link against `libtalon.so` / `libtalon.dylib`:

```c
#include "talon.h"

int main() {
    TalonHandle *db = talon_open("./my-data");
    char *json = NULL;
    talon_run_sql(db, "SELECT 1 + 1", &json);
    printf("%s\n", json);
    talon_free_string(json);
    talon_close(db);
    return 0;
}
```

```bash
# Linux
gcc main.c -L. -ltalon -o app

# macOS
clang main.c -L. -ltalon -o app
```

## Verify Checksums

Legacy releases include a `SHA256SUMS.txt` file. Enterprise Core candidates add
a signed per-platform manifest, SBOM, license inventory, and offline bundle;
verify those before loading the library.

```bash
# Download the checksum file
curl -LO https://github.com/darkmice/talon-bin/releases/latest/download/SHA256SUMS.txt

# Verify
sha256sum -c SHA256SUMS.txt
# or on macOS:
shasum -a 256 -c SHA256SUMS.txt
```

## License

Talon Community Dual License Agreement: SSPL v1 or a separately executed Talon
Commercial Enterprise License. See [`LICENSE`](LICENSE). The previous MIT label
was inconsistent with the repository license and must not be relied upon.
