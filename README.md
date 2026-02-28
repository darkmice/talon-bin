# Talon Binary Releases

Pre-built binaries and libraries for [Talon](https://github.com/darkmice/talon-bin) — AI-native multi-model data engine.

## What is Talon?

Talon is a multi-model data engine designed for AI applications. It combines **SQL + KV + TimeSeries + MessageQueue + Vector** capabilities in a single binary with zero external dependencies.

## Download

Go to the [Releases](https://github.com/darkmice/talon-bin/releases) page to download the latest pre-built binaries.

### Binaries

| Platform | Architecture | File |
|----------|-------------|------|
| Linux | x86_64 | `talon-linux-amd64.tar.gz` |
| Linux | aarch64 | `talon-linux-arm64.tar.gz` |
| macOS | x86_64 (Intel) | `talon-macos-amd64.tar.gz` |
| macOS | aarch64 (Apple Silicon) | `talon-macos-arm64.tar.gz` |

Each binary archive contains:
- `talon-<platform>` — Server binary
- `talon-<platform>-cli` — Command-line client
- `talon-<platform>-tui` — Terminal UI

### Libraries (for embedding)

| Platform | Architecture | File |
|----------|-------------|------|
| Linux | x86_64 | `libtalon-talon-linux-amd64.tar.gz` |
| Linux | aarch64 | `libtalon-talon-linux-arm64.tar.gz` |
| macOS | x86_64 (Intel) | `libtalon-talon-macos-amd64.tar.gz` |
| macOS | aarch64 (Apple Silicon) | `libtalon-talon-macos-arm64.tar.gz` |

Each library archive contains `libtalon.so` / `libtalon.dylib` + `talon.h`.

## Quick Start (Binary)

```bash
# Download and extract (example: macOS Apple Silicon)
curl -LO https://github.com/darkmice/talon-bin/releases/latest/download/talon-macos-arm64.tar.gz
tar xzf talon-macos-arm64.tar.gz

# Start the server
./talon-macos-arm64

# Or use the CLI
./talon-macos-arm64-cli
```

## Use as Rust Dependency

This repo includes a `talon-sys` crate that provides safe Rust bindings. It automatically downloads the pre-built library during `cargo build`.

Add to your `Cargo.toml`:

```toml
[dependencies]
talon-sys = { git = "https://github.com/darkmice/talon-bin.git" }
```

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

Each release includes a `SHA256SUMS.txt` file. Verify your download:

```bash
# Download the checksum file
curl -LO https://github.com/darkmice/talon-bin/releases/latest/download/SHA256SUMS.txt

# Verify
sha256sum -c SHA256SUMS.txt
# or on macOS:
shasum -a 256 -c SHA256SUMS.txt
```

## License

MIT
