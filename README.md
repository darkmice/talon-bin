# Talon Binary Releases

Pre-built binaries for [Talon](https://github.com/darkmice/talon-bin) — AI-native multi-model data engine.

## What is Talon?

Talon is a multi-model data engine designed for AI applications. It combines **SQL + KV + TimeSeries + MessageQueue + Vector** capabilities in a single binary with zero external dependencies.

## Download

Go to the [Releases](https://github.com/darkmice/talon-bin/releases) page to download the latest pre-built binaries.

### Available Platforms

| Platform | Architecture | File |
|----------|-------------|------|
| Linux | x86_64 | `talon-linux-amd64.tar.gz` |
| Linux | aarch64 | `talon-linux-arm64.tar.gz` |
| macOS | x86_64 (Intel) | `talon-macos-amd64.tar.gz` |
| macOS | aarch64 (Apple Silicon) | `talon-macos-arm64.tar.gz` |

### Each archive contains

- `talon-<platform>` — Server binary
- `talon-<platform>-cli` — Command-line client
- `talon-<platform>-tui` — Terminal UI

## Quick Start

```bash
# Download and extract (example: macOS Apple Silicon)
curl -LO https://github.com/darkmice/talon-bin/releases/latest/download/talon-macos-arm64.tar.gz
tar xzf talon-macos-arm64.tar.gz

# Start the server
./talon-macos-arm64

# Or use the CLI
./talon-macos-arm64-cli
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
