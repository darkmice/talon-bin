#!/usr/bin/env bash
# Dispatch the legacy unsigned bundle release from an immutable set of sources.
# The GitHub workflow creates the tag after all platform builds pass.
set -euo pipefail

usage() {
  echo "Usage: $0 vX.Y.Z CORE AI LLM AGENT TRACE SANDBOX EVOCORE SESSION BLOCK" >&2
  echo "Pass full lowercase 40-character commit SHAs for every component." >&2
  exit 2
}

[[ $# -eq 10 ]] || usage
release_tag="$1"
shift
[[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || usage
for source_commit in "$@"; do
  [[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || usage
done

script_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$script_dir"
[[ "$(git branch --show-current)" == main ]] || {
  echo "Release source must be checked out on main." >&2
  exit 1
}
[[ -z "$(git status --porcelain=v1)" ]] || {
  echo "Release source has uncommitted changes." >&2
  exit 1
}
remote_main="$(git ls-remote origin refs/heads/main | cut -f1)"
[[ -n "$remote_main" && "$(git rev-parse HEAD)" == "$remote_main" ]] || {
  echo "Local main does not match origin/main." >&2
  exit 1
}
version="${release_tag#v}"
python3 - "$version" <<'PY'
import pathlib
import re
import sys
import tomllib

version = sys.argv[1]
cargo = tomllib.loads(pathlib.Path("talon-sys/Cargo.toml").read_text())
source = pathlib.Path("talon-sys/build.rs").read_text()
match = re.search(r'^\s*const TALON_LIB_VERSION: &str = "([^"]+)";\s*$', source, re.MULTILINE)
if cargo["package"]["version"] != version or match is None or match.group(1) != version:
    raise SystemExit(f"talon-sys Cargo/build.rs versions must both be {version} before dispatch")
PY

command -v gh >/dev/null || { echo "GitHub CLI is required." >&2; exit 1; }
gh workflow run build-bundle.yml \
  --repo darkmice/talon-bin --ref main \
  -f "release_tag=$release_tag" \
  -f "talon_core_ref=$1" \
  -f "talon_ai_ref=$2" \
  -f "talon_llm_ref=$3" \
  -f "talon_agent_ref=$4" \
  -f "talon_trace_ref=$5" \
  -f "talon_sandbox_ref=$6" \
  -f "talon_evocore_ref=$7" \
  -f "talon_session_ref=$8" \
  -f "talon_block_ref=$9"
