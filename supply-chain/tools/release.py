#!/usr/bin/env python3
"""Build and verify deterministic Talon Enterprise Core release metadata.

The script intentionally uses only the Python standard library and OpenSSL.
It never resolves a branch, tag, `latest`, or a guessed local checkout.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from typing import Any


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
TAG_RE = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$")
SYMBOL_RE = re.compile(r"^talon_[a-z0-9_]+$")
KEY_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
FILE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
TOOLCHAIN_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SEMVER_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
CORE_REPOSITORY = "https://github.com/darkmice/talon-core"
SDK_REPOSITORY = "https://github.com/darkmice/talon-sdk-go"

SDK_REQUIRED_SYMBOLS = [
    "talon_open",
    "talon_close",
    "talon_run_sql",
    "talon_run_sql_bin",
    "talon_run_sql_param_bin",
    "talon_kv_set",
    "talon_kv_get",
    "talon_kv_del",
    "talon_kv_incrby",
    "talon_kv_setnx",
    "talon_vector_insert",
    "talon_vector_search",
    "talon_vector_search_bin",
    "talon_start_server",
    "talon_stop_server",
    "talon_persist",
    "talon_free_string",
    "talon_free_bytes",
    "talon_last_error",
    "talon_last_error_code",
    "talon_clear_last_error",
    "talon_build_manifest",
    "talon_execute",
]

RUNTIME_FEATURE_CONTRACT = (
    "native_build_manifest_v1",
    "native_error_codes_v1",
    "sql_tlv_v1",
    "storage_conditional_batch_v1",
    "native_conditional_transaction_v2",
    "storage_conditional_prefix_scan_v1",
    "revision_stream_v1",
    "revision_stream_v2_mmr_proof",
)

REVISION_STREAM_V2_GATED_REASON = (
    "v2 authenticates fixed-head pages with MMR inclusion and boundary proofs, but SDK "
    "cross-target conformance, release capacity, recovery fixtures, and adversarial soak "
    "evidence remain open; legacy v1 remains linear"
)

PREFIX_SCAN_V1_GATED_REASON = (
    "v1 has merged Core and Go SDK implementations but no immutable Core tag, SDK release tag, "
    "cross-target digest conformance, or four-target release evidence"
)
PREFIX_SCAN_V1_CONFORMANCE_PATH = Path(__file__).resolve().parents[1] / "conformance" / "conditional-prefix-scan-v1.json"
PREFIX_SCAN_V1_IMPLEMENTATION_COMMIT = "2f337f524d0edc2dd2a7720b84aa8f036591b61e"
PREFIX_SCAN_V1_MERGE_COMMIT = "ad0ee98d445193f2c1e3345ca531108ddfa943bd"
PREFIX_SCAN_V1_SDK_IMPLEMENTATION_COMMIT = "2f26fa9c142d286ddfcd08bdbacad617ba3c02d3"
PREFIX_SCAN_V1_SDK_MERGE_COMMIT = "3d66ae0fc7dd3bd3ba8435e178ced4d853a50060"
PREFIX_SCAN_V1_DIGEST = "e7e7da431dc036ef3f584881544503f6124c395c64ccf9984e08381e30b5cee5"

# These public-header tokens bind the release admission check to the authenticated
# v2 wire contract. The locked header digest remains the full source of truth; the
# tokens make an accidental or selectively backported v1/fake-v2 header fail loudly.
REVISION_STREAM_V2_HEADER_TOKENS = (
    '"proof":{"version":1',
    "talon_mmr_sha256_v1",
    "previous_root_sha256",
    "content_sha256",
    "proof_root_sha256",
    "pinned_head",
    "observed_live_head",
    "invalid_request",
    "corrupt_stream",
    "snapshot_not_available",
)

RUNTIME_CAPABILITY_CONTRACT = {
    "storage_conditional_batch": (1, "available"),
    "native_conditional_transaction_v2": (2, "available"),
    "storage_conditional_prefix_scan": (1, "gated"),
    "revision_stream": (2, "gated"),
    "native_quorum": (1, "gated"),
    "server_ha_production_admission": (1, "gated"),
}

EXPECTED_TARGETS = {
    "linux-amd64": {
        "triple": "x86_64-unknown-linux-gnu",
        "runner": "ubuntu-24.04",
        "static_library": "libtalon.a",
        "dynamic_library": "libtalon.so",
    },
    "linux-arm64": {
        "triple": "aarch64-unknown-linux-gnu",
        "runner": "ubuntu-24.04",
        "static_library": "libtalon.a",
        "dynamic_library": "libtalon.so",
    },
    "macos-amd64": {
        "triple": "x86_64-apple-darwin",
        "runner": "macos-15-intel",
        "static_library": "libtalon.a",
        "dynamic_library": "libtalon.dylib",
    },
    "macos-arm64": {
        "triple": "aarch64-apple-darwin",
        "runner": "macos-15",
        "static_library": "libtalon.a",
        "dynamic_library": "libtalon.dylib",
    },
}


class ReleaseError(RuntimeError):
    pass


def load_json(path: Path) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReleaseError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicates)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ReleaseError(f"expected a JSON object in {path}")
    return value


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(*argv: str, cwd: Path | None = None, input_bytes: bytes | None = None) -> bytes:
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            input=input_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        stderr = getattr(exc, "stderr", b"").decode(errors="replace").strip()
        raise ReleaseError(f"command failed: {' '.join(argv)}{': ' + stderr if stderr else ''}") from exc
    return completed.stdout


def validate_lock(lock: dict[str, Any], allow_gated: bool) -> None:
    require_exact_keys(
        lock,
        {
            "schema_version",
            "release_tag",
            "channel",
            "talon_bin_source",
            "core",
            "abi",
            "runtime_attestation",
            "build",
            "signing",
            "targets",
        },
        "release lock",
    )
    if lock.get("schema_version") != "1.0":
        raise ReleaseError("release lock schema_version must be 1.0")
    if lock.get("channel") != "enterprise-core":
        raise ReleaseError("release lock channel must be enterprise-core")
    if not isinstance(lock.get("talon_bin_source"), str) or not lock["talon_bin_source"]:
        raise ReleaseError("release lock talon_bin_source must be non-empty")
    tag = lock.get("release_tag")
    if tag != "UNRELEASED" and not (isinstance(tag, str) and TAG_RE.fullmatch(tag)):
        raise ReleaseError("release_tag must be UNRELEASED or vX.Y.Z")
    core = require_exact_keys(
        lock["core"],
        {"repository", "tag", "commit", "cargo_version", "cargo_lock_sha256"},
        "core",
    )
    if core["repository"] != CORE_REPOSITORY:
        raise ReleaseError(f"core.repository must be {CORE_REPOSITORY}")
    if core["tag"] is not None and (not isinstance(core["tag"], str) or not TAG_RE.fullmatch(core["tag"])):
        raise ReleaseError("core.tag must be null or vX.Y.Z")
    if not isinstance(core["commit"], str) or not SHA_RE.fullmatch(core["commit"]):
        raise ReleaseError("core.commit must be a full 40-character lowercase commit SHA")
    if not isinstance(core["cargo_version"], str) or not SEMVER_RE.fullmatch(core["cargo_version"]):
        raise ReleaseError("core.cargo_version must be semver")
    for label, value in (
        ("core.cargo_lock_sha256", core["cargo_lock_sha256"]),
        ("abi.header_sha256", lock.get("abi", {}).get("header_sha256")),
    ):
        if not isinstance(value, str) or not HASH_RE.fullmatch(value):
            raise ReleaseError(f"{label} must be a SHA-256 hex digest")
    abi = require_exact_keys(
        lock["abi"],
        {"profile", "version", "header", "header_sha256", "required_symbols", "capabilities"},
        "abi",
    )
    if not isinstance(abi["profile"], str) or not abi["profile"]:
        raise ReleaseError("abi.profile must be non-empty")
    if type(abi["version"]) is not int or abi["version"] <= 0:
        raise ReleaseError("abi.version must be a positive integer")
    header = abi["header"]
    header_path = Path(header) if isinstance(header, str) else Path(".")
    if not isinstance(header, str) or header in ("", ".", "..") or header_path.is_absolute() or ".." in header_path.parts:
        raise ReleaseError("abi.header must be a safe relative path")
    symbols = abi["required_symbols"]
    if (
        not isinstance(symbols, list)
        or not symbols
        or any(not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol) for symbol in symbols)
        or len(symbols) != len(set(symbols))
    ):
        raise ReleaseError("abi.required_symbols must be unique Talon C ABI symbols")
    capabilities = require_exact_keys(abi["capabilities"], {"storage_conditional_batch_v1"}, "abi.capabilities")
    capability = require_exact_keys(
        capabilities["storage_conditional_batch_v1"],
        {"status", "reason"},
        "abi.capabilities.storage_conditional_batch_v1",
    )
    if capability["status"] != "gated" or not isinstance(capability["reason"], str) or not capability["reason"]:
        raise ReleaseError("storage_conditional_batch_v1 must remain gated for this SDK contract")
    attestation = validate_runtime_attestation_contract(lock["runtime_attestation"])
    build = require_exact_keys(lock["build"], {"rust_toolchain", "cargo_args", "reproducibility"}, "build")
    if not isinstance(build["rust_toolchain"], str) or not TOOLCHAIN_RE.fullmatch(build["rust_toolchain"]):
        raise ReleaseError("build.rust_toolchain must be an exact stable X.Y.Z version")
    if build["cargo_args"] != ["build", "--locked", "--release", "--lib"]:
        raise ReleaseError("build.cargo_args must be build --locked --release --lib")
    if not isinstance(build["reproducibility"], str) or not build["reproducibility"]:
        raise ReleaseError("build.reproducibility must be non-empty")
    targets = lock.get("targets")
    if not isinstance(targets, list) or len(targets) != len(EXPECTED_TARGETS):
        raise ReleaseError("targets must declare exactly the four Enterprise platforms")
    platforms: set[str] = set()
    for target in targets:
        target = require_exact_keys(
            target,
            {"platform", "triple", "runner", "static_library", "dynamic_library"},
            "target",
        )
        platform = target.get("platform")
        if not isinstance(platform, str) or platform in platforms or platform not in EXPECTED_TARGETS:
            raise ReleaseError(f"invalid or duplicate platform {platform!r}")
        platforms.add(platform)
        expected_target = {"platform": platform, **EXPECTED_TARGETS[platform]}
        if target != expected_target:
            raise ReleaseError(f"target {platform} does not match the Enterprise platform contract")
    if platforms != set(EXPECTED_TARGETS):
        raise ReleaseError("targets do not cover every Enterprise platform")
    signing = require_exact_keys(
        lock["signing"],
        {"status", "algorithm", "key_id", "public_key_sha256", "reason"},
        "signing",
    )
    if signing.get("algorithm") != "Ed25519":
        raise ReleaseError("only Ed25519 manifest signatures are supported")
    if signing.get("status") not in ("gated", "ready"):
        raise ReleaseError("signing.status must be gated or ready")
    if not isinstance(signing.get("reason"), str) or not signing["reason"]:
        raise ReleaseError("signing.reason must be non-empty")
    key_id = signing.get("key_id")
    fingerprint = signing.get("public_key_sha256")
    if not isinstance(key_id, str) or not KEY_ID_RE.fullmatch(key_id):
        raise ReleaseError("signing.key_id is invalid")
    if fingerprint is not None and (not isinstance(fingerprint, str) or not HASH_RE.fullmatch(fingerprint)):
        raise ReleaseError("signing.public_key_sha256 must be null or a SHA-256 hex digest")
    if signing["status"] == "ready" and (key_id == "UNASSIGNED" or fingerprint is None):
        raise ReleaseError("ready signing identity must be assigned and pinned")
    if not allow_gated:
        if tag == "UNRELEASED":
            raise ReleaseError("UNRELEASED lock cannot produce a release candidate")
        if signing.get("status") != "ready":
            raise ReleaseError("release signing is gated")
        if key_id == "UNASSIGNED":
            raise ReleaseError("release signing key_id is not assigned")
        if fingerprint is None:
            raise ReleaseError("release signing public_key_sha256 is not pinned")
        core_tag = lock.get("core", {}).get("tag")
        if not isinstance(core_tag, str) or not TAG_RE.fullmatch(core_tag):
            raise ReleaseError("release-ready Core source must have a locked vX.Y.Z tag")
        if attestation["status"] != "ready":
            raise ReleaseError("binary self-attestation is gated")
        if (
            abi["profile"] != attestation["abi_profile"]
            or abi["version"] != attestation["abi_version"]
            or abi["required_symbols"] != attestation["required_symbols"]
        ):
            raise ReleaseError("release ABI does not match the runtime attestation contract")


def validate_runtime_attestation_contract(value: Any) -> dict[str, Any]:
    attestation = require_exact_keys(
        value,
        {
            "status",
            "reason",
            "manifest_version",
            "abi_profile",
            "abi_version",
            "required_symbols",
            "features",
            "capabilities",
        },
        "runtime_attestation",
    )
    if attestation["status"] not in ("gated", "ready"):
        raise ReleaseError("runtime_attestation.status must be gated or ready")
    if not isinstance(attestation["reason"], str) or not attestation["reason"]:
        raise ReleaseError("runtime_attestation.reason must be non-empty")
    if type(attestation["manifest_version"]) is not int or attestation["manifest_version"] != 1:
        raise ReleaseError("runtime_attestation.manifest_version must be 1")
    if (
        attestation["abi_profile"] != "talon-native-c"
        or type(attestation["abi_version"]) is not int
        or attestation["abi_version"] != 1
    ):
        raise ReleaseError("runtime attestation ABI profile/version does not match the SDK")
    if attestation["required_symbols"] != SDK_REQUIRED_SYMBOLS:
        raise ReleaseError("runtime attestation required symbols do not match the SDK")
    features = attestation["features"]
    if not isinstance(features, list) or features != list(RUNTIME_FEATURE_CONTRACT):
        raise ReleaseError("runtime attestation features do not exactly match the SDK contract")
    capabilities = attestation["capabilities"]
    if not isinstance(capabilities, list) or len(capabilities) != len(RUNTIME_CAPABILITY_CONTRACT):
        raise ReleaseError("runtime attestation capabilities are incomplete")
    seen: set[str] = set()
    for index, raw in enumerate(capabilities):
        capability = require_exact_keys(raw, {"name", "version", "status", "reason"}, f"runtime_attestation.capabilities[{index}]")
        name = capability["name"]
        if not isinstance(name, str) or name in seen or name not in RUNTIME_CAPABILITY_CONTRACT:
            raise ReleaseError(f"unknown or duplicate runtime capability {name!r}")
        seen.add(name)
        expected_version, expected_status = RUNTIME_CAPABILITY_CONTRACT[name]
        status = capability["status"]
        if type(capability["version"]) is not int or capability["version"] != expected_version or status != expected_status:
            raise ReleaseError(f"runtime capability {name} does not match the production contract")
        reason = capability["reason"]
        if status == "gated":
            if not isinstance(reason, str) or not reason.strip():
                raise ReleaseError(f"gated runtime capability {name} requires a reason")
            if name == "revision_stream" and reason != REVISION_STREAM_V2_GATED_REASON:
                raise ReleaseError("runtime capability revision_stream gated reason does not match the v2 contract")
            if name == "storage_conditional_prefix_scan" and reason != PREFIX_SCAN_V1_GATED_REASON:
                raise ReleaseError("runtime capability storage_conditional_prefix_scan gated reason does not match the v1 contract")
        elif reason is not None:
            raise ReleaseError(f"available runtime capability {name} must have a null reason")
    return attestation


def prefix_scan_optional_bytes(value: Any, label: str) -> bytes:
    if value is None:
        return b"\x00"
    if not isinstance(value, str):
        raise ReleaseError(f"{label} must be null or a string")
    encoded = value.encode("utf-8")
    return b"\x01" + len(encoded).to_bytes(4, "big") + encoded


def prefix_scan_optional_u64(value: Any, label: str) -> bytes:
    if value is None:
        return b"\x00"
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value):
        raise ReleaseError(f"{label} must be null or a canonical positive u64 string")
    number = int(value)
    if number > 2**64 - 1:
        raise ReleaseError(f"{label} exceeds u64")
    return b"\x01" + number.to_bytes(8, "big")


def prefix_scan_bytes(value: Any, label: str) -> bytes:
    if not isinstance(value, list) or any(type(item) is not int or item < 0 or item > 255 for item in value):
        raise ReleaseError(f"{label} must be an array of byte values")
    return bytes(value)


def prefix_scan_u64(value: Any, label: str, *, allow_zero: bool = True) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise ReleaseError(f"{label} must be a canonical u64 string")
    number = int(value)
    if number > 2**64 - 1 or (not allow_zero and number == 0):
        raise ReleaseError(f"{label} is outside its u64 range")
    return number


def conditional_prefix_scan_response_digest(request: dict[str, Any], response: dict[str, Any]) -> str:
    required_request = require_exact_keys(
        request,
        {"version", "request_id", "namespace", "prefix", "limit", "cursor", "revision"},
        "conditional-prefix-scan digest request",
    )
    required_response = require_exact_keys(
        response,
        {
            "observed_revision",
            "snapshot_revision",
            "position",
            "entries",
            "next_cursor",
            "next_cursor_expires_at_unix_ms",
        },
        "conditional-prefix-scan digest response",
    )
    if type(required_request["version"]) is not int or required_request["version"] != 1:
        raise ReleaseError("conditional-prefix-scan digest version must be 1")
    if not isinstance(required_request["request_id"], str) or not required_request["request_id"].strip():
        raise ReleaseError("conditional-prefix-scan digest request_id must be non-empty text")
    if not isinstance(required_request["namespace"], str) or not required_request["namespace"]:
        raise ReleaseError("conditional-prefix-scan digest namespace must be non-empty text")
    if type(required_request["limit"]) is not int or not 1 <= required_request["limit"] <= 256:
        raise ReleaseError("conditional-prefix-scan digest limit must be 1..256")
    prefix = prefix_scan_bytes(required_request["prefix"], "conditional-prefix-scan digest prefix")

    digest = hashlib.sha256()
    digest.update(b"TALON_CONDITIONAL_PREFIX_SCAN_RESULT_V1")
    digest.update(required_request["version"].to_bytes(2, "big"))
    for label in ("request_id", "namespace"):
        encoded = required_request[label].encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    digest.update(len(prefix).to_bytes(4, "big"))
    digest.update(prefix)
    digest.update(required_request["limit"].to_bytes(2, "big"))
    digest.update(prefix_scan_optional_bytes(required_request["cursor"], "conditional-prefix-scan digest cursor"))
    digest.update(prefix_scan_optional_u64(required_request["revision"], "conditional-prefix-scan digest revision"))
    for label in ("observed_revision", "snapshot_revision", "position"):
        digest.update(prefix_scan_u64(required_response[label], f"conditional-prefix-scan digest {label}").to_bytes(8, "big"))
    entries = required_response["entries"]
    if not isinstance(entries, list) or len(entries) > required_request["limit"]:
        raise ReleaseError("conditional-prefix-scan digest entries exceed the request limit")
    digest.update(len(entries).to_bytes(4, "big"))
    position = prefix_scan_u64(required_response["position"], "conditional-prefix-scan digest position")
    previous_key = None
    for offset, entry in enumerate(entries):
        entry = require_exact_keys(entry, {"index", "key", "value"}, f"conditional-prefix-scan digest entries[{offset}]")
        index = prefix_scan_u64(entry["index"], f"conditional-prefix-scan digest entries[{offset}].index")
        if index != position + offset:
            raise ReleaseError("conditional-prefix-scan digest entry indexes must be contiguous")
        key = prefix_scan_bytes(entry["key"], f"conditional-prefix-scan digest entries[{offset}].key")
        value = prefix_scan_bytes(entry["value"], f"conditional-prefix-scan digest entries[{offset}].value")
        if previous_key is not None and key <= previous_key:
            raise ReleaseError("conditional-prefix-scan digest entry keys must be strictly ordered")
        previous_key = key
        digest.update(index.to_bytes(8, "big"))
        digest.update(len(key).to_bytes(4, "big"))
        digest.update(key)
        digest.update(len(value).to_bytes(4, "big"))
        digest.update(value)
    digest.update(prefix_scan_optional_bytes(required_response["next_cursor"], "conditional-prefix-scan digest next_cursor"))
    digest.update(prefix_scan_optional_u64(
        required_response["next_cursor_expires_at_unix_ms"],
        "conditional-prefix-scan digest next_cursor_expires_at_unix_ms",
    ))
    return digest.hexdigest()


def validate_prefix_scan_v1_conformance(path: Path, *, require_ready: bool = False, expected_core: dict[str, Any] | None = None) -> dict[str, Any]:
    value = load_json(path)
    value = require_exact_keys(
        value,
        {"schema_version", "contract", "status", "reason", "source", "sdk", "runtime", "limits", "digest_vector", "release_evidence"},
        "conditional-prefix-scan v1 conformance",
    )
    if value["schema_version"] != "1.0" or value["contract"] != "talon-conditional-prefix-scan-v1":
        raise ReleaseError("conditional-prefix-scan conformance schema or contract is invalid")
    if value["status"] not in ("gated", "ready"):
        raise ReleaseError("conditional-prefix-scan conformance status must be gated or ready")
    if value["status"] == "gated":
        if value["reason"] != PREFIX_SCAN_V1_GATED_REASON:
            raise ReleaseError("conditional-prefix-scan conformance gated reason does not match the v1 contract")
    elif value["reason"] is not None:
        raise ReleaseError("ready conditional-prefix-scan conformance cannot have a reason")

    source = require_exact_keys(value["source"], {"repository", "implementation_pull_request", "implementation_commit", "merge_commit", "release_identity"}, "conditional-prefix-scan conformance source")
    if (
        source["repository"] != CORE_REPOSITORY
        or source["implementation_pull_request"] != 11
        or source["implementation_commit"] != PREFIX_SCAN_V1_IMPLEMENTATION_COMMIT
        or source["merge_commit"] != PREFIX_SCAN_V1_MERGE_COMMIT
    ):
        raise ReleaseError("conditional-prefix-scan conformance source does not match the reviewed Core implementation")
    if value["status"] == "gated":
        if source["release_identity"] is not None:
            raise ReleaseError("gated conditional-prefix-scan conformance cannot name a release identity")
    else:
        identity = require_exact_keys(source["release_identity"], {"tag", "commit"}, "conditional-prefix-scan release identity")
        if not isinstance(identity["tag"], str) or not TAG_RE.fullmatch(identity["tag"]) or not isinstance(identity["commit"], str) or not SHA_RE.fullmatch(identity["commit"]):
            raise ReleaseError("conditional-prefix-scan release identity must contain a tag and full commit SHA")
        if expected_core is not None and (identity["tag"] != expected_core["tag"] or identity["commit"] != expected_core["commit"]):
            raise ReleaseError("conditional-prefix-scan release identity does not match the release lock")

    sdk = require_exact_keys(value["sdk"], {"repository", "implementation_pull_request", "implementation_commit", "merge_commit", "release_identity"}, "conditional-prefix-scan conformance SDK")
    if (
        sdk["repository"] != SDK_REPOSITORY
        or sdk["implementation_pull_request"] != 7
        or sdk["implementation_commit"] != PREFIX_SCAN_V1_SDK_IMPLEMENTATION_COMMIT
        or sdk["merge_commit"] != PREFIX_SCAN_V1_SDK_MERGE_COMMIT
    ):
        raise ReleaseError("conditional-prefix-scan conformance source does not match the reviewed Go SDK implementation")
    if value["status"] == "gated":
        if sdk["release_identity"] is not None:
            raise ReleaseError("gated conditional-prefix-scan conformance cannot name an SDK release identity")
    else:
        identity = require_exact_keys(sdk["release_identity"], {"tag", "commit"}, "conditional-prefix-scan SDK release identity")
        if not isinstance(identity["tag"], str) or not TAG_RE.fullmatch(identity["tag"]) or not isinstance(identity["commit"], str) or not SHA_RE.fullmatch(identity["commit"]):
            raise ReleaseError("conditional-prefix-scan SDK release identity must contain a tag and full commit SHA")

    runtime = require_exact_keys(value["runtime"], {"feature", "capability"}, "conditional-prefix-scan conformance runtime")
    capability = require_exact_keys(runtime["capability"], {"name", "version", "status"}, "conditional-prefix-scan conformance capability")
    if runtime["feature"] != "storage_conditional_prefix_scan_v1" or capability != {"name": "storage_conditional_prefix_scan", "version": 1, "status": "available"}:
        raise ReleaseError("conditional-prefix-scan conformance runtime contract is invalid")

    expected_limits = {
        "lease_ttl_ms": 300000,
        "max_states": 4096,
        "max_entries": 256,
        "max_request_id_bytes": 128,
        "max_prefix_bytes": 8192,
        "max_key_bytes": 65536,
        "max_request_bytes": 65536,
        "max_value_bytes": 3 * 1024 * 1024,
        "max_page_value_bytes": 8 * 1024 * 1024,
        "max_response_bytes": 16 * 1024 * 1024,
    }
    if value["limits"] != expected_limits:
        raise ReleaseError("conditional-prefix-scan conformance limits do not match v1")
    vector = require_exact_keys(value["digest_vector"], {"request", "response", "sha256"}, "conditional-prefix-scan digest vector")
    if vector["sha256"] != PREFIX_SCAN_V1_DIGEST or conditional_prefix_scan_response_digest(vector["request"], vector["response"]) != vector["sha256"]:
        raise ReleaseError("conditional-prefix-scan digest vector does not match v1")
    evidence = require_exact_keys(value["release_evidence"], {"required_targets", "requirements"}, "conditional-prefix-scan release evidence")
    if evidence["required_targets"] != list(EXPECTED_TARGETS) or evidence["requirements"] != [
        "portable_c_abi_build_manifest_smoke",
        "conditional_prefix_scan_v1_core_conformance",
        "sdk_digest_vector",
        "runtime_native_probe",
    ]:
        raise ReleaseError("conditional-prefix-scan release evidence is incomplete")
    if require_ready and value["status"] != "ready":
        raise ReleaseError("conditional-prefix-scan conformance is gated")
    return value


def target_for(lock: dict[str, Any], platform: str) -> dict[str, Any]:
    matches = [value for value in lock["targets"] if value.get("platform") == platform]
    if len(matches) != 1:
        raise ReleaseError(f"platform {platform!r} is not uniquely declared in the release lock")
    return matches[0]


def git_output(root: Path, *args: str) -> str:
    return run("git", *args, cwd=root).decode().strip()


def validate_source(lock: dict[str, Any], core_root: Path) -> int:
    expected = lock["core"]
    actual_commit = git_output(core_root, "rev-parse", "HEAD")
    if actual_commit != expected["commit"]:
        raise ReleaseError(f"Core HEAD {actual_commit} does not match locked commit {expected['commit']}")
    if git_output(core_root, "status", "--porcelain=v1"):
        raise ReleaseError("Core source worktree is dirty")
    if expected.get("tag"):
        tagged_commit = git_output(core_root, "rev-parse", f"{expected['tag']}^{{commit}}")
        if tagged_commit != expected["commit"]:
            raise ReleaseError(f"Core tag {expected['tag']} does not resolve to the locked commit")
    cargo_toml = (core_root / "Cargo.toml").read_text(encoding="utf-8")
    package_block = cargo_toml.split("[package]", 1)[-1].split("[", 1)[0]
    match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', package_block, re.MULTILINE)
    actual_version = match.group(1) if match else None
    if actual_version != expected["cargo_version"]:
        raise ReleaseError(f"Core Cargo version {actual_version!r} does not match lock")
    if sha256_file(core_root / "Cargo.lock") != expected["cargo_lock_sha256"]:
        raise ReleaseError("Core Cargo.lock hash does not match lock")
    header = core_root / lock["abi"]["header"]
    if sha256_file(header) != lock["abi"]["header_sha256"]:
        raise ReleaseError("Core ABI header hash does not match lock")
    capability = lock["abi"].get("capabilities", {}).get("storage_conditional_batch_v1", {})
    header_text = header.read_text(encoding="utf-8")
    has_conditional_abi = "TALON_STORAGE_CONDITIONAL_BATCH_VERSION" in header_text
    if capability.get("status") == "available" and not has_conditional_abi:
        raise ReleaseError("conditional-batch is marked available but absent from the locked ABI header")
    attestation = lock["runtime_attestation"]
    if attestation["status"] == "ready":
        expected_macros = {
            "TALON_NATIVE_BUILD_MANIFEST_VERSION": attestation["manifest_version"],
            "TALON_NATIVE_ABI_VERSION": attestation["abi_version"],
            "TALON_STORAGE_CONDITIONAL_BATCH_VERSION": RUNTIME_CAPABILITY_CONTRACT["storage_conditional_batch"][0],
            "TALON_STORAGE_CONDITIONAL_TRANSACTION_VERSION": RUNTIME_CAPABILITY_CONTRACT["native_conditional_transaction_v2"][0],
            "TALON_STORAGE_CONDITIONAL_PREFIX_SCAN_VERSION": RUNTIME_CAPABILITY_CONTRACT["storage_conditional_prefix_scan"][0],
            "TALON_REVISION_STREAM_VERSION": RUNTIME_CAPABILITY_CONTRACT["revision_stream"][0],
        }
        for macro, expected_version in expected_macros.items():
            match = re.search(rf"^\s*#\s*define\s+{re.escape(macro)}\s+([0-9]+)[uU]?\s*$", header_text, re.MULTILINE)
            if match is None or int(match.group(1)) != expected_version:
                raise ReleaseError(f"Core ABI header does not declare {macro}={expected_version}")
        for symbol in attestation["required_symbols"]:
            if re.search(rf"\b{re.escape(symbol)}\s*\(", header_text) is None:
                raise ReleaseError(f"Core ABI header does not declare required symbol {symbol}")
        for token in REVISION_STREAM_V2_HEADER_TOKENS:
            if token not in header_text:
                raise ReleaseError(f"Core ABI header omits revision-stream v2 contract token {token}")
    return int(git_output(core_root, "show", "-s", "--format=%ct", "HEAD"))


def compute_build_binding(manifest: dict[str, Any]) -> str:
    digest = hashlib.sha256()

    def bind(value: Any) -> None:
        encoded = str(value).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)

    dirty = manifest["git_dirty"]
    fields = [
        manifest["manifest_version"],
        manifest["core_semver"],
        manifest["git_commit"],
        str(dirty).lower() if isinstance(dirty, bool) else "unknown",
        manifest["target"],
        manifest["cargo_lock_sha256"],
        manifest["header_sha256"],
        manifest["abi"]["profile"],
        manifest["abi"]["version"],
    ]
    for value in fields:
        bind(value)
    for symbol in manifest["abi"]["required_symbols"]:
        bind(symbol)
    for feature in manifest["features"]:
        bind(feature)
    for capability in manifest["capabilities"]:
        bind(capability["name"])
        bind(capability["version"])
        bind(capability["status"])
        bind(capability.get("reason") or "")
    return digest.hexdigest()


def validate_core_build_manifest(
    manifest: dict[str, Any],
    lock: dict[str, Any],
    target: dict[str, Any],
) -> None:
    require_exact_keys(
        manifest,
        {
            "manifest_version",
            "core_semver",
            "git_commit",
            "git_dirty",
            "target",
            "cargo_lock_sha256",
            "header_sha256",
            "abi",
            "features",
            "capabilities",
            "build_binding_sha256",
        },
        "Core build manifest",
    )
    expected_source = lock["core"]
    if (
        type(manifest["manifest_version"]) is not int
        or manifest["core_semver"] != expected_source["cargo_version"]
        or manifest["git_commit"] != expected_source["commit"]
        or manifest["git_dirty"] is not False
        or manifest["target"] != target["triple"]
        or manifest["cargo_lock_sha256"] != expected_source["cargo_lock_sha256"]
        or manifest["header_sha256"] != lock["abi"]["header_sha256"]
    ):
        raise ReleaseError("Core self-manifest source/build identity does not match the release lock")
    contract = lock["runtime_attestation"]
    if manifest["manifest_version"] != contract["manifest_version"]:
        raise ReleaseError("Core self-manifest version does not match the release lock")
    abi = require_exact_keys(manifest["abi"], {"profile", "version", "required_symbols"}, "Core build manifest ABI")
    if (
        abi["profile"] != contract["abi_profile"]
        or type(abi["version"]) is not int
        or abi["version"] != contract["abi_version"]
        or abi["required_symbols"] != contract["required_symbols"]
    ):
        raise ReleaseError("Core self-manifest ABI does not match the release lock")
    features = manifest["features"]
    if features != contract["features"]:
        raise ReleaseError("Core self-manifest features do not match the release lock")
    actual_capabilities = manifest["capabilities"]
    if not isinstance(actual_capabilities, list) or len(actual_capabilities) != len(contract["capabilities"]):
        raise ReleaseError("Core self-manifest capabilities do not match the release lock")
    normalized_capabilities = []
    for index, raw in enumerate(actual_capabilities):
        if not isinstance(raw, dict):
            raise ReleaseError(f"Core self-manifest capability {index} must be an object")
        unknown = set(raw) - {"name", "version", "status", "reason"}
        missing = {"name", "version", "status"} - set(raw)
        if unknown or missing:
            raise ReleaseError(
                f"Core self-manifest capability {index} fields differ: missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        if type(raw["version"]) is not int:
            raise ReleaseError(f"Core self-manifest capability {index} version must be an integer")
        normalized_capabilities.append({**raw, "reason": raw.get("reason")})
    if normalized_capabilities != contract["capabilities"]:
        raise ReleaseError("Core self-manifest capability versions or gates do not match the release lock")
    binding = manifest["build_binding_sha256"]
    if not isinstance(binding, str) or not HASH_RE.fullmatch(binding) or compute_build_binding(manifest) != binding:
        raise ReleaseError("Core self-manifest build_binding_sha256 is invalid")


def public_key_fingerprint(public_key: Path) -> str:
    der = run("openssl", "pkey", "-pubin", "-in", str(public_key), "-outform", "DER")
    return sha256_bytes(der)


def make_tar(entries: list[tuple[Path, str]], output: Path, epoch: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for source, name in sorted(entries, key=lambda pair: pair[1]):
            data = source.read_bytes()
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = epoch
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            zipped.write(stream.getvalue())


def component_purl(package: dict[str, Any]) -> str:
    name = str(package.get("name", "unknown")).replace(" ", "%20")
    version = str(package.get("version", "unknown"))
    return f"pkg:cargo/{name}@{version}"


def build_sbom(metadata: dict[str, Any], core_commit: str) -> dict[str, Any]:
    components = []
    for package in sorted(metadata.get("packages", []), key=lambda p: (p.get("name", ""), p.get("version", ""), p.get("id", ""))):
        component: dict[str, Any] = {
            "type": "library",
            "bom-ref": component_purl(package),
            "name": package.get("name"),
            "version": package.get("version"),
            "purl": component_purl(package),
        }
        if package.get("license"):
            component["licenses"] = [{"expression": package["license"]}]
        if package.get("source"):
            component["externalReferences"] = [{"type": "distribution", "url": package["source"]}]
        components.append(component)
    root_package = next(
        (package for package in metadata.get("packages", []) if package.get("name") == "talon"),
        None,
    )
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "component": {
                "type": "library",
                "name": "talon-core",
                "version": root_package.get("version", "unknown") if root_package else "unknown",
                "properties": [{"name": "talon:core-git-commit", "value": core_commit}],
            }
        },
        "components": components,
    }


def build_license_inventory(metadata: dict[str, Any]) -> dict[str, Any]:
    packages = []
    for package in sorted(metadata.get("packages", []), key=lambda p: (p.get("name", ""), p.get("version", ""), p.get("id", ""))):
        license_file = package.get("license_file")
        packages.append({
            "name": package.get("name"),
            "version": package.get("version"),
            "source": package.get("source"),
            "license_expression": package.get("license"),
            "license_file": Path(license_file).name if license_file else None,
        })
    return {"schema_version": "1.0", "packages": packages}


def file_record(path: Path, name: str | None = None) -> dict[str, Any]:
    return {"path": name or path.name, "size": path.stat().st_size, "sha256": sha256_file(path)}


def command_build(args: argparse.Namespace) -> None:
    lock = load_json(args.lock)
    validate_lock(lock, allow_gated=False)
    validate_prefix_scan_v1_conformance(
        getattr(args, "prefix_scan_conformance", PREFIX_SCAN_V1_CONFORMANCE_PATH),
        require_ready=True,
        expected_core=lock["core"],
    )
    epoch = validate_source(lock, args.core_root)
    target = target_for(lock, args.platform)
    core_build_manifest = load_json(args.core_build_manifest)
    validate_core_build_manifest(core_build_manifest, lock, target)
    actual_bin_commit = git_output(args.talon_bin_root, "rev-parse", "HEAD")
    if git_output(args.talon_bin_root, "status", "--porcelain=v1"):
        raise ReleaseError("talon-bin source worktree is dirty")
    metadata = load_json(args.cargo_metadata)
    public_fingerprint = public_key_fingerprint(args.public_key)
    pinned_fingerprint = lock["signing"].get("public_key_sha256")
    if pinned_fingerprint and public_fingerprint != pinned_fingerprint:
        raise ReleaseError("signing public key fingerprint does not match release lock")

    args.output.mkdir(parents=True, exist_ok=True)
    required_names = [target["static_library"], target["dynamic_library"], "talon.h", "LICENSE.core", "NOTICE"]
    entries: list[tuple[Path, str]] = []
    for name in required_names:
        source = args.stage / name
        if not source.is_file() or source.stat().st_size == 0:
            raise ReleaseError(f"required staged file is missing or empty: {name}")
        entries.append((source, name))

    archive = args.output / f"libtalon-core-{args.platform}.tar.gz"
    make_tar(entries, archive, epoch)
    sbom_path = args.output / f"libtalon-core-{args.platform}.sbom.cdx.json"
    sbom_path.write_bytes(canonical_json(build_sbom(metadata, lock["core"]["commit"])))
    licenses_path = args.output / f"libtalon-core-{args.platform}.licenses.json"
    licenses_path.write_bytes(canonical_json(build_license_inventory(metadata)))
    core_license_path = args.output / "LICENSE.core"
    core_license_path.write_bytes((args.stage / "LICENSE.core").read_bytes())
    notice_path = args.output / "NOTICE"
    notice_path.write_bytes((args.stage / "NOTICE").read_bytes())
    manifest_path = args.output / f"libtalon-core-{args.platform}.manifest.json"
    manifest = {
        "schema_version": "1.0",
        "release": {
            "tag": lock["release_tag"],
            "channel": lock["channel"],
            "talon_bin_commit": actual_bin_commit,
        },
        "source": {
            "repository": lock["core"]["repository"],
            "tag": lock["core"].get("tag"),
            "commit": lock["core"]["commit"],
            "cargo_version": lock["core"]["cargo_version"],
            "cargo_lock_sha256": lock["core"]["cargo_lock_sha256"],
            "source_date_epoch": epoch,
            "clean": True,
        },
        "abi": {
            "profile": lock["abi"]["profile"],
            "version": lock["abi"]["version"],
            "header_sha256": lock["abi"]["header_sha256"],
            "required_symbols": lock["abi"]["required_symbols"],
        },
        "build": {
            "source": args.build_source,
            "workflow_run_id": args.workflow_run_id,
            "runner": args.runner,
            "target": target["triple"],
            "rustc": args.rustc,
            "cargo": args.cargo,
            "command": lock["build"]["cargo_args"],
            "reproducibility": lock["build"]["reproducibility"],
        },
        "artifact": {
            "kind": "talon-core-native-library",
            "platform": args.platform,
            "archive": file_record(archive),
            "files": [file_record(source, name) for source, name in sorted(entries, key=lambda pair: pair[1])],
        },
        "materials": {
            "sbom": file_record(sbom_path),
            "license_inventory": file_record(licenses_path),
            "core_license": file_record(core_license_path),
            "notice": file_record(notice_path),
        },
        "signing": {
            "algorithm": "Ed25519",
            "key_id": lock["signing"]["key_id"],
            "public_key_sha256": public_fingerprint,
            "signature": manifest_path.name + ".sig",
        },
        "compatibility": {
            "sdk_contract": "talon-native-manifest-v1",
            "runtime_verification": "ed25519-manifest+sha256-native+abi-identity",
            "binary_self_attestation": True,
        },
        "gates": lock["abi"].get("capabilities", {}),
    }
    validate_manifest(manifest)
    manifest_path.write_bytes(canonical_json(manifest))
    print(manifest_path)


def command_sign(args: argparse.Namespace) -> None:
    signature_path = args.signature.resolve()
    if signature_path in {args.manifest.resolve(), args.private_key.resolve()}:
        raise ReleaseError("signature output must not overwrite the manifest or private key")
    expected_name = args.manifest.name + ".sig"
    if args.signature.name != expected_name:
        raise ReleaseError(f"signature output filename must be {expected_name}")
    run("openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(args.private_key), "-in", str(args.manifest), "-out", str(args.signature))


def safe_tar_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    result = []
    names: set[str] = set()
    for member in archive.getmembers():
        path = Path(member.name)
        if member.name.startswith("/") or ".." in path.parts or not member.isfile():
            raise ReleaseError(f"unsafe or unsupported archive member {member.name!r}")
        if member.name in names:
            raise ReleaseError(f"duplicate archive member {member.name!r}")
        names.add(member.name)
        result.append(member)
    return result


def require_exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReleaseError(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        raise ReleaseError(f"{label} fields differ: missing={sorted(expected - actual)}, unknown={sorted(actual - expected)}")
    return value


def validate_file_record(value: Any, label: str) -> None:
    record = require_exact_keys(value, {"path", "size", "sha256"}, label)
    path = record["path"]
    if (
        not isinstance(path, str)
        or path != Path(path).name
        or path in ("", ".", "..")
        or not FILE_NAME_RE.fullmatch(path)
    ):
        raise ReleaseError(f"{label}.path must be a safe basename")
    if type(record["size"]) is not int or record["size"] <= 0:
        raise ReleaseError(f"{label}.size must be positive")
    if not isinstance(record["sha256"], str) or not HASH_RE.fullmatch(record["sha256"]):
        raise ReleaseError(f"{label}.sha256 is invalid")


def validate_manifest(manifest: dict[str, Any]) -> None:
    require_exact_keys(manifest, {"schema_version", "release", "source", "abi", "build", "artifact", "materials", "signing", "compatibility", "gates"}, "manifest")
    if manifest["schema_version"] != "1.0":
        raise ReleaseError("unsupported manifest schema")
    release_info = require_exact_keys(manifest["release"], {"tag", "channel", "talon_bin_commit"}, "release")
    if not isinstance(release_info["tag"], str) or not TAG_RE.fullmatch(release_info["tag"]):
        raise ReleaseError("release.tag must be vX.Y.Z")
    if release_info["channel"] != "enterprise-core" or not SHA_RE.fullmatch(str(release_info["talon_bin_commit"])):
        raise ReleaseError("release channel or talon-bin commit is invalid")
    source = require_exact_keys(manifest["source"], {"repository", "tag", "commit", "cargo_version", "cargo_lock_sha256", "source_date_epoch", "clean"}, "source")
    if (
        source["repository"] != CORE_REPOSITORY
        or not isinstance(source["tag"], str)
        or not TAG_RE.fullmatch(source["tag"])
        or not isinstance(source["cargo_version"], str)
        or not SEMVER_RE.fullmatch(source["cargo_version"])
        or not SHA_RE.fullmatch(str(source["commit"]))
        or not HASH_RE.fullmatch(str(source["cargo_lock_sha256"]))
        or source["clean"] is not True
    ):
        raise ReleaseError("source identity is invalid")
    if type(source["source_date_epoch"]) is not int or source["source_date_epoch"] <= 0:
        raise ReleaseError("source_date_epoch is invalid")
    abi = require_exact_keys(manifest["abi"], {"profile", "version", "header_sha256", "required_symbols"}, "abi")
    if abi["profile"] != "talon-native-c" or type(abi["version"]) is not int or abi["version"] != 1 or not HASH_RE.fullmatch(str(abi["header_sha256"])):
        raise ReleaseError("ABI identity is invalid")
    if (
        not isinstance(abi["required_symbols"], list)
        or any(not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol) for symbol in abi["required_symbols"])
        or len(abi["required_symbols"]) != len(SDK_REQUIRED_SYMBOLS)
        or set(abi["required_symbols"]) != set(SDK_REQUIRED_SYMBOLS)
    ):
        raise ReleaseError("ABI required_symbols does not match the SDK contract")
    build = require_exact_keys(manifest["build"], {"source", "workflow_run_id", "runner", "target", "rustc", "cargo", "command", "reproducibility"}, "build")
    if any(not isinstance(build[name], str) or not build[name] for name in ("source", "workflow_run_id", "runner", "target", "rustc", "cargo", "reproducibility")):
        raise ReleaseError("build identity is incomplete")
    if build["command"] != ["build", "--locked", "--release", "--lib"]:
        raise ReleaseError("build command is not the Enterprise locked build")
    artifact = require_exact_keys(manifest["artifact"], {"kind", "platform", "archive", "files"}, "artifact")
    if artifact["kind"] != "talon-core-native-library":
        raise ReleaseError("artifact kind is not pure Talon Core")
    platform = artifact["platform"]
    if platform not in EXPECTED_TARGETS:
        raise ReleaseError("artifact platform is unsupported")
    target = EXPECTED_TARGETS[platform]
    if build["target"] != target["triple"] or build["runner"] != target["runner"]:
        raise ReleaseError("artifact platform does not match build target and runner")
    validate_file_record(artifact["archive"], "artifact.archive")
    if artifact["archive"]["path"] != f"libtalon-core-{platform}.tar.gz":
        raise ReleaseError("artifact archive filename does not match platform")
    if not isinstance(artifact["files"], list) or len(artifact["files"]) != 5:
        raise ReleaseError("artifact.files is incomplete")
    for index, record in enumerate(artifact["files"]):
        validate_file_record(record, f"artifact.files[{index}]")
    names = [record["path"] for record in artifact["files"]]
    if len(names) != len(set(names)):
        raise ReleaseError("artifact.files contains duplicate paths")
    expected_names = {
        target["static_library"],
        target["dynamic_library"],
        "talon.h",
        "LICENSE.core",
        "NOTICE",
    }
    if set(names) != expected_names:
        raise ReleaseError("artifact.files does not match the platform file contract")
    materials = require_exact_keys(manifest["materials"], {"sbom", "license_inventory", "core_license", "notice"}, "materials")
    for name, record in materials.items():
        validate_file_record(record, f"materials.{name}")
    expected_material_paths = {
        "sbom": f"libtalon-core-{platform}.sbom.cdx.json",
        "license_inventory": f"libtalon-core-{platform}.licenses.json",
        "core_license": "LICENSE.core",
        "notice": "NOTICE",
    }
    if {name: record["path"] for name, record in materials.items()} != expected_material_paths:
        raise ReleaseError("material filenames do not match the platform contract")
    files_by_name = {record["path"]: record for record in artifact["files"]}
    if files_by_name["talon.h"]["sha256"] != abi["header_sha256"]:
        raise ReleaseError("packaged talon.h does not match the ABI header identity")
    if files_by_name["LICENSE.core"] != materials["core_license"]:
        raise ReleaseError("inner and outer Core license records differ")
    if files_by_name["NOTICE"] != materials["notice"]:
        raise ReleaseError("inner and outer NOTICE records differ")
    signing = require_exact_keys(manifest["signing"], {"algorithm", "key_id", "public_key_sha256", "signature"}, "signing")
    if signing["algorithm"] != "Ed25519" or not HASH_RE.fullmatch(str(signing["public_key_sha256"])):
        raise ReleaseError("manifest signing profile is invalid")
    if (
        not isinstance(signing["key_id"], str)
        or not KEY_ID_RE.fullmatch(signing["key_id"])
        or signing["key_id"] == "UNASSIGNED"
        or not isinstance(signing["signature"], str)
        or signing["signature"] != Path(signing["signature"]).name
        or not signing["signature"].endswith(".sig")
    ):
        raise ReleaseError("manifest signing identity is incomplete")
    compatibility = require_exact_keys(manifest["compatibility"], {"sdk_contract", "runtime_verification", "binary_self_attestation"}, "compatibility")
    if (
        compatibility["sdk_contract"] != "talon-native-manifest-v1"
        or compatibility["runtime_verification"] != "ed25519-manifest+sha256-native+abi-identity"
        or compatibility["binary_self_attestation"] is not True
    ):
        raise ReleaseError("SDK compatibility contract is invalid")
    gates = require_exact_keys(manifest["gates"], {"storage_conditional_batch_v1"}, "gates")
    capability = require_exact_keys(gates["storage_conditional_batch_v1"], {"status", "reason"}, "gates.storage_conditional_batch_v1")
    if capability["status"] != "gated" or not isinstance(capability["reason"], str) or not capability["reason"]:
        raise ReleaseError("conditional-batch gate must remain gated for this SDK contract")


def command_verify(args: argparse.Namespace) -> None:
    manifest = load_json(args.manifest)
    validate_manifest(manifest)
    if (
        not TAG_RE.fullmatch(args.expected_release_tag)
        or not SHA_RE.fullmatch(args.expected_talon_bin_commit)
        or args.expected_core_repository != CORE_REPOSITORY
        or not TAG_RE.fullmatch(args.expected_core_tag)
        or not SHA_RE.fullmatch(args.expected_core_commit)
        or not SEMVER_RE.fullmatch(args.expected_core_version)
        or args.expected_abi_profile != "talon-native-c"
        or type(args.expected_abi_version) is not int
        or args.expected_abi_version != 1
        or not HASH_RE.fullmatch(args.expected_header_sha256)
    ):
        raise ReleaseError("trusted release/source/ABI policy is invalid")
    if (
        manifest["release"]["tag"] != args.expected_release_tag
        or manifest["release"]["talon_bin_commit"] != args.expected_talon_bin_commit
        or manifest["source"]["repository"] != args.expected_core_repository
        or manifest["source"]["tag"] != args.expected_core_tag
        or manifest["source"]["commit"] != args.expected_core_commit
        or manifest["source"]["cargo_version"] != args.expected_core_version
        or manifest["abi"]["profile"] != args.expected_abi_profile
        or manifest["abi"]["version"] != args.expected_abi_version
        or manifest["abi"]["header_sha256"] != args.expected_header_sha256
    ):
        raise ReleaseError("manifest release/source/ABI identity does not match trusted policy")
    expected_manifest_name = f"libtalon-core-{manifest['artifact']['platform']}.manifest.json"
    if args.manifest.name != expected_manifest_name:
        raise ReleaseError("manifest filename does not match platform")
    if manifest["signing"]["signature"] != expected_manifest_name + ".sig":
        raise ReleaseError("declared signature filename does not match manifest filename")
    if args.signature.name != manifest["signing"]["signature"]:
        raise ReleaseError("signature filename does not match manifest")
    if not HASH_RE.fullmatch(args.expected_key_sha256):
        raise ReleaseError("expected signing key SHA-256 is invalid")
    if not KEY_ID_RE.fullmatch(args.expected_key_id) or args.expected_key_id == "UNASSIGNED":
        raise ReleaseError("expected signing key ID is invalid")
    if manifest["signing"]["key_id"] != args.expected_key_id:
        raise ReleaseError("manifest signing key ID does not match trusted policy")
    if manifest["signing"]["public_key_sha256"] != args.expected_key_sha256:
        raise ReleaseError("manifest signing key fingerprint does not match trusted policy")
    if public_key_fingerprint(args.public_key) != args.expected_key_sha256:
        raise ReleaseError("trusted public key fingerprint does not match manifest")
    run("openssl", "pkeyutl", "-verify", "-rawin", "-pubin", "-inkey", str(args.public_key), "-in", str(args.manifest), "-sigfile", str(args.signature))
    base = args.manifest.parent
    archive_record = manifest["artifact"]["archive"]
    archive_path = base / archive_record["path"]
    if file_record(archive_path)["sha256"] != archive_record["sha256"] or archive_path.stat().st_size != archive_record["size"]:
        raise ReleaseError("native archive hash or size does not match manifest")
    expected = {item["path"]: item for item in manifest["artifact"]["files"]}
    with tarfile.open(archive_path, "r:gz") as archive:
        members = safe_tar_members(archive)
        if {member.name for member in members} != set(expected):
            raise ReleaseError("native archive member set does not match manifest")
        for member in members:
            data = archive.extractfile(member).read()
            record = expected[member.name]
            if len(data) != record["size"] or sha256_bytes(data) != record["sha256"]:
                raise ReleaseError(f"archive member {member.name} does not match manifest")
    for material in manifest["materials"].values():
        path = base / material["path"]
        if not path.is_file() or path.stat().st_size != material["size"] or sha256_file(path) != material["sha256"]:
            raise ReleaseError(f"material {material['path']} does not match manifest")
    print(f"verified {args.manifest.name}")


def command_offline(args: argparse.Namespace) -> None:
    command_verify(args)
    manifest = load_json(args.manifest)
    base = args.manifest.parent
    paths = [
        base / manifest["artifact"]["archive"]["path"],
        args.manifest,
        args.signature,
        args.public_key,
    ]
    paths.extend(base / record["path"] for record in manifest["materials"].values())
    unique: dict[str, Path] = {}
    for path in paths:
        if not FILE_NAME_RE.fullmatch(path.name):
            raise ReleaseError(f"offline bundle input has unsafe filename: {path.name!r}")
        if path.name in unique:
            raise ReleaseError(f"offline bundle has duplicate basename: {path.name}")
        unique[path.name] = path
    for name, path in unique.items():
        if not path.is_file():
            raise ReleaseError(f"offline bundle input is missing: {name}")
    if args.output.resolve() in {path.resolve() for path in unique.values()}:
        raise ReleaseError("offline bundle output must not overwrite a verified input")
    epoch = int(manifest["source"]["source_date_epoch"])
    make_tar([(path, name) for name, path in unique.items()], args.output, epoch)
    print(args.output)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate-lock")
    validate.add_argument("--lock", type=Path, required=True)
    validate.add_argument("--allow-gated", action="store_true")
    validate.set_defaults(func=lambda args: validate_lock(load_json(args.lock), args.allow_gated))

    conformance = commands.add_parser("validate-prefix-scan-conformance")
    conformance.add_argument("--path", type=Path, default=PREFIX_SCAN_V1_CONFORMANCE_PATH)
    conformance.add_argument("--require-ready", action="store_true")
    conformance.set_defaults(func=lambda args: validate_prefix_scan_v1_conformance(args.path, require_ready=args.require_ready))

    build = commands.add_parser("build")
    build.add_argument("--lock", type=Path, required=True)
    build.add_argument("--core-root", type=Path, required=True)
    build.add_argument("--talon-bin-root", type=Path, required=True)
    build.add_argument("--stage", type=Path, required=True)
    build.add_argument("--cargo-metadata", type=Path, required=True)
    build.add_argument("--core-build-manifest", type=Path, required=True)
    build.add_argument("--public-key", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--platform", required=True)
    build.add_argument("--build-source", required=True)
    build.add_argument("--workflow-run-id", required=True)
    build.add_argument("--runner", required=True)
    build.add_argument("--rustc", required=True)
    build.add_argument("--cargo", required=True)
    build.add_argument("--prefix-scan-conformance", type=Path, default=PREFIX_SCAN_V1_CONFORMANCE_PATH)
    build.set_defaults(func=command_build)

    sign = commands.add_parser("sign")
    sign.add_argument("--manifest", type=Path, required=True)
    sign.add_argument("--private-key", type=Path, required=True)
    sign.add_argument("--signature", type=Path, required=True)
    sign.set_defaults(func=command_sign)

    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--signature", type=Path, required=True)
    verify.add_argument("--public-key", type=Path, required=True)
    verify.add_argument("--expected-key-id", required=True)
    verify.add_argument("--expected-key-sha256", required=True)
    add_trusted_identity_arguments(verify)
    verify.set_defaults(func=command_verify)

    offline = commands.add_parser("offline")
    offline.add_argument("--manifest", type=Path, required=True)
    offline.add_argument("--signature", type=Path, required=True)
    offline.add_argument("--public-key", type=Path, required=True)
    offline.add_argument("--expected-key-id", required=True)
    offline.add_argument("--expected-key-sha256", required=True)
    add_trusted_identity_arguments(offline)
    offline.add_argument("--output", type=Path, required=True)
    offline.set_defaults(func=command_offline)
    return root


def add_trusted_identity_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--expected-release-tag", required=True)
    command.add_argument("--expected-talon-bin-commit", required=True)
    command.add_argument("--expected-core-repository", required=True)
    command.add_argument("--expected-core-tag", required=True)
    command.add_argument("--expected-core-commit", required=True)
    command.add_argument("--expected-core-version", required=True)
    command.add_argument("--expected-abi-profile", required=True)
    command.add_argument("--expected-abi-version", type=int, required=True)
    command.add_argument("--expected-header-sha256", required=True)


def main() -> int:
    args = parser().parse_args()
    try:
        args.func(args)
    except ReleaseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
