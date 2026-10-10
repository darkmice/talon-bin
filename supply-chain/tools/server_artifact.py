#!/usr/bin/env python3
"""Build/verify a separately signed HTTP server archive using the native trust lock."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile

import release


def digest(data):
    return hashlib.sha256(data).hexdigest()


def identity(lock, commit, platform):
    return {"schema_version": 1, "kind": "talon-core-http-server", "release_tag": lock["release_tag"],
            "talon_bin_commit": commit, "core": lock["core"], "platform": platform,
            "target": release.EXPECTED_TARGETS[platform]["triple"], "signing": lock["signing"]}


def build(args, lock):
    release.validate_lock(lock, allow_gated=False)
    release.validate_source(lock, args.core_root)
    if not release.SHA_RE.fullmatch(args.talon_bin_commit):
        raise release.ReleaseError("invalid talon-bin source identity")
    if release.public_key_fingerprint(args.public_key) != lock["signing"]["public_key_sha256"]:
        raise release.ReleaseError("server signing key differs from pinned trust anchor")
    epoch = int(release.git_output(args.core_root, "show", "-s", "--format=%ct", "HEAD"))
    entries = {"talon": args.binary.read_bytes(), "LICENSE.core": (args.core_root / "LICENSE").read_bytes(),
               "NOTICE": args.notice.read_bytes()}
    if not entries["talon"]:
        raise release.ReleaseError("empty server binary")
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, data in sorted(entries.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mtime, member.mode = len(data), epoch, 0o755 if name == "talon" else 0o644
            archive.addfile(member, io.BytesIO(data))
    archive_bytes = gzip.compress(stream.getvalue(), mtime=epoch)
    prefix = "talon-core-server-" + args.platform
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / (prefix + ".tar.gz")).write_bytes(archive_bytes)
    manifest = identity(lock, args.talon_bin_commit, args.platform)
    manifest["build"] = {"command": ["cargo", "build", "--locked", "--release", "--lib", "--bin", "talon"],
                         "rust_toolchain": lock["build"]["rust_toolchain"], "workflow_run_id": args.run_id}
    manifest["archive"] = {"path": prefix + ".tar.gz", "sha256": digest(archive_bytes), "size": len(archive_bytes)}
    manifest["files"] = [{"path": name, "sha256": digest(data), "size": len(data)} for name, data in sorted(entries.items())]
    path = args.output / (prefix + ".manifest.json")
    path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    print(path)


def verify(args, lock):
    release.validate_lock(lock, allow_gated=False)
    manifest_bytes = args.manifest.read_bytes()
    manifest = release.load_json(args.manifest)
    expected = identity(lock, args.talon_bin_commit, args.platform)
    if set(manifest) != set(expected) | {"archive", "files", "build"} or any(manifest[k] != v for k, v in expected.items()):
        raise release.ReleaseError("server manifest identity differs from trusted release policy")
    if release.public_key_fingerprint(args.public_key) != lock["signing"]["public_key_sha256"]:
        raise release.ReleaseError("untrusted server signing key")
    release.run("openssl", "pkeyutl", "-verify", "-pubin", "-inkey", str(args.public_key),
                "-rawin", "-in", str(args.manifest), "-sigfile", str(args.signature))
    build_record = manifest["build"]
    if set(build_record) != {"command", "rust_toolchain", "workflow_run_id"} or build_record["command"] != ["cargo", "build", "--locked", "--release", "--lib", "--bin", "talon"] or build_record["rust_toolchain"] != lock["build"]["rust_toolchain"] or not str(build_record["workflow_run_id"]).isdigit():
        raise release.ReleaseError("server build identity differs from the release contract")
    archive = manifest["archive"]
    prefix = "talon-core-server-" + args.platform
    if set(archive) != {"path", "sha256", "size"} or archive["path"] != prefix + ".tar.gz":
        raise release.ReleaseError("unexpected server archive name")
    data = (args.manifest.parent / archive["path"]).read_bytes()
    if digest(data) != archive["sha256"] or len(data) != archive["size"]:
        raise release.ReleaseError("server archive checksum mismatch")
    records = manifest["files"]
    if len(records) != 3 or {r["path"] for r in records} != {"talon", "LICENSE.core", "NOTICE"}:
        raise release.ReleaseError("server member set differs")
    expected_files = {r["path"]: r for r in records}
    seen = set()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar:
            if not member.isfile() or member.name not in expected_files or member.name in seen:
                raise release.ReleaseError("unsafe or duplicate server archive member")
            seen.add(member.name)
            record = expected_files[member.name]
            if set(record) != {"path", "sha256", "size"} or member.size != record["size"]:
                raise release.ReleaseError("server member size differs")
            payload = tar.extractfile(member).read()
            if digest(payload) != record["sha256"]:
                raise release.ReleaseError("server member digest differs")
            if member.name == "talon" and member.mode != 0o755:
                raise release.ReleaseError("server is not executable")
    if seen != set(expected_files):
        raise release.ReleaseError("missing server members")
    print("verified signed HTTP server", args.platform, digest(manifest_bytes))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--talon-bin-commit", required=True)
    parser.add_argument("--platform", choices=release.EXPECTED_TARGETS, required=True)
    parser.add_argument("--public-key", type=Path, required=True)
    for name in ("core-root", "binary", "notice", "output", "manifest", "signature"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    lock = release.load_json(args.lock)
    (build if args.command == "build" else verify)(args, lock)


if __name__ == "__main__":
    main()
