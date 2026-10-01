#!/usr/bin/env python3
"""Stage a versioned Go module from four verified signed runtime bundles.

This command does not publish or tag the module. A release operator must review
the staged bytes and create the immutable module commit and tag separately.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil

import release


def package(args: argparse.Namespace) -> None:
    lock = release.load_json(args.lock)
    release.validate_lock(lock, allow_gated=False)
    if not release.SHA_RE.fullmatch(args.talon_bin_commit):
        raise release.ReleaseError("talon-bin commit must be a full lowercase SHA")
    if args.output.exists():
        raise release.ReleaseError("Go module output already exists")
    public_key = args.signed_root / "macos-arm64" / "talon-release-public.pem"
    if release.public_key_fingerprint(public_key) != lock["signing"]["public_key_sha256"]:
        raise release.ReleaseError("packaged public key does not match trusted release lock")

    sources: dict[str, list[Path]] = {}
    for platform in release.EXPECTED_TARGETS:
        base = args.signed_root / platform
        prefix = f"libtalon-core-runtime-{platform}"
        manifest = base / f"{prefix}.manifest.json"
        signature = base / f"{prefix}.manifest.json.sig"
        release.command_verify(argparse.Namespace(
            manifest=manifest,
            signature=signature,
            public_key=public_key,
            expected_key_id=lock["signing"]["key_id"],
            expected_key_sha256=lock["signing"]["public_key_sha256"],
            expected_release_tag=lock["release_tag"],
            expected_talon_bin_commit=args.talon_bin_commit,
            expected_core_repository=lock["core"]["repository"],
            expected_core_tag=lock["core"]["tag"],
            expected_core_commit=lock["core"]["commit"],
            expected_core_version=lock["core"]["cargo_version"],
            expected_abi_profile=lock["abi"]["profile"],
            expected_abi_version=lock["abi"]["version"],
            expected_header_sha256=lock["abi"]["header_sha256"],
        ))
        value = release.load_json(manifest)
        if value["artifact"]["kind"] != "talon-core-native-runtime" or value["artifact"]["platform"] != platform:
            raise release.ReleaseError("Go module requires exact runtime-only artifact for every platform")
        names = [f"{prefix}.tar.gz", f"{prefix}.manifest.json", f"{prefix}.manifest.json.sig", f"{prefix}.sbom.cdx.json", f"{prefix}.licenses.json", "LICENSE.core", "NOTICE"]
        sources[platform] = [base / name for name in names]

    args.output.mkdir(parents=True)
    for name in ("go.mod", "runtime.go"):
        shutil.copyfile(args.template / name, args.output / name)
    for source in sorted(args.template.glob("assets_*.go")):
        shutil.copyfile(source, args.output / source.name)
    shutil.copyfile(args.license, args.output / "LICENSE")
    identity = {
        "status": "ready",
        "release_tag": lock["release_tag"],
        "talon_bin_commit": args.talon_bin_commit,
        "core_repository": lock["core"]["repository"],
        "core_tag": lock["core"]["tag"],
        "core_commit": lock["core"]["commit"],
        "core_version": lock["core"]["cargo_version"],
        "abi_profile": lock["abi"]["profile"],
        "abi_version": lock["abi"]["version"],
        "header_sha256": lock["abi"]["header_sha256"],
        "key_id": lock["signing"]["key_id"],
        "key_sha256": lock["signing"]["public_key_sha256"],
        "public_key_pem": public_key.read_text(encoding="ascii"),
    }
    (args.output / "identity.json").write_bytes(release.canonical_json(identity))
    for platform, files in sources.items():
        destination = args.output / "native" / platform
        destination.mkdir(parents=True)
        for source in files:
            shutil.copyfile(source, destination / source.name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--talon-bin-commit", required=True)
    parser.add_argument("--signed-root", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--license", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    package(parser.parse_args())


if __name__ == "__main__":
    main()
