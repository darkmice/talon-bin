#!/usr/bin/env python3
"""Verify one completed four-target run and stage its exact release assets.

This command reads signed artifacts and writes a new staging directory. It does
not publish a release, create a tag, or modify a repository.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil

import release


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def review(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise release.ReleaseError("release staging directory already exists")
    if not release.SHA_RE.fullmatch(args.talon_bin_commit):
        raise release.ReleaseError("talon-bin source commit must be a full lowercase SHA")
    lock = release.load_json(args.lock)
    release.validate_lock(lock, allow_gated=False)
    release.validate_prefix_scan_v1_conformance(
        args.conformance, require_ready=True, expected_core=lock["core"]
    )
    module = args.artifacts / "talon-core-go-runtime-release"
    identity = release.load_json(module / "identity.json")
    expected_identity = {
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
    }
    for name, expected in expected_identity.items():
        if identity.get(name) != expected:
            raise release.ReleaseError(f"Go runtime identity {name} does not match the release lock")

    platforms = tuple(release.EXPECTED_TARGETS)
    shared = ("LICENSE.core", "NOTICE", "talon-release-public.pem")
    first = args.artifacts / f"talon-enterprise-core-{platforms[0]}"
    public_key = first / "talon-release-public.pem"
    if identity.get("public_key_pem") != public_key.read_text(encoding="ascii"):
        raise release.ReleaseError("Go runtime public key differs from the signed artifact key")
    for platform in platforms:
        directory = args.artifacts / f"talon-enterprise-core-{platform}"
        if not directory.is_dir():
            raise release.ReleaseError(f"missing platform artifact {platform}")
        for name in shared:
            if sha256(directory / name) != sha256(first / name):
                raise release.ReleaseError(f"shared file {name} differs on {platform}")
        checksums = (directory / "SHA256SUMS.txt").read_text(encoding="ascii").splitlines()
        checked = set()
        for line in checksums:
            digest, name = line.split(maxsplit=1)
            name = name.removeprefix("./")
            if not release.FILE_NAME_RE.fullmatch(name) or name in checked:
                raise release.ReleaseError(f"unsafe or duplicate checksum member {name!r}")
            if sha256(directory / name) != digest:
                raise release.ReleaseError(f"artifact checksum mismatch for {platform}/{name}")
            checked.add(name)
        actual = {path.name for path in directory.iterdir() if path.is_file() and path.name != "SHA256SUMS.txt"}
        if checked != actual:
            raise release.ReleaseError(f"artifact checksums do not cover exactly {platform}")

        for profile in ("full", "runtime"):
            prefix = f"libtalon-core-{platform}" if profile == "full" else f"libtalon-core-runtime-{platform}"
            manifest = directory / f"{prefix}.manifest.json"
            release.command_verify(argparse.Namespace(
                manifest=manifest,
                signature=directory / f"{prefix}.manifest.json.sig",
                public_key=directory / "talon-release-public.pem",
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
            if profile == "runtime":
                native_dir = module / "native" / platform
                expected_names = {
                    prefix + suffix for suffix in (
                        ".tar.gz", ".manifest.json", ".manifest.json.sig",
                        ".sbom.cdx.json", ".licenses.json",
                    )
                } | {"LICENSE.core", "NOTICE"}
                actual_names = {path.name for path in native_dir.iterdir() if path.is_file()}
                if actual_names != expected_names:
                    raise release.ReleaseError(f"Go module native members differ on {platform}")
                for name in expected_names:
                    if sha256(native_dir / name) != sha256(directory / name):
                        raise release.ReleaseError(f"Go module member {platform}/{name} differs from signed artifact")

    args.output.mkdir(parents=True)
    for name in shared:
        shutil.copyfile(first / name, args.output / name)
    for platform in platforms:
        directory = args.artifacts / f"talon-enterprise-core-{platform}"
        for path in directory.iterdir():
            if path.name in shared or path.name == "SHA256SUMS.txt":
                continue
            if not path.is_file() or (args.output / path.name).exists():
                raise release.ReleaseError(f"unsafe or duplicate release asset {path.name}")
            shutil.copyfile(path, args.output / path.name)
    lines = [f"{sha256(path)}  {path.name}" for path in sorted(args.output.iterdir())]
    (args.output / "SHA256SUMS.txt").write_text("\n".join(lines) + "\n", encoding="ascii")
    print(f"reviewed {len(lines)} signed release assets")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--conformance", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--talon-bin-commit", required=True)
    review(parser.parse_args())


if __name__ == "__main__":
    main()
