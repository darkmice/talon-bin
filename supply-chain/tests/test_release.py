from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import jsonschema


MODULE_PATH = Path(__file__).parents[1] / "tools" / "release.py"
SPEC = importlib.util.spec_from_file_location("talon_release", MODULE_PATH)
release = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(release)


def command(*argv: str, cwd: Path | None = None) -> str:
    return subprocess.run(argv, cwd=cwd, check=True, stdout=subprocess.PIPE, text=True).stdout.strip()


class ReleaseSupplyChainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.core = self.root / "core"
        self.bin = self.root / "bin"
        self.stage = self.root / "stage"
        self.output = self.root / "output"
        for path in (self.core / "include", self.bin, self.stage, self.output):
            path.mkdir(parents=True, exist_ok=True)
        (self.core / "Cargo.toml").write_text('[package]\nname = "talon"\nversion = "1.2.3"\n', encoding="utf-8")
        (self.core / "Cargo.lock").write_text("# locked\n", encoding="utf-8")
        (self.core / "include" / "talon.h").write_text(
            "\n".join([
                "#define TALON_STORAGE_CONDITIONAL_BATCH_VERSION 1u",
                "#define TALON_STORAGE_CONDITIONAL_TRANSACTION_VERSION 2u",
                "#define TALON_STORAGE_CONDITIONAL_PREFIX_SCAN_VERSION 1u",
                "#define TALON_REVISION_STREAM_VERSION 2u",
                "#define TALON_NATIVE_ABI_VERSION 1u",
                "#define TALON_NATIVE_BUILD_MANIFEST_VERSION 1u",
                *(f"/* {token} */" for token in release.REVISION_STREAM_V2_HEADER_TOKENS),
                *(f"int {symbol}(void);" for symbol in release.SDK_REQUIRED_SYMBOLS),
                "",
            ]),
            encoding="utf-8",
        )
        (self.bin / "README.md").write_text("fixture\n", encoding="utf-8")
        for repo in (self.core, self.bin):
            command("git", "init", "-q", cwd=repo)
            command("git", "config", "user.name", "test", cwd=repo)
            command("git", "config", "user.email", "test@example.invalid", cwd=repo)
            command("git", "add", "-A", cwd=repo)
            command("git", "commit", "-qm", "fixture", cwd=repo)
        self.core_commit = command("git", "rev-parse", "HEAD", cwd=self.core)
        self.bin_commit = command("git", "rev-parse", "HEAD", cwd=self.bin)
        command("git", "tag", "v1.2.3", cwd=self.core)
        self.private_key = self.root / "private.pem"
        self.public_key = self.root / "public.pem"
        command("openssl", "genpkey", "-algorithm", "ED25519", "-out", str(self.private_key))
        command("openssl", "pkey", "-in", str(self.private_key), "-pubout", "-out", str(self.public_key))
        self.public_fingerprint = release.public_key_fingerprint(self.public_key)
        for name, data in {
            "libtalon.a": b"static-library",
            "libtalon.so": b"dynamic-library",
            "libtalon.dylib": b"dynamic-library",
            "talon.h": (self.core / "include" / "talon.h").read_bytes(),
            "LICENSE.core": b"fixture core license\n",
            "NOTICE": b"fixture notice\n",
        }.items():
            (self.stage / name).write_bytes(data)
        self.metadata = self.root / "metadata.json"
        self.metadata.write_text(json.dumps({"packages": [{
            "id": "path+file:///core#talon@1.2.3",
            "name": "talon",
            "version": "1.2.3",
            "license": None,
            "license_file": "LICENSE",
            "source": None,
        }]}), encoding="utf-8")
        lock = {
            "schema_version": "1.0",
            "release_tag": "v1.2.3",
            "channel": "enterprise-core",
            "talon_bin_source": "workflow HEAD",
            "core": {
                "repository": release.CORE_REPOSITORY,
                "tag": "v1.2.3",
                "commit": self.core_commit,
                "cargo_version": "1.2.3",
                "cargo_lock_sha256": release.sha256_file(self.core / "Cargo.lock"),
            },
            "abi": {
                "profile": "talon-native-c",
                "version": 1,
                "header": "include/talon.h",
                "header_sha256": release.sha256_file(self.core / "include" / "talon.h"),
                "required_symbols": release.SDK_REQUIRED_SYMBOLS,
                "capabilities": {"storage_conditional_batch_v1": {"status": "gated", "reason": "fixture"}},
            },
            "runtime_attestation": {
                "status": "ready",
                "reason": "fixture candidate is challenged before packaging",
                "manifest_version": 1,
                "abi_profile": "talon-native-c",
                "abi_version": 1,
                "required_symbols": release.SDK_REQUIRED_SYMBOLS,
                "features": [
                    "native_build_manifest_v1",
                    "native_error_codes_v1",
                    "sql_tlv_v1",
                    "storage_conditional_batch_v1",
                    "native_conditional_transaction_v2",
                    "storage_conditional_prefix_scan_v1",
                    "revision_stream_v1",
                    "revision_stream_v2_mmr_proof",
                ],
                "capabilities": [
                    {"name": "storage_conditional_batch", "version": 1, "status": "available", "reason": None},
                    {"name": "native_conditional_transaction_v2", "version": 2, "status": "available", "reason": None},
                    {
                        "name": "storage_conditional_prefix_scan",
                        "version": 1,
                        "status": "gated",
                        "reason": release.PREFIX_SCAN_V1_GATED_REASON,
                    },
                    {
                        "name": "revision_stream",
                        "version": 2,
                        "status": "gated",
                        "reason": release.REVISION_STREAM_V2_GATED_REASON,
                    },
                    {"name": "native_quorum", "version": 1, "status": "gated", "reason": "fixture quorum scope is incomplete"},
                    {"name": "server_ha_production_admission", "version": 1, "status": "gated", "reason": "fixture HA evidence is incomplete"},
                ],
            },
            "build": {
                "rust_toolchain": "1.92.0",
                "cargo_args": ["build", "--locked", "--release", "--lib"],
                "reproducibility": "fixture",
            },
            "signing": {
                "status": "ready",
                "algorithm": "Ed25519",
                "key_id": "fixture-key",
                "public_key_sha256": self.public_fingerprint,
                "reason": "fixture signing identity",
            },
            "targets": [
                {"platform": platform, **target}
                for platform, target in release.EXPECTED_TARGETS.items()
            ],
        }
        self.lock = self.root / "lock.json"
        self.lock.write_bytes(release.canonical_json(lock))
        conformance = release.load_json(release.PREFIX_SCAN_V1_CONFORMANCE_PATH)
        conformance["status"] = "ready"
        conformance["reason"] = None
        conformance["source"]["release_identity"] = {"tag": "v1.2.3", "commit": self.core_commit}
        conformance["sdk"]["release_identity"] = {"tag": "v1.2.3", "commit": self.bin_commit}
        self.prefix_scan_conformance = self.root / "conditional-prefix-scan-v1.json"
        self.prefix_scan_conformance.write_bytes(release.canonical_json(conformance))
        build_manifest = {
            "manifest_version": 1,
            "core_semver": "1.2.3",
            "git_commit": self.core_commit,
            "git_dirty": False,
            "target": "x86_64-unknown-linux-gnu",
            "cargo_lock_sha256": lock["core"]["cargo_lock_sha256"],
            "header_sha256": lock["abi"]["header_sha256"],
            "abi": {
                "profile": lock["runtime_attestation"]["abi_profile"],
                "version": lock["runtime_attestation"]["abi_version"],
                "required_symbols": lock["runtime_attestation"]["required_symbols"],
            },
            "features": lock["runtime_attestation"]["features"],
            "capabilities": [
                {key: value for key, value in capability.items() if value is not None}
                for capability in lock["runtime_attestation"]["capabilities"]
            ],
            "build_binding_sha256": "",
        }
        build_manifest["build_binding_sha256"] = release.compute_build_binding(build_manifest)
        self.core_build_manifest = self.root / "core-build-manifest.json"
        self.core_build_manifest.write_bytes(release.canonical_json(build_manifest))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def build(self, platform: str = "linux-amd64") -> Path:
        target = release.EXPECTED_TARGETS[platform]
        build_manifest = release.load_json(self.core_build_manifest)
        build_manifest["target"] = target["triple"]
        build_manifest["build_binding_sha256"] = release.compute_build_binding(build_manifest)
        self.core_build_manifest.write_bytes(release.canonical_json(build_manifest))
        args = argparse.Namespace(
            lock=self.lock,
            core_root=self.core,
            talon_bin_root=self.bin,
            stage=self.stage,
            cargo_metadata=self.metadata,
            core_build_manifest=self.core_build_manifest,
            public_key=self.public_key,
            output=self.output,
            platform=platform,
            build_source="test",
            workflow_run_id="1",
            runner=target["runner"],
            rustc="rustc fixture",
            cargo="cargo fixture",
            prefix_scan_conformance=self.prefix_scan_conformance,
        )
        release.command_build(args)
        return self.output / f"libtalon-core-{platform}.manifest.json"

    def commit_core_header_change(self, old: str, new: str) -> dict:
        header = self.core / "include" / "talon.h"
        text = header.read_text(encoding="utf-8")
        self.assertIn(old, text)
        header.write_text(text.replace(old, new, 1), encoding="utf-8")
        command("git", "add", "include/talon.h", cwd=self.core)
        command("git", "commit", "-qm", "header drift", cwd=self.core)
        command("git", "tag", "-f", "v1.2.3", cwd=self.core)
        lock = release.load_json(self.lock)
        lock["core"]["commit"] = command("git", "rev-parse", "HEAD", cwd=self.core)
        lock["abi"]["header_sha256"] = release.sha256_file(header)
        return lock

    def verify_args(self, manifest: Path, signature: Path) -> argparse.Namespace:
        lock = release.load_json(self.lock)
        return argparse.Namespace(
            manifest=manifest,
            signature=signature,
            public_key=self.public_key,
            expected_key_id="fixture-key",
            expected_key_sha256=self.public_fingerprint,
            expected_release_tag=lock["release_tag"],
            expected_talon_bin_commit=self.bin_commit,
            expected_core_repository=lock["core"]["repository"],
            expected_core_tag=lock["core"]["tag"],
            expected_core_commit=lock["core"]["commit"],
            expected_core_version=lock["core"]["cargo_version"],
            expected_abi_profile=lock["abi"]["profile"],
            expected_abi_version=lock["abi"]["version"],
            expected_header_sha256=lock["abi"]["header_sha256"],
        )

    def test_signed_manifest_archive_and_offline_bundle_verify(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        release.command_verify(self.verify_args(manifest, signature))
        offline = self.output / "offline.tar.gz"
        args = self.verify_args(manifest, signature)
        args.output = offline
        release.command_offline(args)
        self.assertTrue(offline.is_file())
        self.assertGreater(offline.stat().st_size, 0)

    def test_four_platform_fixture_manifests_sign_verify_and_package_offline(self) -> None:
        schema = release.load_json(MODULE_PATH.parents[1] / "schema" / "talon-native-manifest-v1.schema.json")
        validator = jsonschema.Draft202012Validator(schema)
        for platform, target in release.EXPECTED_TARGETS.items():
            with self.subTest(platform=platform):
                manifest = self.build(platform)
                value = release.load_json(manifest)
                validator.validate(value)
                self.assertEqual(value["artifact"]["platform"], platform)
                self.assertEqual(value["build"]["target"], target["triple"])
                self.assertEqual(value["build"]["runner"], target["runner"])
                self.assertEqual(
                    {record["path"] for record in value["artifact"]["files"]},
                    {target["static_library"], target["dynamic_library"], "talon.h", "LICENSE.core", "NOTICE"},
                )
                signature = Path(str(manifest) + ".sig")
                release.command_sign(
                    argparse.Namespace(
                        manifest=manifest,
                        private_key=self.private_key,
                        signature=signature,
                    )
                )
                release.command_verify(self.verify_args(manifest, signature))
                offline = self.output / f"talon-enterprise-core-{platform}-fixture-offline.tar.gz"
                args = self.verify_args(manifest, signature)
                args.output = offline
                release.command_offline(args)
                self.assertTrue(offline.is_file())

                value["build"]["runner"] = "untrusted-runner"
                with self.assertRaisesRegex(release.ReleaseError, "platform does not match"):
                    release.validate_manifest(value)

    def test_json_schema_accepts_generator_output_and_rejects_contract_drift(self) -> None:
        schema = release.load_json(MODULE_PATH.parents[1] / "schema" / "talon-native-manifest-v1.schema.json")
        jsonschema.Draft202012Validator.check_schema(schema)
        validator = jsonschema.Draft202012Validator(schema)
        manifest = release.load_json(self.build())
        validator.validate(manifest)
        manifest["abi"]["required_symbols"].remove("talon_build_manifest")
        with self.assertRaises(jsonschema.ValidationError):
            validator.validate(manifest)
        manifest = release.load_json(self.build())
        manifest["artifact"]["files"].append(dict(manifest["artifact"]["files"][0]))
        with self.assertRaises(jsonschema.ValidationError):
            validator.validate(manifest)

    def test_archive_tamper_is_rejected(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        archive = self.output / "libtalon-core-linux-amd64.tar.gz"
        archive.write_bytes(archive.read_bytes() + b"tamper")
        with self.assertRaises(release.ReleaseError):
            release.command_verify(self.verify_args(manifest, signature))

    def test_native_library_hash_record_is_bound_to_archive_member(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        library = next(record for record in value["artifact"]["files"] if record["path"] == "libtalon.so")
        library["sha256"] = "0" * 64
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "archive member libtalon.so"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_signature_tamper_is_rejected(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        value = json.loads(manifest.read_text(encoding="utf-8"))
        value["release"]["tag"] = "v9.9.9"
        manifest.write_bytes(release.canonical_json(value))
        with self.assertRaises(release.ReleaseError):
            release.command_verify(self.verify_args(manifest, signature))

    def test_same_key_signed_rollback_identity_is_rejected(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        value["release"]["tag"] = "v1.2.2"
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "does not match trusted policy"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_manifest_cannot_delete_sdk_required_self_attestation_symbol(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        value["abi"]["required_symbols"].remove("talon_build_manifest")
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "required_symbols"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_unknown_manifest_field_is_rejected(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        value = json.loads(manifest.read_text(encoding="utf-8"))
        value["mutable_latest"] = True
        manifest.write_bytes(release.canonical_json(value))
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "unknown"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_unknown_external_manifest_gate_is_rejected(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        value = release.load_json(manifest)
        value["gates"]["revision_stream_v1"] = {"status": "gated", "reason": "not part of external manifest v1"}
        manifest.write_bytes(release.canonical_json(value))
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "gates fields differ"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_signature_filename_mismatch_is_rejected(self) -> None:
        manifest = self.build()
        expected_signature = Path(str(manifest) + ".sig")
        release.command_sign(
            argparse.Namespace(
                manifest=manifest,
                private_key=self.private_key,
                signature=expected_signature,
            )
        )
        signature = self.output / "wrong-name.sig"
        expected_signature.rename(signature)
        with self.assertRaisesRegex(release.ReleaseError, "signature filename"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_sign_rejects_wrong_name_and_input_overwrite(self) -> None:
        manifest = self.build()
        with self.assertRaisesRegex(release.ReleaseError, "signature output filename"):
            release.command_sign(
                argparse.Namespace(
                    manifest=manifest,
                    private_key=self.private_key,
                    signature=self.output / "wrong-name.sig",
                )
            )
        before = manifest.read_bytes()
        with self.assertRaisesRegex(release.ReleaseError, "must not overwrite"):
            release.command_sign(
                argparse.Namespace(
                    manifest=manifest,
                    private_key=self.private_key,
                    signature=manifest,
                )
            )
        self.assertEqual(manifest.read_bytes(), before)

    def test_packaged_key_cannot_replace_out_of_band_trust(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        args = self.verify_args(manifest, signature)
        args.expected_key_sha256 = "0" * 64
        with self.assertRaisesRegex(release.ReleaseError, "trusted policy"):
            release.command_verify(args)

    def test_unassigned_key_id_is_never_a_valid_signed_manifest_identity(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        value["signing"]["key_id"] = "UNASSIGNED"
        with self.assertRaisesRegex(release.ReleaseError, "signing identity"):
            release.validate_manifest(value)

        schema = release.load_json(MODULE_PATH.parents[1] / "schema" / "talon-native-manifest-v1.schema.json")
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(schema).validate(value)

        value["signing"]["key_id"] = "fixture-key"
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        args = self.verify_args(manifest, signature)
        args.expected_key_id = "UNASSIGNED"
        with self.assertRaisesRegex(release.ReleaseError, "expected signing key ID"):
            release.command_verify(args)

    def test_duplicate_archive_member_is_rejected(self) -> None:
        manifest = self.build()
        archive_path = self.output / "libtalon-core-linux-amd64.tar.gz"
        value = release.load_json(manifest)
        epoch = value["source"]["source_date_epoch"]
        entries = [
            (self.stage / "libtalon.a", "libtalon.a"),
            (self.stage / "libtalon.a", "libtalon.a"),
            (self.stage / "libtalon.so", "libtalon.so"),
            (self.stage / "talon.h", "talon.h"),
            (self.stage / "LICENSE.core", "LICENSE.core"),
            (self.stage / "NOTICE", "NOTICE"),
        ]
        release.make_tar(entries, archive_path, epoch)
        value["artifact"]["archive"] = release.file_record(archive_path)
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "duplicate archive member"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_manifest_rejects_cross_field_header_mismatch(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        value["abi"]["header_sha256"] = "0" * 64
        manifest.write_bytes(release.canonical_json(value))
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        with self.assertRaisesRegex(release.ReleaseError, "ABI header identity"):
            release.command_verify(self.verify_args(manifest, signature))

    def test_offline_bundle_reverifies_inputs(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        archive = self.output / "libtalon-core-linux-amd64.tar.gz"
        archive.write_bytes(archive.read_bytes() + b"tamper")
        args = self.verify_args(manifest, signature)
        args.output = self.output / "offline.tar.gz"
        with self.assertRaisesRegex(release.ReleaseError, "archive hash"):
            release.command_offline(args)

    def test_offline_bundle_rejects_basename_collision(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        collision_dir = self.root / "collision"
        collision_dir.mkdir()
        colliding_public_key = collision_dir / "NOTICE"
        colliding_public_key.write_bytes(self.public_key.read_bytes())
        args = self.verify_args(manifest, signature)
        args.public_key = colliding_public_key
        args.output = self.output / "offline.tar.gz"
        with self.assertRaisesRegex(release.ReleaseError, "duplicate basename"):
            release.command_offline(args)

    def test_offline_bundle_cannot_overwrite_verified_input(self) -> None:
        manifest = self.build()
        signature = Path(str(manifest) + ".sig")
        release.command_sign(argparse.Namespace(manifest=manifest, private_key=self.private_key, signature=signature))
        before = manifest.read_bytes()
        args = self.verify_args(manifest, signature)
        args.output = manifest
        with self.assertRaisesRegex(release.ReleaseError, "must not overwrite"):
            release.command_offline(args)
        self.assertEqual(manifest.read_bytes(), before)

    def test_repeated_packaging_is_byte_deterministic(self) -> None:
        manifest = self.build()
        archive = self.output / "libtalon-core-linux-amd64.tar.gz"
        first = (release.sha256_file(manifest), release.sha256_file(archive))
        self.build()
        second = (release.sha256_file(manifest), release.sha256_file(archive))
        self.assertEqual(first, second)

    def test_release_manifest_requires_binary_self_attestation(self) -> None:
        manifest = self.build()
        value = release.load_json(manifest)
        self.assertTrue(value["compatibility"]["binary_self_attestation"])
        value["compatibility"]["binary_self_attestation"] = False
        with self.assertRaisesRegex(release.ReleaseError, "compatibility"):
            release.validate_manifest(value)

    def test_release_ready_lock_rejects_missing_self_attestation(self) -> None:
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["status"] = "gated"
        with self.assertRaisesRegex(release.ReleaseError, "self-attestation is gated"):
            release.validate_lock(lock, allow_gated=False)
        release.validate_lock(lock, allow_gated=True)

    def test_build_entrypoint_cannot_bypass_unreleased_lock(self) -> None:
        lock = release.load_json(self.lock)
        lock["release_tag"] = "UNRELEASED"
        self.lock.write_bytes(release.canonical_json(lock))
        with self.assertRaisesRegex(release.ReleaseError, "UNRELEASED"):
            self.build()

    def test_tracked_development_lock_keeps_unreleased_gates(self) -> None:
        lock = release.load_json(MODULE_PATH.parents[1] / "release-lock.json")
        self.assertEqual(lock["release_tag"], "UNRELEASED")
        self.assertIsNone(lock["core"]["tag"])
        self.assertEqual(lock["signing"]["status"], "gated")
        self.assertEqual(lock["signing"]["key_id"], "UNASSIGNED")
        self.assertIsNone(lock["signing"]["public_key_sha256"])
        self.assertEqual(lock["runtime_attestation"]["status"], "gated")
        self.assertEqual(lock["runtime_attestation"]["features"], list(release.RUNTIME_FEATURE_CONTRACT))
        capabilities = {
            capability["name"]: capability
            for capability in lock["runtime_attestation"]["capabilities"]
        }
        for name in ("storage_conditional_prefix_scan", "revision_stream", "native_quorum", "server_ha_production_admission"):
            self.assertEqual(capabilities[name]["status"], "gated")
        self.assertEqual(capabilities["storage_conditional_prefix_scan"]["version"], 1)
        self.assertEqual(capabilities["storage_conditional_prefix_scan"]["reason"], release.PREFIX_SCAN_V1_GATED_REASON)
        self.assertEqual(capabilities["revision_stream"]["version"], 2)
        self.assertEqual(capabilities["revision_stream"]["reason"], release.REVISION_STREAM_V2_GATED_REASON)

    def test_prefix_scan_v1_conformance_is_gated_and_digest_bound(self) -> None:
        conformance_path = release.PREFIX_SCAN_V1_CONFORMANCE_PATH
        conformance = release.validate_prefix_scan_v1_conformance(conformance_path)
        self.assertEqual(conformance["status"], "gated")
        with self.assertRaisesRegex(release.ReleaseError, "is gated"):
            release.validate_prefix_scan_v1_conformance(conformance_path, require_ready=True)
        conformance["digest_vector"]["response"]["entries"][0]["value"] = [116, 119, 111]
        tampered = self.root / "tampered-prefix-scan-conformance.json"
        tampered.write_bytes(release.canonical_json(conformance))
        with self.assertRaisesRegex(release.ReleaseError, "digest vector"):
            release.validate_prefix_scan_v1_conformance(tampered)
        conformance = release.load_json(conformance_path)
        conformance["source"]["merge_commit"] = "0" * 40
        tampered.write_bytes(release.canonical_json(conformance))
        with self.assertRaisesRegex(release.ReleaseError, "reviewed Core implementation"):
            release.validate_prefix_scan_v1_conformance(tampered)
        conformance = release.load_json(conformance_path)
        conformance["sdk"]["merge_commit"] = "0" * 40
        tampered.write_bytes(release.canonical_json(conformance))
        with self.assertRaisesRegex(release.ReleaseError, "reviewed Go SDK implementation"):
            release.validate_prefix_scan_v1_conformance(tampered)

    def test_release_build_requires_ready_prefix_scan_conformance(self) -> None:
        conformance = release.load_json(self.prefix_scan_conformance)
        conformance["status"] = "gated"
        conformance["reason"] = release.PREFIX_SCAN_V1_GATED_REASON
        conformance["source"]["release_identity"] = None
        conformance["sdk"]["release_identity"] = None
        self.prefix_scan_conformance.write_bytes(release.canonical_json(conformance))
        with self.assertRaisesRegex(release.ReleaseError, "conformance is gated"):
            self.build()

    def test_prefix_scan_runtime_contract_cannot_be_promoted_or_deleted(self) -> None:
        lock = release.load_json(self.lock)
        capability = next(
            item
            for item in lock["runtime_attestation"]["capabilities"]
            if item["name"] == "storage_conditional_prefix_scan"
        )
        capability["status"] = "available"
        capability["reason"] = None
        with self.assertRaisesRegex(release.ReleaseError, "storage_conditional_prefix_scan"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["features"].remove("storage_conditional_prefix_scan_v1")
        with self.assertRaisesRegex(release.ReleaseError, "exactly match"):
            release.validate_lock(lock, allow_gated=False)

    def test_tracked_old_outer_abi_cannot_be_promoted_by_flipping_release_gates(self) -> None:
        lock = release.load_json(MODULE_PATH.parents[1] / "release-lock.json")
        lock["release_tag"] = "v9.9.9"
        lock["core"]["tag"] = "v9.9.9"
        lock["runtime_attestation"]["status"] = "ready"
        lock["signing"].update(
            status="ready",
            key_id="test-only-key",
            public_key_sha256="0" * 64,
        )
        with self.assertRaisesRegex(release.ReleaseError, "release ABI does not match"):
            release.validate_lock(lock, allow_gated=False)

    def test_runtime_required_symbol_drift_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["required_symbols"].remove("talon_build_manifest")
        with self.assertRaisesRegex(release.ReleaseError, "required symbols"):
            release.validate_lock(lock, allow_gated=False)

    def test_core_self_manifest_header_or_symbol_drift_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        target = release.target_for(lock, "linux-amd64")
        manifest = release.load_json(self.core_build_manifest)
        manifest["header_sha256"] = "0" * 64
        with self.assertRaisesRegex(release.ReleaseError, "source/build identity"):
            release.validate_core_build_manifest(manifest, lock, target)
        manifest = release.load_json(self.core_build_manifest)
        manifest["abi"]["required_symbols"].remove("talon_build_manifest")
        with self.assertRaisesRegex(release.ReleaseError, "ABI"):
            release.validate_core_build_manifest(manifest, lock, target)

    def test_core_self_manifest_cannot_be_reused_for_another_target(self) -> None:
        lock = release.load_json(self.lock)
        manifest = release.load_json(self.core_build_manifest)
        macos_target = release.target_for(lock, "macos-arm64")
        with self.assertRaisesRegex(release.ReleaseError, "source/build identity"):
            release.validate_core_build_manifest(manifest, lock, macos_target)

        manifest["target"] = macos_target["triple"]
        with self.assertRaisesRegex(release.ReleaseError, "build_binding_sha256"):
            release.validate_core_build_manifest(manifest, lock, macos_target)

    def test_revision_stream_feature_and_capability_drift_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["features"].remove("revision_stream_v2_mmr_proof")
        with self.assertRaisesRegex(release.ReleaseError, "exactly match"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["features"].remove("revision_stream_v1")
        with self.assertRaisesRegex(release.ReleaseError, "exactly match"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        revision = next(
            capability
            for capability in lock["runtime_attestation"]["capabilities"]
            if capability["name"] == "revision_stream"
        )
        revision["version"] = 1
        with self.assertRaisesRegex(release.ReleaseError, "revision_stream"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["features"].append("revision_stream_v3_unreviewed")
        with self.assertRaisesRegex(release.ReleaseError, "exactly match"):
            release.validate_lock(lock, allow_gated=False)

    def test_revision_stream_v2_gated_reason_drift_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        revision = next(
            capability
            for capability in lock["runtime_attestation"]["capabilities"]
            if capability["name"] == "revision_stream"
        )
        revision["reason"] = "v2 is ready enough"
        with self.assertRaisesRegex(release.ReleaseError, "gated reason"):
            release.validate_lock(lock, allow_gated=False)

    def test_revision_stream_v2_header_version_drift_is_rejected(self) -> None:
        lock = self.commit_core_header_change(
            "#define TALON_REVISION_STREAM_VERSION 2u",
            "#define TALON_REVISION_STREAM_VERSION 1u",
        )
        with self.assertRaisesRegex(release.ReleaseError, "TALON_REVISION_STREAM_VERSION=2"):
            release.validate_source(lock, self.core)

    def test_revision_stream_v2_header_proof_contract_drift_is_rejected(self) -> None:
        lock = self.commit_core_header_change("talon_mmr_sha256_v1", "unreviewed_proof_scheme")
        with self.assertRaisesRegex(release.ReleaseError, "talon_mmr_sha256_v1"):
            release.validate_source(lock, self.core)

    def test_revision_stream_v2_header_proof_version_drift_is_rejected(self) -> None:
        lock = self.commit_core_header_change('"proof":{"version":1', '"proof":{"version":2')
        with self.assertRaisesRegex(release.ReleaseError, "proof"):
            release.validate_source(lock, self.core)

    def test_unadmitted_runtime_capabilities_cannot_be_promoted_by_lock(self) -> None:
        for name in ("revision_stream", "native_quorum", "server_ha_production_admission"):
            with self.subTest(name=name):
                lock = release.load_json(self.lock)
                capability = next(
                    value
                    for value in lock["runtime_attestation"]["capabilities"]
                    if value["name"] == name
                )
                capability["status"] = "available"
                capability["reason"] = None
                with self.assertRaisesRegex(release.ReleaseError, name):
                    release.validate_lock(lock, allow_gated=False)

    def test_external_storage_gate_cannot_be_promoted_by_lock_or_manifest(self) -> None:
        lock = release.load_json(self.lock)
        lock["abi"]["capabilities"]["storage_conditional_batch_v1"] = {
            "status": "available",
            "reason": "single-field promotion",
        }
        with self.assertRaisesRegex(release.ReleaseError, "must remain gated"):
            release.validate_lock(lock, allow_gated=False)
        manifest = release.load_json(self.build())
        manifest["gates"]["storage_conditional_batch_v1"]["status"] = "available"
        with self.assertRaisesRegex(release.ReleaseError, "must remain gated"):
            release.validate_manifest(manifest)

    def test_core_self_manifest_binding_tamper_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        manifest = release.load_json(self.core_build_manifest)
        manifest["build_binding_sha256"] = "0" * 64
        with self.assertRaisesRegex(release.ReleaseError, "build_binding_sha256"):
            release.validate_core_build_manifest(
                manifest,
                lock,
                release.target_for(lock, "linux-amd64"),
            )

    def test_native_probe_rejects_oversized_self_manifest(self) -> None:
        if sys.platform not in ("darwin", "linux"):
            self.skipTest("dlopen probe fixture is supported on macOS and Linux")
        probe = self.root / "read-build-manifest"
        library = self.root / ("fixture.dylib" if sys.platform == "darwin" else "fixture.so")
        probe_source = MODULE_PATH.parents[1] / "tools" / "read-build-manifest.c"
        fixture_source = MODULE_PATH.parents[1] / "tests" / "fixtures" / "oversized-build-manifest.c"
        probe_command = ["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", str(probe_source)]
        if sys.platform == "linux":
            probe_command.append("-ldl")
        probe_command.extend(["-o", str(probe)])
        command(*probe_command)
        library_args = ["-dynamiclib"] if sys.platform == "darwin" else ["-shared", "-fPIC"]
        command("cc", "-std=c11", "-Wall", "-Wextra", "-Werror", *library_args, str(fixture_source), "-o", str(library))
        result = subprocess.run([probe, library], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("exceeds the 1 MiB bound", result.stderr)

    def test_native_probe_fails_closed_on_missing_or_failed_self_attestation(self) -> None:
        if sys.platform not in ("darwin", "linux"):
            self.skipTest("dlopen probe fixture is supported on macOS and Linux")
        probe = self.root / "read-build-manifest"
        probe_source = MODULE_PATH.parents[1] / "tools" / "read-build-manifest.c"
        probe_command = ["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", str(probe_source)]
        if sys.platform == "linux":
            probe_command.append("-ldl")
        probe_command.extend(["-o", str(probe)])
        command(*probe_command)
        library_args = ["-dynamiclib"] if sys.platform == "darwin" else ["-shared", "-fPIC"]

        fixtures = {
            "missing": "int unrelated_symbol(void) { return 0; }\n",
            "failed": (
                "int talon_build_manifest(char **out) { *out = 0; return 1; }\n"
                "void talon_free_string(char *value) { (void)value; }\n"
            ),
        }
        for name, source in fixtures.items():
            with self.subTest(name=name):
                fixture_source = self.root / f"{name}.c"
                fixture_source.write_text(source, encoding="utf-8")
                library = self.root / (f"{name}.dylib" if sys.platform == "darwin" else f"{name}.so")
                command("cc", "-std=c11", "-Wall", "-Wextra", "-Werror", *library_args, str(fixture_source), "-o", str(library))
                result = subprocess.run(
                    [probe, library],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn(
                    "cannot resolve talon_build_manifest" if name == "missing" else "talon_build_manifest failed",
                    result.stderr,
                )

    def test_boolean_values_cannot_masquerade_as_integer_identity_fields(self) -> None:
        lock = release.load_json(self.lock)
        lock["abi"]["version"] = True
        with self.assertRaisesRegex(release.ReleaseError, "positive integer"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["manifest_version"] = True
        with self.assertRaisesRegex(release.ReleaseError, "manifest_version"):
            release.validate_lock(lock, allow_gated=False)
        lock = release.load_json(self.lock)
        lock["runtime_attestation"]["capabilities"][0]["version"] = True
        with self.assertRaisesRegex(release.ReleaseError, "storage_conditional_batch"):
            release.validate_lock(lock, allow_gated=False)

        manifest_path = self.build()
        manifest = release.load_json(manifest_path)
        manifest["abi"]["version"] = True
        with self.assertRaisesRegex(release.ReleaseError, "ABI identity"):
            release.validate_manifest(manifest)
        manifest = release.load_json(manifest_path)
        manifest["artifact"]["archive"]["size"] = True
        with self.assertRaisesRegex(release.ReleaseError, "size must be positive"):
            release.validate_manifest(manifest)

        lock = release.load_json(self.lock)
        runtime = release.load_json(self.core_build_manifest)
        runtime["capabilities"][0]["version"] = True
        runtime["build_binding_sha256"] = release.compute_build_binding(runtime)
        with self.assertRaisesRegex(release.ReleaseError, "version must be an integer"):
            release.validate_core_build_manifest(
                runtime,
                lock,
                release.target_for(lock, "linux-amd64"),
            )

    def test_dirty_or_wrong_core_source_is_rejected(self) -> None:
        lock = release.load_json(self.lock)
        (self.core / "untracked").write_text("dirty", encoding="utf-8")
        with self.assertRaisesRegex(release.ReleaseError, "dirty"):
            release.validate_source(lock, self.core)

    def test_release_lock_rejects_gated_signing(self) -> None:
        lock = release.load_json(self.lock)
        lock["signing"]["status"] = "gated"
        with self.assertRaisesRegex(release.ReleaseError, "signing is gated"):
            release.validate_lock(lock, allow_gated=False)
        release.validate_lock(lock, allow_gated=True)

    def test_release_lock_rejects_unknown_fields_and_target_drift(self) -> None:
        lock = release.load_json(self.lock)
        lock["mutable_latest"] = True
        with self.assertRaisesRegex(release.ReleaseError, "unknown"):
            release.validate_lock(lock, allow_gated=False)
        del lock["mutable_latest"]
        lock["targets"][0]["runner"] = "mutable-runner"
        with self.assertRaisesRegex(release.ReleaseError, "platform contract"):
            release.validate_lock(lock, allow_gated=False)

    def test_gated_lock_rejects_malformed_signing_identity(self) -> None:
        for key_id in ("bad/key", "bad\nkey"):
            with self.subTest(key_id=key_id):
                lock = release.load_json(self.lock)
                lock["signing"].update(status="gated", key_id=key_id, public_key_sha256=None)
                with self.assertRaisesRegex(release.ReleaseError, "signing.key_id"):
                    release.validate_lock(lock, allow_gated=True)
        lock = release.load_json(self.lock)
        lock["signing"].update(status="gated", key_id="UNASSIGNED", public_key_sha256="not-a-hash")
        with self.assertRaisesRegex(release.ReleaseError, "public_key_sha256"):
            release.validate_lock(lock, allow_gated=True)

    def test_lock_rejects_non_semver_and_core_repository_substitution(self) -> None:
        lock = release.load_json(self.lock)
        lock["core"]["cargo_version"] = "latest"
        with self.assertRaisesRegex(release.ReleaseError, "semver"):
            release.validate_lock(lock, allow_gated=True)
        for repository in ("https://example.invalid/talon-core", release.CORE_REPOSITORY + "\nattacker"):
            with self.subTest(repository=repository):
                lock = release.load_json(self.lock)
                lock["core"]["repository"] = repository
                with self.assertRaisesRegex(release.ReleaseError, "core.repository"):
                    release.validate_lock(lock, allow_gated=True)

    def test_manifest_rejects_non_semver_source_version(self) -> None:
        manifest_path = self.build()
        manifest = release.load_json(manifest_path)
        manifest["source"]["cargo_version"] = "latest"
        with self.assertRaisesRegex(release.ReleaseError, "source identity"):
            release.validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
