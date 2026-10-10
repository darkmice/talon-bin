import argparse
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "tools"))
import server_artifact
import test_release as fixture_test
command, release = fixture_test.command, fixture_test.release


class ServerArtifactTest(unittest.TestCase):
    setUp = fixture_test.ReleaseSupplyChainTest.setUp
    tearDown = fixture_test.ReleaseSupplyChainTest.tearDown

    def server(self):
        (self.core / "LICENSE").write_bytes(b"fixture license")
        args = argparse.Namespace(core_root=self.core, binary=self.stage / "libtalon.so",
                                  notice=self.stage / "NOTICE", output=self.output,
                                  public_key=self.public_key, talon_bin_commit=self.bin_commit,
                                  platform="linux-amd64", run_id="123")
        lock = release.load_json(self.lock)
        with patch.object(server_artifact.release, "validate_source"):
            server_artifact.build(args, lock)
        args.manifest = self.output / "talon-core-server-linux-amd64.manifest.json"
        args.signature = args.manifest.with_name(args.manifest.name + ".sig")
        self.sign(args)
        return args, lock

    def sign(self, args):
        command("openssl", "pkeyutl", "-sign", "-inkey", str(self.private_key), "-rawin",
                "-in", str(args.manifest), "-out", str(args.signature))

    def test_signed_server_and_executable_members(self):
        args, lock = self.server()
        server_artifact.verify(args, lock)

    def test_tampered_archive_rejected(self):
        args, lock = self.server()
        (self.output / "talon-core-server-linux-amd64.tar.gz").write_bytes(b"tampered")
        with self.assertRaisesRegex(server_artifact.release.ReleaseError, "checksum"):
            server_artifact.verify(args, lock)

    def test_other_release_and_duplicate_member_rejected_even_if_signed(self):
        args, lock = self.server()
        value = json.loads(args.manifest.read_text())
        value["release_tag"] = "v9.9.9"
        args.manifest.write_text(json.dumps(value))
        self.sign(args)
        with self.assertRaisesRegex(server_artifact.release.ReleaseError, "identity"):
            server_artifact.verify(args, lock)
        value["release_tag"] = lock["release_tag"]
        value["files"][1] = value["files"][0]
        args.manifest.write_text(json.dumps(value))
        self.sign(args)
        with self.assertRaisesRegex(server_artifact.release.ReleaseError, "member set"):
            server_artifact.verify(args, lock)
