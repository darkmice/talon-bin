#!/usr/bin/env python3
"""Run the real Go Server SQL client and restart checks on an isolated HTTP process."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--sdk", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    manifest = json.loads(args.manifest.read_text())
    binary_sha = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    assert binary_sha == next(r["sha256"] for r in manifest["files"] if r["path"] == "talon")
    with tempfile.TemporaryDirectory(prefix="talon-http-release-") as private:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            addr = "127.0.0.1:" + str(listener.getsockname()[1])
        token = "isolated-release-regression"
        env = {k: v for k, v in os.environ.items() if not k.startswith("TALON_NATIVE_")}
        env.update({"GOWORK": "off", "TALON_SERVER_URL": "http://" + addr, "TALON_SERVER_TOKEN": token,
                    "TALON_SERVER_SQL_RELEASE_TAG": manifest["release_tag"],
                    "TALON_SERVER_SQL_TALON_BIN_COMMIT": manifest["talon_bin_commit"],
                    "TALON_SERVER_SQL_CORE_COMMIT": manifest["core"]["commit"],
                    "TALON_SERVER_SQL_ARTIFACT_SHA256": binary_sha})
        def request(sql):
            body = json.dumps({"cmd": "sql", "action": "query", "params": {"sql": sql, "protocol_version": 1}}).encode()
            req = urllib.request.Request("http://" + addr + "/api/sql", data=body,
                                         headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.load(response)
        def start(log):
            child = subprocess.Popen([str(args.binary.resolve()), "--data", private + "/db", "--addr", addr, "--token", token], stdout=log, stderr=log)
            deadline = time.monotonic() + 20
            while True:
                if child.poll() is not None:
                    raise RuntimeError("released HTTP process exited")
                try:
                    health = urllib.request.Request("http://" + addr + "/health", headers={"Authorization": "Bearer " + token})
                    with urllib.request.urlopen(health, timeout=1) as response:
                        if json.load(response).get("ok"):
                            return child
                except Exception:
                    pass
                if time.monotonic() > deadline:
                    child.kill(); child.wait()
                    raise RuntimeError("released HTTP startup timed out")
                time.sleep(0.05)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.with_suffix(".server.log").open("w") as log:
            child = start(log)
            try:
                command = ["go", "test", "./server", "-count=1", "-v", "-ldflags=-linkmode=external",
                           "-run", "TestServerSQLConstraintRelease|TestServerSQLLiveDecimalRoundTrip"]
                with args.output.with_suffix(".sdk.log").open("w") as sdk_log:
                    result = subprocess.run(command, cwd=args.sdk, env=env, stdout=sdk_log, stderr=subprocess.STDOUT)
                if result.returncode:
                    raise RuntimeError("real Go HTTP client regression failed; inspect .sdk.log")
                for sql in ("CREATE TABLE restart_uq (id INT PRIMARY KEY, checked_at INT, UNIQUE(checked_at))",
                            "INSERT INTO restart_uq VALUES(1,11)"):
                    assert request(sql)["ok"]
            finally:
                child.terminate(); child.wait(timeout=20)
            child = start(log)
            try:
                rejection = request("INSERT INTO restart_uq VALUES(2,11)")
                assert not rejection["ok"] and "UNIQUE" in rejection["error"]
                rows = request("SELECT id,checked_at FROM restart_uq")
                assert rows["ok"] and len(rows["data"]["rows"]) == 1
            finally:
                child.terminate(); child.wait(timeout=20)
        record = {"release_tag": manifest["release_tag"], "talon_bin_commit": manifest["talon_bin_commit"],
                  "core_commit": manifest["core"]["commit"], "server_sha256": binary_sha,
                  "sdk_source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.sdk, text=True).strip(),
                  "sdk_command": command, "sdk_exit_code": result.returncode, "restart_unique_rejection": True,
                  "restart_rows": 1, "isolated": True, "production_deployment": False}
        args.output.write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps(record))


if __name__ == "__main__":
    main()
