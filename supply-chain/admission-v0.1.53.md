# v0.1.53 Enterprise Core candidate admission

The [four-target candidate run 36878054786](https://github.com/darkmice/talon-bin/actions/runs/36878054786) completed successfully from `talon-bin` source `78aff97873febe52ebfc0a09d73a7c5d64941618`. It checked out tagged Core `v0.1.1` at `6010d748aebd3c4e595534ed4a9e959e7e5334cf`.

Every Linux AMD64, Linux ARM64, macOS AMD64, and macOS ARM64 target passed:

- Exact Core source build and ABI symbol checks.
- Executed build-manifest self-attestation and the linked C ABI smoke, including a conditional prefix scan.
- Core native prefix-scan unit and HTTP end-to-end conformance on a matching runner.
- Ed25519 signing, manifest verification, and offline artifact packaging.
- SDK native SQL/KV acceptance and the prefix-scan digest vector.

The run also verified and staged a four-platform Go runtime **candidate** with `identity.status=gated`. The SDK candidate used there was `4698cdfbc6ab89cb0892c1dc23a1d1459459670b`; its prefix-scan protocol is unchanged from the released SDK `v0.7.4` at `5f2c2d346d7bb256503cf3e32a342f4dd062fb70`. The subsequent SDK commit `5d9e2f9c17b9ef50f08b8ea075688e3b686548ec` adds the public embedded `talon.Open` release test.

This evidence admits the locked runtime attestation and prefix-scan conformance. Formal publication still requires release mode from merged `main`, review of those signed artifacts, a `ready` Go module tag, and SDK acceptance against the published module.
