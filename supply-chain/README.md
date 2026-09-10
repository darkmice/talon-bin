# Talon Enterprise Core supply chain

This directory is the source of truth for the native Core artifact contract
consumed by Enterprise installations and language SDKs. It does not contain AI
Platform domain logic.

## Release invariants

- `release-lock.json` names one full Core commit, Cargo version, Cargo.lock
  digest, ABI profile/header digest, exact Rust toolchain, and four supported
  macOS/Linux targets. Branch names, `latest`, and abbreviated SHAs are invalid.
- A lock with `release_tag: UNRELEASED`, gated signing, or an unpinned signing
  key cannot produce a release candidate.
- `storage_conditional_batch_v1` remains `gated` until the implementation is in
  a clean, immutable Core commit and the corresponding header hash is locked.
  An uncommitted worktree is never a release source.
- `runtime_attestation` records the exact Core manifest v1 contract required by
  the current Go SDK: ABI profile/version, all 23 required symbols, features,
  capability versions, and their admission states. Its current `gated` status
  is deliberate: the pinned clean Core commit predates `talon_build_manifest`.
  The contract fields describe the next release gate and are not evidence that
  the dirty development checkout was released.
- `conformance/conditional-prefix-scan-v1.json` records the reviewed v1
  prefix-scan digest vector and all four-target evidence requirements. It is
  deliberately `gated`: a Core PR reference is not a release identity, and it
  cannot be promoted without a clean tag, typed SDK surface, and cross-target
  proof.
- Each platform gets a deterministic inner native archive, CycloneDX SBOM,
  license inventory, Core license, NOTICE, signed manifest, and deterministic
  outer offline bundle. The native compiler output is not claimed bit-for-bit
  reproducible across mutable hosted runner images; the manifest records the
  exact build source, toolchain, target, runner label, command, and source date.
- The tracked or packaged public key is diagnostic material, not its own trust
  root. Consumers must pin the expected key ID and DER public-key SHA-256 out of
  band (for example in the SDK or Enterprise configuration).

The new GitHub workflow only uploads workflow artifacts. It does not create a
GitHub Release, publish an SDK package, commit, or push.

The producer matrix builds four artifact targets. That is not the same as four
Go SDK loader targets: the current `talon-sdk-go` runtime selector implements
only `darwin/arm64` and `linux/amd64`. macOS AMD64 and Linux ARM64 artifacts must
not be reported as SDK runtime E2E coverage until the SDK adds those selectors
and executes their native fixtures.

## SDK runtime verification contract

Before loading a native library, `talon-sdk-go` and other consumers must:

1. Parse only `schema_version: 1.0` and reject duplicate/unknown critical
   fields rather than silently downgrading.
2. Select the exact current platform entry and require
   `artifact.kind == talon-core-native-library`. Cross-check the platform against
   its fixed target triple, runner, archive name, native library names, and
   manifest/signature filenames; do not treat these signed fields independently.
3. Verify the Ed25519 signature over the exact manifest bytes using a trusted
   out-of-band public key, then match `key_id` and `public_key_sha256` to the
   consumer's pinned trust policy.
4. Verify the archive and selected native library SHA-256 and size before
   extraction or loading. Archive extraction must reject absolute paths,
   traversal, links, devices, duplicate names, and unexpected members. The
   packaged `talon.h` digest must equal `abi.header_sha256`; inner and outer
   license/NOTICE records must also agree.
5. Require `compatibility.binary_self_attestation == true`, load the selected
   library, call `talon_build_manifest`, and cross-check its source, target,
   Cargo.lock/header digests, ABI, symbols, features, capabilities, and
   recomputed `build_binding_sha256` against the signed manifest and trusted SDK
   contract. A required runtime capability with `status: gated` must fail closed.
6. Match the expected release channel, Core commit/Cargo version, ABI profile,
   ABI version, header digest, and required symbols.
7. Never fall back to another local path, an unsigned library, a mutable
   release URL, or a SHA inferred only from a tag.

The release workflow challenges each candidate dynamic library in its target
runtime (QEMU is used for the Linux ARM64 cross-build), validates the returned
Core manifest, and only then emits an external manifest with
`binary_self_attestation: true`. The external manifest v1 `gates` object remains
limited to `storage_conditional_batch_v1` because the current Go SDK strictly
rejects unknown fields there. The legacy `revision_stream_v1` feature, the
`revision_stream_v2_mmr_proof` feature, and the full capability map are
authenticated by the loaded Core self-manifest instead. Admission states
are code-owned in this contract: revision stream, native quorum, server HA, and
the SDK's external storage gate cannot be promoted merely by editing the lock.
Their gated states require reasons and fail closed until a reviewed code/test
contract change accompanies the missing production evidence.

## Local verification

The release tool itself uses only the Python standard library and OpenSSL. The
test suite additionally uses the fully version-pinned packages in
`test-requirements.txt` to run a real JSON Schema Draft 2020-12 validator.

```bash
python3 -m pip install -r supply-chain/test-requirements.txt
python3 supply-chain/tools/release.py validate-lock \
  --lock supply-chain/release-lock.json --allow-gated
python3 supply-chain/tools/release.py validate-prefix-scan-conformance \
  --path supply-chain/conformance/conditional-prefix-scan-v1.json
python3 -m unittest discover -s supply-chain/tests -v
```

Production generation requires a release-ready lock and an Ed25519 private key
provided to Actions as `TALON_RELEASE_SIGNING_KEY_PEM_B64`. The corresponding
DER public-key SHA-256 must be pinned in the lock before dispatch. A clean,
tagged Core whose ABI matches `runtime_attestation` is also required; the current
development lock cannot generate a candidate.

Offline verification must supply the trusted key and complete release/source/ABI
identity separately from the bundle. Pinning only a long-lived key would still
allow a correctly signed older release or different Core identity to be
substituted:

```bash
python3 supply-chain/tools/release.py verify \
  --manifest libtalon-core-linux-amd64.manifest.json \
  --signature libtalon-core-linux-amd64.manifest.json.sig \
  --public-key talon-release-public.pem \
  --expected-key-id 'enterprise-release-key-id-from-trusted-config' \
  --expected-key-sha256 '64-lowercase-hex-characters-from-trusted-config' \
  --expected-release-tag 'vX.Y.Z' \
  --expected-talon-bin-commit '40-lowercase-hex-characters' \
  --expected-core-repository 'https://github.com/darkmice/talon-core' \
  --expected-core-tag 'vX.Y.Z' \
  --expected-core-commit '40-lowercase-hex-characters' \
  --expected-core-version 'X.Y.Z' \
  --expected-abi-profile 'talon-native-c' \
  --expected-abi-version 1 \
  --expected-header-sha256 '64-lowercase-hex-characters'
```
