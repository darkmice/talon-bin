# Talon signed Go runtime module

This nested Go module is a gated source template. `identity.json` is
`{"status":"gated"}` and `Materialize` returns `ErrUnavailable`; it does not
make the unsigned `talon-bin` release archives loadable by the Go SDK.

`build-enterprise-core.yml` builds and verifies four runtime-only bundles, then
stages one Go module. Candidate mode keeps its identity `gated`; release mode
requires the admitted lock and conformance on `main` and stages `ready` assets.
The module includes the dynamic library,
signed manifest, signature, SBOM, license inventory, Core license, and NOTICE for
each platform. Build tags embed only the target platform in an application.

Review the release-mode artifact, its source commit, the four signatures and SDK
cross-target tests. Publish its exact bytes under an immutable nested-module tag
`go-runtime/vX.Y.Z`, then pin that version in `talon-sdk-go`. The SDK must verify
the manifest and runtime self-attestation before `dlopen`; the module checksum
alone is not a replacement for the release signature. Do not tag this gated
template as a working native release.
