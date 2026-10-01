# Talon signed Go runtime module

This nested Go module is a gated source template. `identity.json` is
`{"status":"gated"}` and `Materialize` returns `ErrUnavailable`; it does not
make the unsigned `talon-bin` release archives loadable by the Go SDK.

After a release-ready Core lock and production Ed25519 signing identity exist,
`build-enterprise-core.yml` builds and verifies four runtime-only bundles, then
stages one Go module candidate. The candidate includes the dynamic library,
signed manifest, signature, SBOM, license inventory, Core license, and NOTICE for
each platform. Build tags embed only the target platform in an application.

Review the staged candidate, its source commit, the four signatures and SDK
cross-target tests. Publish its exact bytes under an immutable nested-module tag
`go-runtime/vX.Y.Z`, then pin that version in `talon-sdk-go`. The SDK must verify
the manifest and runtime self-attestation before `dlopen`; the module checksum
alone is not a replacement for the release signature. Do not tag this gated
template as a working native release.
