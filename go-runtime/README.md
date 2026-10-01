# Signed Talon Core Go runtime

`v0.1.54` embeds the four Ed25519-signed Talon Core runtime artifacts.
Build tags embed only the current platform. `talon-sdk-go` checks the
pinned signing key, exact release and source identity, every archive
member, and the loaded Core self-manifest before opening a database.

The runtime assets are bound to the `talon-bin` source commit recorded
in `identity.json`; this module's nested tag names the later commit
containing those reviewed assets.
