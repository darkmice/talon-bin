# Native shared Core and application process boundary

## Ownership contract

fjall 3.0.3 holds an exclusive lock on a database directory. Only one physical
Talon Store may own it. The Core implementation at
`a968c4db18d1f14b51d9d4b2e4f55ec5a642e72c` adds `Talon::open_shared`,
`open_shared_with_config`, and `open_shared_with_cluster`. They canonicalize
the directory, compare storage and cluster configuration, serialize first
opens, and keep a process registry of weak owners. The final logical owner's
drop releases the physical Store while the registry lock is held. This avoids
the interval where a weak reference is dead but fjall still owns the lock.
An incompatible configuration fails rather than borrowing the existing
instance. A lock held by another process remains a fjall `Locked` error.

The original `Talon::open` and its signature remain the exclusive embedded
API. Embedded Rust callers opting into sharing must use `open_shared` and,
for SQL transactions across calls, create a stable `SessionId`, use the
`run_sql_as` / `run_sql_param_as` family, and call `end_session` when their
logical connection ends. The C ABI `talon_open` now borrows the shared owner;
each returned opaque handle already has its own stable `SessionId`. The
existing `talon_run_sql`, `talon_run_sql_bin`, `talon_run_sql_param_bin`, and
`talon_execute` SQL paths pass that session. `talon_close` calls `end_session`,
which rolls back an unfinished explicit transaction. Core still allows only
one active explicit SQL transaction; another session receives `busy`, including
for reads, until the owner commits, rolls back, or closes. Concurrent
independent write transactions are not provided.

No C function signature or required ABI symbol changed. Core's self-manifest
adds `native_shared_core_v1` and capability `native_shared_core` version 1.
The Go SDK checks the loaded signed Core's capability, version, and feature.
GoFrame uses a pool limit of four only with this attestation; older signed
artifacts retain a limit of one. This capability is local to the native
library: it says nothing about a Server endpoint or a published bundle.

## Multiple application processes

Several independent applications must connect to **one Talon service
process**, which opens the directory once. Each client process must stop
opening that directory through the native embedded API. The service is a
deployment and application migration choice; the shared native registry does
not cross process boundaries.

| Surface | Current source behavior | Transaction/session boundary | GoFrame native adapter |
| --- | --- | --- | --- |
| Native C ABI / Go SDK | In-process shared Core and independent handles | Stable session per handle, close rolls back | Yes, embedded only |
| HTTP `POST /api/sql` | Multiple remote clients; SQL and typed bind inputs | Stateless request; no cross-request transaction; current response has no column labels | No |
| TCP `--tcp-addr` | Framed command protocol, used by Talon CLI/TUI | Stable session per TCP connection; disconnect calls `end_session` | No |
| PgWire `--pg-addr` | Optional `pgwire-server` build feature and SQL compatibility surface | Stable session per connection; disconnect calls `end_session` | No |

HTTP and TCP are exposed by the Core server binary. PgWire requires the
feature-enabled build and is a compatibility protocol, not proof of full
PostgreSQL behavior. A remote GoFrame driver would need its own protocol,
column metadata, transaction, trust, and error-code contract; the existing
embedded adapter cannot be pointed at these server URLs.

## Evidence and release state

- Core tests cover path aliases and configuration mismatch, concurrent
  open/final-close, independent-process lock rejection, two C ABI handles,
  transaction ownership, `busy`, native result metadata, and close rollback.
- The Go SDK's `TestLocalSignedCoreKVInterop` signs a clean local Core library
  with an ephemeral test key, then opens two handles and launches the real
  GoFrame adapter against that library. Its four-connection fixture checks
  peer visibility, `busy`, rollback, commit, and the public GoFrame pool path.
- These are local source and test-artifact results. This repository's
  `supply-chain/release-lock.json` remains `UNRELEASED` with gated signing
  and an older pinned Core commit. No Talon binary, native library, SDK tag,
  or production deployment is admitted by this document.
- Application migration remains: choose the single service owner, move
  independent processes to a suitable client protocol, and verify each
  application's required SQL metadata and transaction behavior there.
