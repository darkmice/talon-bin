#ifndef TALON_RUNTIME_V1_H
#define TALON_RUNTIME_V1_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
#define TALON_RUNTIME_PROTOCOL_VERSION 1u
#define TALON_RUNTIME_MAX_MESSAGE_BYTES (8u * 1024u * 1024u)
/* Input is a borrowed UTF-8 JSON byte buffer, valid for this call only.
 * Output is a NUL-terminated JSON envelope allocated by this exact library.
 * 0: output envelope returned (inspect ok/code); -1: no valid output buffer.
 * Every non-NULL output must be freed once with talon_runtime_free_v1.
 * Never pass Rust references, trait objects, allocator-owned Strings or Store
 * pointers across the ABI. Opaque IDs are process/library-local, never pointers.
 * Call/close may run concurrently; each admitted operation retains its own
 * native database owner until execution, callbacks and cleanup finish.
 */
int talon_runtime_call_v1(const unsigned char *input, size_t length, char **output);
void talon_runtime_free_v1(char *output);
#ifdef __cplusplus
}
#endif
#endif
