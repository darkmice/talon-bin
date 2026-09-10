#include <stddef.h>
#include <string.h>

#define OVERSIZED_MANIFEST_BYTES (1024u * 1024u + 2u)

static char oversized_manifest[OVERSIZED_MANIFEST_BYTES];

int talon_build_manifest(char **out_json) {
    if (out_json == NULL) {
        return -1;
    }
    memset(oversized_manifest, 'x', sizeof(oversized_manifest));
    oversized_manifest[sizeof(oversized_manifest) - 1u] = '\0';
    *out_json = oversized_manifest;
    return 0;
}

void talon_free_string(char *value) {
    (void)value;
}
