/* Read the self-attested build manifest from one exact Talon Core library. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TALON_BUILD_MANIFEST_MAX_BYTES (1024u * 1024u)

typedef int (*talon_build_manifest_fn)(char **);
typedef void (*talon_free_string_fn)(char *);

int main(int argc, char **argv) {
    void *library;
    talon_build_manifest_fn build_manifest = NULL;
    talon_free_string_fn free_string = NULL;
    char *manifest = NULL;
    const char *manifest_end;
    size_t manifest_size;
    const char *error;

    if (argc != 2) {
        fprintf(stderr, "usage: %s /path/to/libtalon\n", argv[0]);
        return 2;
    }
    library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (library == NULL) {
        fprintf(stderr, "cannot load Core library: %s\n", dlerror());
        return 1;
    }
    dlerror();
    *(void **)(&build_manifest) = dlsym(library, "talon_build_manifest");
    error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "cannot resolve talon_build_manifest: %s\n", error);
        dlclose(library);
        return 1;
    }
    *(void **)(&free_string) = dlsym(library, "talon_free_string");
    error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "cannot resolve talon_free_string: %s\n", error);
        dlclose(library);
        return 1;
    }
    if (build_manifest(&manifest) != 0 || manifest == NULL) {
        fputs("talon_build_manifest failed\n", stderr);
        dlclose(library);
        return 1;
    }
    manifest_end = memchr(manifest, '\0', TALON_BUILD_MANIFEST_MAX_BYTES + 1u);
    if (manifest_end == NULL) {
        fputs("Core build manifest exceeds the 1 MiB bound or is not terminated\n", stderr);
        free_string(manifest);
        dlclose(library);
        return 1;
    }
    manifest_size = (size_t)(manifest_end - manifest);
    if (fwrite(manifest, 1u, manifest_size, stdout) != manifest_size || fputc('\n', stdout) == EOF) {
        fputs("cannot write Core build manifest\n", stderr);
        free_string(manifest);
        dlclose(library);
        return 1;
    }
    free_string(manifest);
    if (dlclose(library) != 0) {
        fprintf(stderr, "cannot close Core library: %s\n", dlerror());
        return 1;
    }
    return 0;
}
