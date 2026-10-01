package nativecore

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestModuleMaterializesOnlyWhenReady(t *testing.T) {
	var identity Identity
	if err := json.Unmarshal(identityJSON, &identity); err != nil {
		t.Fatal(err)
	}
	dir, actual, err := Materialize()
	if identity.Status != "ready" {
		if !errors.Is(err, ErrUnavailable) || dir != "" {
			t.Fatalf("gated Materialize = (%q, %v), want ErrUnavailable", dir, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("ready Materialize: %v", err)
	}
	if actual != identity {
		t.Fatal("materialized identity differs from pinned identity")
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	platform := map[string]string{
		"darwin/amd64": "macos-amd64",
		"darwin/arm64": "macos-arm64",
		"linux/amd64":  "linux-amd64",
		"linux/arm64":  "linux-arm64",
	}[runtime.GOOS+"/"+runtime.GOARCH]
	if platform == "" {
		t.Fatalf("ready module test requires a supported platform, got %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	prefix := "libtalon-core-runtime-" + platform
	for _, name := range []string{
		prefix + ".tar.gz",
		prefix + ".manifest.json",
		prefix + ".manifest.json.sig",
		prefix + ".sbom.cdx.json",
		prefix + ".licenses.json",
		"LICENSE.core",
		"NOTICE",
	} {
		info, statErr := os.Stat(filepath.Join(dir, name))
		if statErr != nil || info.Size() == 0 {
			t.Fatalf("materialized %s: info=%v err=%v", name, info, statErr)
		}
	}
}
