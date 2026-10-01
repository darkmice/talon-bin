// Package nativecore carries release-pinned, signed Talon Core runtime assets.
// A release build replaces the gated identity and native directory atomically.
package nativecore

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

//go:embed identity.json
var identityJSON []byte

// Platform-specific files populate this value. Other targets remain unsupported.
var nativeAssets embed.FS

var ErrUnavailable = errors.New("signed Talon Core Go runtime has not been released")

// Identity is generated from one verified four-platform release. It is an
// expected identity, never a substitute for SDK verification of the manifests.
type Identity struct {
	Status         string `json:"status"`
	ReleaseTag     string `json:"release_tag"`
	TalonBinCommit string `json:"talon_bin_commit"`
	CoreRepository string `json:"core_repository"`
	CoreTag        string `json:"core_tag"`
	CoreCommit     string `json:"core_commit"`
	CoreVersion    string `json:"core_version"`
	ABIProfile     string `json:"abi_profile"`
	ABIVersion     int    `json:"abi_version"`
	HeaderSHA256   string `json:"header_sha256"`
	KeyID          string `json:"key_id"`
	KeySHA256      string `json:"key_sha256"`
	PublicKeyPEM   string `json:"public_key_pem"`
}

var stage struct {
	sync.Once
	dir      string
	identity Identity
	err      error
}

// Materialize copies only this process's platform assets to a private directory.
// The SDK must verify the signature and every member before loading native code.
func Materialize() (string, Identity, error) {
	stage.Do(func() {
		stage.dir, stage.identity, stage.err = materialize(runtime.GOOS, runtime.GOARCH)
	})
	return stage.dir, stage.identity, stage.err
}

func materialize(goos, goarch string) (string, Identity, error) {
	var identity Identity
	if err := json.Unmarshal(identityJSON, &identity); err != nil {
		return "", Identity{}, err
	}
	if identity.Status != "ready" {
		return "", Identity{}, ErrUnavailable
	}
	platform := ""
	switch goos + "/" + goarch {
	case "darwin/amd64":
		platform = "macos-amd64"
	case "darwin/arm64":
		platform = "macos-arm64"
	case "linux/amd64":
		platform = "linux-amd64"
	case "linux/arm64":
		platform = "linux-arm64"
	default:
		return "", Identity{}, fmt.Errorf("unsupported Talon native platform %s/%s", goos, goarch)
	}
	dir, err := os.MkdirTemp("", "talon-native-bundle-")
	if err != nil {
		return "", Identity{}, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", Identity{}, err
	}
	prefix := "libtalon-core-runtime-" + platform
	names := []string{prefix + ".tar.gz", prefix + ".manifest.json", prefix + ".manifest.json.sig", prefix + ".sbom.cdx.json", prefix + ".licenses.json", "LICENSE.core", "NOTICE"}
	for _, name := range names {
		source, err := nativeAssets.Open("native/" + platform + "/" + name)
		if err != nil {
			_ = os.RemoveAll(dir)
			return "", Identity{}, fmt.Errorf("runtime asset %s: %w", name, err)
		}
		target, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, err = io.Copy(target, source)
			if closeErr := target.Close(); err == nil {
				err = closeErr
			}
		}
		_ = source.Close()
		if err != nil {
			_ = os.RemoveAll(dir)
			return "", Identity{}, err
		}
	}
	return dir, identity, nil
}
