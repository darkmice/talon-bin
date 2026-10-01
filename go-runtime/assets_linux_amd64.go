//go:build linux && amd64

package nativecore

import "embed"

//go:embed native/linux-amd64
var platformAssets embed.FS

func init() { nativeAssets = platformAssets }
