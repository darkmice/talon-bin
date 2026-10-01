//go:build linux && arm64

package nativecore

import "embed"

//go:embed native/linux-arm64
var platformAssets embed.FS

func init() { nativeAssets = platformAssets }
