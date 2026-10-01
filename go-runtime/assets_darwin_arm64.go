//go:build darwin && arm64

package nativecore

import "embed"

//go:embed native/macos-arm64
var platformAssets embed.FS

func init() { nativeAssets = platformAssets }
