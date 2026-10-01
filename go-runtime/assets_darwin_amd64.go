//go:build darwin && amd64

package nativecore

import "embed"

//go:embed native/macos-amd64
var platformAssets embed.FS

func init() { nativeAssets = platformAssets }
