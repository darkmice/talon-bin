package nativecore

import (
	"errors"
	"testing"
)

func TestGatedModuleDoesNotMaterializeNativeCode(t *testing.T) {
	if _, _, err := Materialize(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Materialize error = %v, want ErrUnavailable", err)
	}
}
