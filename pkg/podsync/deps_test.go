package podsync_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestOnlyThisModule enforces the zero-dependency rule: go.mod has no require
// block and every non-standard package in the build is under this module.
func TestOnlyThisModule(t *testing.T) {
	mod, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "require") {
		t.Error("go.mod has a require directive")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go tool not on PATH")
	}
	cmd := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...")
	cmd.Dir = "../.."
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, p := range strings.Fields(string(out)) {
		if p != "github.com/LavishGent/podsync" && !strings.HasPrefix(p, "github.com/LavishGent/podsync/") {
			t.Errorf("external dependency: %s", p)
		}
	}
}
