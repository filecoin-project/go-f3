package msgdump

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
)

func DirForNetwork(dir string, nn gpbft.NetworkName) string {
	basename := fmt.Sprintf("msgs-%s", strings.ReplaceAll(string(nn), "/", "-"))
	return filepath.Join(dir, basename)
}
