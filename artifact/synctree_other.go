//go:build !darwin

package artifact

import (
	"errors"
	"os"
)

func syncTree(root string) error {
	return walkSyncTree(root, func(file *os.File) error {
		return errors.Join(file.Sync(), file.Close())
	})
}
