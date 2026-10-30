package artifact

import (
	"fmt"
	"io/fs"
	"os"
)

func walkSyncTree(root string, visit func(*os.File) error) error {
	dir, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("artifact: open tree root: %w", err)
	}
	defer dir.Close()
	return fs.WalkDir(dir.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		file, err := dir.Open(path)
		if err != nil {
			return err
		}
		return visit(file)
	})
}
