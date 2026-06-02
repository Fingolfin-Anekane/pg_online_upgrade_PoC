package phases

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// copyTree recursively copies the directory at src into dst, preserving the
// relative layout and file permissions. dst is created if absent. It is used to
// archive pg_upgrade's output directory before the old cluster is removed.
func copyTree(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("archive: stat source %s: %w", src, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("archive: source %s is not a directory", src)
	}
	return filepath.Walk(src, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if fi.IsDir() {
			return os.MkdirAll(target, fi.Mode().Perm())
		}
		return copyFile(path, target, fi.Mode().Perm())
	})
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("archive: open %s: %w", src, err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("archive: mkdir for %s: %w", dst, err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("archive: create %s: %w", dst, err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("archive: copy %s -> %s: %w", src, dst, err)
	}
	return nil
}
