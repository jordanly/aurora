// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"aurora.local/tools/internal/bootstrap"
)

func StageJava(ctx context.Context, root string, args []string) error {
	flags := flag.NewFlagSet("stage-java", flag.ContinueOnError)
	output := flags.String("output", "", "fresh materialized JDK directory (required)")
	offline := flags.Bool("offline", os.Getenv("AURORA_INPLACE_OFFLINE") == "1", "disable archive downloads")
	cache := flags.String("cache", envPath("AURORA_INPLACE_CACHE", filepath.Join(root, ".cache/inplace-build")), "private verified archive cache")
	seed := flags.String("seed-archives", envPath("AURORA_INPLACE_SEED_ARCHIVES", filepath.Join(root, ".pi-tools/downloads")), "verified archive seed directory")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *output == "" {
		return errors.New("usage: stage-java --output NEW_DIR [--offline]")
	}
	return stageJava(ctx, filepath.Join(root, "build-support/java/toolchains.json"), *output, *cache, *seed, *offline)
}

func stageJava(ctx context.Context, manifest, output, cache, seed string, offline bool) error {
	output, err := bootstrap.CheckPath(output)
	if err != nil {
		return err
	}
	receipt := output + ".provenance.json"
	for _, path := range []string{output, receipt} {
		if _, err = bootstrap.CheckPath(path); err != nil {
			return err
		}
		if _, err = os.Lstat(path); !os.IsNotExist(err) {
			return fmt.Errorf("stage-java requires absent output and receipt: %s", path)
		}
	}
	parent, err := bootstrap.PrivateCache(filepath.Dir(output))
	if err != nil {
		return err
	}
	cache, err = bootstrap.PrivateCache(cache)
	if err != nil {
		return err
	}
	unlock, err := bootstrap.Lock(filepath.Join(cache, "stage-java.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	pins, err := bootstrap.LoadManifest(manifest)
	if err != nil {
		return err
	}
	pin, ok := pins.Tools["java"]
	if !ok {
		return errors.New("Java pin missing")
	}
	// Selecting only Java avoids fetching or materializing Gradle for lab staging.
	pins.Tools = map[string]bootstrap.Pin{"java": pin}
	work, err := os.MkdirTemp(parent, ".stage-java-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	extracted, err := bootstrap.Tools(ctx, pins, cache, work, seed, offline)
	if err != nil {
		return err
	}
	java := extracted["java"]
	for _, name := range []string{"bin/java", "bin/keytool", "release"} {
		info, e := os.Stat(filepath.Join(java, name))
		if e != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("Java archive missing regular %s", name)
		}
	}
	digest, err := TreeDigest(java)
	if err != nil {
		return err
	}
	record := map[string]any{"schema": 1, "tool": "java", "platform": pins.Platform, "archive": pin, "treeSha256": digest}
	stagedReceipt := filepath.Join(work, "receipt.json")
	if err = writeJSON(stagedReceipt, record); err != nil {
		return err
	}
	// Both publications refuse replacement, including a destination created while
	// extraction ran. An interrupted receipt-only publication is fail-closed.
	if err = os.Link(stagedReceipt, receipt); err != nil {
		return err
	}
	if err = renameNewDirectory(java, output); err != nil {
		os.Remove(receipt)
		return err
	}
	directory, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return err
	}
	fmt.Println(output)
	return nil
}

func renameNewDirectory(source, destination string) error {
	from, err := syscall.BytePtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.BytePtrFromString(destination)
	if err != nil {
		return err
	}
	// Linux renameat2(RENAME_NOREPLACE), AT_FDCWD. The supported tools platform
	// is linux/arm64; unlike os.Rename this cannot replace an existing directory.
	_, _, errno := syscall.Syscall6(syscall.SYS_RENAMEAT2, ^uintptr(99), uintptr(unsafe.Pointer(from)), ^uintptr(99), uintptr(unsafe.Pointer(to)), 1, 0)
	if errno != 0 {
		return &os.LinkError{Op: "rename-noreplace", Old: source, New: destination, Err: errno}
	}
	return nil
}
