package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

const testDir = "./tests/"

func main() {
	fmt.Println("Build all executables ...")

	cmd := exec.Command("go", "build", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}

	tests, err := filepath.Glob(testDir + "*.sh")
	if err != nil {
		log.Fatalf("glob failed: %v", err)
	}

	var wg sync.WaitGroup

	for _, testPath := range tests {
		fmt.Printf("starting test %q...\n", testPath)
		wg.Go(func() {
			success := runTest(testPath)
			r := "SUCCESS"
			if !success {
				r = "FAILED"
			}
			fmt.Printf("%q done: %s\n", testPath, r)
		})
	}
	wg.Wait()
}

func runTest(testPath string) bool {
	if err := exec.Command(testPath).Run(); err != nil {
		var exitErr *exec.ExitError
		switch {
		case errors.As(err, &exitErr):
			return false
		default:
			log.Fatal("could not run cmd for %s: %v", testPath, err)
		}
	}
	return true
}
