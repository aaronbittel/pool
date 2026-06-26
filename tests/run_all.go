package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

const testDir = "./tests/"

func main() {
	fmt.Println("Build all executables...")
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

	results := map[string]bool{}

	fmt.Println("Starting test runner...")
	for _, path := range tests {
		cmd := exec.Command(path)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			var exitErr *exec.ExitError
			switch {
			case errors.As(err, &exitErr):
				results[path] = false
			default:
				log.Fatal("could not run cmd for %s %v: %v", path, cmd, err)
			}
		} else {
			results[path] = true
		}
	}

	fmt.Println("\nsummary:")
	for _, path := range tests {
		res := "SUCCESS"
		if !results[path] {
			res = "FAILED"
		}
		fmt.Printf("- %s: %s\n", path, res)
	}
}
