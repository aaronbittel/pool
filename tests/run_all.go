package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

const testDir = "./tests/"

type result struct {
	test   string
	ok     bool
	worker int
}

func main() {
	workers := flag.Int("worker", 3, "number of workers")
	flag.Parse()

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

	jobs := make(chan string)
	results := make(chan result)

	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for testPath := range jobs {
				fmt.Printf("[worker %d] running %q...\n", workerID, testPath)

				ok := runTest(testPath)

				results <- result{
					test:   testPath,
					ok:     ok,
					worker: workerID,
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		for _, t := range tests {
			jobs <- t
		}
		close(jobs)
	}()

	allOK := true

	for r := range results {
		status := "SUCCESS"
		if !r.ok {
			status = "FAILED"
			allOK = false
		}
		fmt.Printf("[worker %d] %q: %s\n", r.worker, r.test, status)
	}

	fmt.Println()

	if allOK {
		fmt.Println("All tests passed successfully")
	} else {
		fmt.Println("Some tests failed")
	}
}

func runTest(testPath string) bool {
	if err := exec.Command(testPath).Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false
		}
		log.Fatalf("could not run cmd for %s: %v", testPath, err)
	}
	return true
}
