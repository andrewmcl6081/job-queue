package main

import (
	"fmt"
	"log"

	"github.com/andrewmcl6081/job-queue/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	fmt.Printf("%+v\n", cfg)
}