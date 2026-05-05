package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL			string
	RedisURL			string
	APIPort				string
	WorkerID			string
	DispatcherInterval	time.Duration
	WorkerBatchSize		int
	WorkerBlockMS		int
}

func Load() (*Config, error) {
	dispatcherInterval, err := getDurationEnvOrDefault("DISPATCHER_INTERVAL", time.Second)
	if err != nil {
		return nil, err
	}

	workerBatchSize, err := getIntEnvOrDefault("WORKER_BATCH_SIZE", 10)
	if err != nil {
		return nil, err
	}

	workerBlockMS, err := getIntEnvOrDefault("WORKER_BLOCK_MS", 5000)
	if err != nil {
		return nil, err
	}

	cfg := &Config {
		DatabaseURL: 			getEnvOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"),
		RedisURL:				getEnvOrDefault("REDIS_URL", "redis://localhost:6379"),
		APIPort:				getEnvOrDefault("API_PORT", "8080"),
		WorkerID:				getWorkerID(),
		DispatcherInterval:		dispatcherInterval,
		WorkerBatchSize:		workerBatchSize,
		WorkerBlockMS:			workerBlockMS,
	}

	return cfg, nil
}

func getEnvOrDefault(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnvOrDefault(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", key, value, err)
	}

	return parsed, nil
}

func getIntEnvOrDefault(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", key, value, err)
	}

	return parsed, nil
}

func getWorkerID() string {
	value := os.Getenv("WORKER_ID")
	if value != "" {
		return value
	}

	pid := strconv.Itoa(os.Getpid())

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return pid
	}

	return hostname + "-" + pid
}