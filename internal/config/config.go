package config

import (
	"time"
)

type Config struct {
	DatabaseURL					string
	RedisURL						string
	APIPort							string
	WorkerID						string
	DispatcherInterval	time.Duration
	WorkerBatchSize			int
	WorkerBlockMS				int
}