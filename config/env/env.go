package env

import (

	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/base58"
	"github.com/joho/godotenv"
)

type Config struct {
	// Server
	Port string

	// Database
	DatabaseURL string

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// RabbitMQ
	RabbitMQURL string

	// JWT
	JWTSecret string

	// Solana
	SolanaRPCURL         string
	BackendPrivateKeyHex string // 🔑 Now tracking your raw hex string here
	programID string 

	// gRPC
	GRPCPort string

	// Reward
	RewardThreshold float64
}

func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, reading from environment")
	}

	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	threshold, _ := strconv.ParseFloat(getEnv("REWARD_THRESHOLD", "10.0"), 64)

	cfg := &Config{
		Port:                 getEnv("PORT", "8080"),
		DatabaseURL:          mustGetEnv("DATABASE_URL"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:        getEnv("REDIS_PASSWORD", ""),
		RedisDB:              redisDB,
		RabbitMQURL:          mustGetEnv("RABBITMQ_URL"),
		JWTSecret:            mustGetEnv("JWT_SECRET"),
		SolanaRPCURL:         getEnv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
		BackendPrivateKeyHex: mustGetEnv("BACKEND_PRIVATE_KEY_HEX"), // Loaded straight from env
		programID:             mustGetEnv("PROGRAM_ID"),
		GRPCPort:             getEnv("GRPC_PORT", "50051"),
		RewardThreshold:      threshold,
	}

	// Fail-fast validation check: Ensure the hex string is actually a valid Solana keypair layout
	if _, err := cfg.GetBackendPrivateKey(); err != nil {
		log.Fatalf("FATAL: BACKEND_PRIVATE_KEY_HEX environment variable validation failed: %v", err)
	}

	return cfg
}

// GetBackendPrivateKey converts your Hex private key seamlessly into the native
// solana.PrivateKey layout object required by your queue worker.
func (c *Config) GetBackendPrivateKey() (solana.PrivateKey, error) {
	rawBytes, err := base58.Decode(c.BackendPrivateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base58 private key: %w", err)
	}

	// Solana keypair must be 64 bytes
	if len(rawBytes) != 64 {
		return nil, fmt.Errorf("invalid solana private key length: got %d, expected 64", len(rawBytes))
	}

	return solana.PrivateKey(rawBytes), nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required environment variable %q is not set", key)
	}
	return v
}
