package env

import (
	"log"
	"os"
	"strconv"

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
	SolanaRPCURL    string
	AdminWalletPath string // path to keypair JSON (never raw bytes in env)

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

	return &Config{
		Port:            getEnv("PORT", "8080"),
		DatabaseURL:     mustGetEnv("DATABASE_URL"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:         redisDB,
		RabbitMQURL:     mustGetEnv("RABBITMQ_URL"),
		JWTSecret:       mustGetEnv("JWT_SECRET"),
		SolanaRPCURL:    getEnv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
		AdminWalletPath: mustGetEnv("ADMIN_WALLET_PATH"),
		GRPCPort:        getEnv("GRPC_PORT", "50051"),
		RewardThreshold: threshold,
	}
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
